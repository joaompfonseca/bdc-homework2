import argparse
import math

from pyspark import RDD, SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans


def dist(u: tuple, C: list) -> float:
    min_i, min_d = 0, float('inf')
    for i, c in enumerate(C):
        d = math.dist(u, c)
        if d < min_d:
            min_i, min_d = i, d
    return min_i, min_d


def mean_vector(X: list) -> tuple:
    n = len(X[0])
    mean = [0.0] * n
    for i in range(n):
        for x in X:
            mean[i] += x[i]
    return tuple(m / len(X) for m in mean)


def computeVectorX(fixed_a, fixed_b, alpha, beta, ell, k):
    gamma = 0.5
    x_dist = [0.0] * k
    power = 0.5
    t_max = 10

    for _ in range(t_max):
        f_a = fixed_a
        f_b = fixed_b
        power /= 2

        for i in range(k):
            temp = (1 - gamma) * beta[i] * ell[i] / (gamma * alpha[i] + (1 - gamma) * beta[i])
            x_dist[i] = temp
            f_a += alpha[i] * temp * temp
            temp = ell[i] - temp
            f_b += beta[i] * temp * temp

        if f_a == f_b:
            break

        gamma = gamma + power if f_a > f_b else gamma - power

    return x_dist


def CentroidSelection(P: RDD, K: int) -> list[tuple]:
    fixed_a = 0.0
    fixed_b = 0.0
    alpha   = [0.0] * K
    beta    = [0.0] * K
    mu_A    = [0.0] * K
    mu_B    = [0.0] * K
    ell     = [0.0] * K
    
    def compute(X: list) -> tuple:
        count_A = len([x for x in X if x[-1] == 'A'])
        count_B = len([x for x in X if x[-1] == 'B'])
        mu_A    = mean_vector([x[:-1] for x in X if x[-1] == 'A'])
        mu_B    = mean_vector([x[:-1] for x in X if x[-1] == 'B'])
        ell     = math.dist(mu_A, mu_B)
        fixed_a_part = sum(math.dist(x[:-1], mu_A) for x in X if x[-1] == 'A')
        fixed_b_part = sum(math.dist(x[:-1], mu_B) for x in X if x[-1] == 'B')
        return count_A, count_B, mu_A, mu_B, ell, fixed_a_part, fixed_b_part
    
    P_data = (P.mapValues(lambda X: compute(X)) # [(centroid, (count_A, count_B, mu_A, mu_B, ell, fixed_a_part, fixed_b_part)),...]
               .sortByKey()
               .collect())
    
    NA = sum(P_data[i][1][0] for i in range(K))
    NB = sum(P_data[i][1][1] for i in range(K))    
    
    for i in range(K):
                
        alpha[i] = P_data[i][1][0] / NA if NA > 0 else 0.0
        beta[i]  = P_data[i][1][1] / NB if NB > 0 else 0.0
        
        mu_A[i] = P_data[i][1][2]
        mu_B[i] = P_data[i][1][3]
        
        ell[i] = P_data[i][1][4]
        
        fixed_a += P_data[i][1][5]
        fixed_b += P_data[i][1][6]
        
    fixed_a /= NA if NA > 0 else 1.0
    fixed_b /= NB if NB > 0 else 1.0
        
    x = computeVectorX(fixed_a, fixed_b, alpha, beta, ell, K)
    
    C = [] 
    for i in range(K):
        xi = x[i]
        ci = []
        for j in range(len(mu_A[i])):
            cj = ((ell[i] - xi) * mu_A[i][j] + xi * mu_B[i][j]) / ell[i] if ell[i] != 0 else mu_A[i][j]
            ci.append(cj)
        C.append(tuple(ci))

    return C 
        

def MRFairLloyd(U: RDD, K: int, M: int) -> list[tuple]:
    # Compute initial set of centroids C using KMeans-parallel
    C = KMeans.train(U.map(lambda u: u[:-1]), K, maxIterations=0).clusterCenters
    
    for _ in range(M):        
        # Partition U into K clusters using set of centroids C
        P = (U.map       (lambda u: (dist(u[:-1], C)[0], u)) # [(centroid, point),...]
              .groupByKey())                                 # [(centroid, [point,...]),...]
        # Compute a fair set of centroids C based on point labels
        C = CentroidSelection(P, K)        
    return C


def MRComputeFairObjective(U: RDD, C: list) -> float:
        return (U.map        ( lambda u:    (u[-1], (dist(u[:-1], C)[1] ** 2, 1)) ) # [(label, (distance, count)),...]
                 .reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1])            ) # [(label, (sum_distance, sum_count)),...]
                 .map        ( lambda x:    (1 / x[1][1]) * x[1][0]               ) # [objective,...]       
                 .max())                                                            # max objective


def main(data_path, L, K, M):

    # Print command-line arguments
    print(f'Input file = {data_path}, L = {L}, K = {K}, M = {M}')

    # Setup Spark
    conf = SparkConf().setAppName('G11HW2')
    sc = SparkContext(conf=conf)

    # Subdivide the input file into L random partitions
    docs = sc.textFile(data_path).repartition(numPartitions=L).cache()
    input_points = docs.map(lambda x: [float(i) for i in x.split(',')[:-1]] + [x.split(',')[-1]]).cache()

    # Print N, NA, NB
    N = input_points.count()
    NA = input_points.filter(lambda x: x[-1] == 'A').count()
    NB = input_points.filter(lambda x: x[-1] == 'B').count()
    print(f'N = {N}, NA = {NA}, NB = {NB}')

    # Compute standard centroids (no labels)
    time_start = 0 # TODO: measure start time
    C_stand = KMeans.train(input_points.map(lambda x: x[:-1]), K, maxIterations=M).clusterCenters
    time_end = 0   # TODO: measure end time
    C_stand_time = time_end - time_start # TODO: must be in ms (integer)
    
    # Compute fair centroids (use labels)
    time_start = 0 # TODO: measure start time
    C_fair = MRFairLloyd(input_points, K, M)
    time_end = 0   # TODO: measure end time
    C_fair_time = time_end - time_start # TODO: must be in ms (integer)

    # Compute fair objective for standard centroids
    time_start = 0 # TODO: measure start time
    phi_stand = MRComputeFairObjective(input_points, C_stand)
    time_end = 0   # TODO: measure end time
    phi_stand_time = time_end - time_start # TODO: must be in ms (integer)
    
    # Compute fair objective for fair centroids
    time_start = 0 # TODO: measure start time
    phi_fair = MRComputeFairObjective(input_points, C_fair)
    time_end = 0   # TODO: measure end time
    phi_fair_time = time_end - time_start # TODO: must be in ms (integer)
    
    # Print objectives
    print(f'Fair Objective with Standard Centers = {phi_stand:.4f}')
    print(f'Fair Objective with Fair Centers = {phi_fair:.4f}')
    
    # Print time taken to compute centroids and objectives
    print(f'Time to compute standard centers = {C_stand_time} ms')
    print(f'Time to compute fair centers = {C_fair_time} ms')
    print(f'Time to compute objective with standard centers = {phi_stand_time} ms')
    print(f'Time to compute objective with fair centers = {phi_fair_time} ms')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', type=str, help='Path to the input file')
    parser.add_argument('L', type=int, help='Number of partitions')
    parser.add_argument('K', type=int, help='Number of centroids')
    parser.add_argument('M', type=int, help='Number of iterations')

    args = parser.parse_args()
    main(args.data_path, args.L, args.K, args.M)
