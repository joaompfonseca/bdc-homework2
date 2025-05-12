import argparse
import math
import time

from typing import Iterable, Tuple, List
from pyspark import RDD, SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans


def dist(u: Tuple, C: List) -> float:
    min_i, min_d = 0, float('inf')
    for i, c in enumerate(C):
        d = math.dist(u, c)
        if d < min_d:
            min_i, min_d = i, d
    return min_i, min_d


def partial_compute(pairs: Iterable[Tuple[int, Tuple]]) -> List[Tuple]:
    data = dict()

    for centroid, point in pairs:
        D = len(point) - 1 # Dimension of the points
        
        if centroid not in data:
            data[centroid] = [
                0,         # count_A
                [0.0] * D, # sum_A
                0,         # count_B
                [0.0] * D, # sum_B
            ]
        # Update the counts and sums based on the label
        if point[-1] == 'A':            
            data[centroid][0] += 1
            for i in range(D):
                data[centroid][1][i] += point[i]
        elif point[-1] == 'B':
            data[centroid][2] += 1
            for i in range(D):
                data[centroid][3][i] += point[i]
    
    # Prepare the output
    partial_results = []
    for centroid, results in data.items():
        centroid_results = []
        for value in results:
            if type(value) == list:
                centroid_results.append(tuple(value))
            else:
                centroid_results.append(value)
        partial_results.append((centroid, tuple(centroid_results)))
        
    return partial_results # [(centroid, (count_A, sum_A, count_B, sum_B)), ...]


def reduce_compute(x: List[Tuple], y: List[Tuple]) -> List[Tuple]:
    return (
        x[0] + y[0],                              # count_A
        tuple(a + b for a, b in zip(x[1], y[1])), # sum_A
        x[2] + y[2],                              # count_B
        tuple(a + b for a, b in zip(x[3], y[3]))  # sum_B
    )


def final_compute(x: Tuple) -> Tuple:
    count_A, sum_A, count_B, sum_B = x
    mu_A = [v / count_A if count_A > 0 else 0.0 for v in sum_A]
    mu_B = [v / count_B if count_B > 0 else 0.0 for v in sum_B]
    ell = math.dist(mu_A, mu_B)
    return (count_A, tuple(mu_A), count_B, tuple(mu_B), ell)


def partial_compute_2(pairs: Iterable[Tuple[int, Tuple]], P_data: dict) -> List[Tuple]:
    data = dict()
    
    for centroid, point in pairs:
        if centroid not in data:
            data[centroid] = [
                0.0, # delta_A
                0.0  # delta_B
            ]
        # Update the deltas based on the label
        if point[-1] == 'A':
            data[centroid][0] += math.dist(point[:-1], P_data[centroid][1]) ** 2
        elif point[-1] == 'B':
            data[centroid][1] += math.dist(point[:-1], P_data[centroid][3]) ** 2
    
    # Prepare the output
    partial_results = []
    for centroid, results in data.items():
        partial_results.append((centroid, tuple(results)))
        
    return partial_results # [(centroid, (delta_A, delta_B)), ...]


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


def CentroidSelection(P: RDD, K: int) -> List[Tuple]:
    fixed_A = 0.0
    fixed_B = 0.0
    alpha   = [0.0] * K
    beta    = [0.0] * K
    ell     = [0.0] * K
    
    P_data = (P.mapPartitions( lambda x:    partial_compute(x)   ) # [(centroid, (count_A, sum_A, count_B, sum_B)), ...]
               .reduceByKey  ( lambda x, y: reduce_compute(x, y) ) # [(centroid, (count_A, sum_A, count_B, sum_B)), ...]
               .mapValues    ( lambda x:    final_compute(x)     ) # [(centroid, (count_A, mu_A, count_B, mu_B, ell)), ...]
               .collectAsMap())                                    # {centroid: (count_A, mu_A, count_B, mu_B, ell)}
    
    P_data_2 = (P.mapPartitions( lambda x:    partial_compute_2(x, P_data) ) # [(centroid, (delta_A, delta_B)), ...]
                 .reduceByKey  ( lambda x, y: (x[0] + y[0], x[1] + y[1])   ) # [(centroid, (delta_A, delta_B)), ...]
                 .collectAsMap())                                            # {centroid: (delta_A, delta_B)}
    
    NA = sum(P_data[i][0] for i in range(K))
    NB = sum(P_data[i][2] for i in range(K))
    
    for i in range(K):
        alpha[i] = P_data[i][0] / NA if NA > 0 else 0.0
        beta[i]  = P_data[i][2] / NB if NB > 0 else 0.0
        ell[i] = P_data[i][4]
        fixed_A += P_data_2[i][0]
        fixed_B += P_data_2[i][1]
    fixed_A /= NA if NA > 0 else 1.0
    fixed_B /= NB if NB > 0 else 1.0
        
    x = computeVectorX(fixed_A, fixed_B, alpha, beta, ell, K)
    
    C = [] 
    for i in range(K):
        mu_A, mu_B = P_data[i][1], P_data[i][3]
        ci = []
        for j in range(len(mu_A)):
            cj = ((ell[i] - x[i]) * mu_A[j] + x[i] * mu_B[j]) / ell[i] if ell[i] != 0 else mu_A[j]
            ci.append(cj)
        C.append(tuple(ci))
    return C 
        

def MRFairLloyd(U: RDD, K: int, M: int) -> List[Tuple]:
    # Compute initial set of centroids C using KMeans-parallel
    C = KMeans.train(U.map(lambda u: u[:-1]), K, maxIterations=0).clusterCenters
    
    for _ in range(M):        
        # Partition U into K clusters using set of centroids C
        P = U.map(lambda u: (dist(u[:-1], C)[0], u)) # [(centroid, point),...]
        # Compute a fair set of centroids C based on point labels
        C = CentroidSelection(P, K)
    return C


def MRComputeFairObjective(U: RDD, C: List) -> float:
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
    sc.setLogLevel('ERROR')

    # Subdivide the input file into L random partitions
    docs = sc.textFile(data_path).repartition(numPartitions=L).cache()
    input_points = docs.map(lambda x: [float(i) for i in x.split(',')[:-1]] + [x.split(',')[-1]]).cache()

    # Force the two cached RDDs to materialise before the time mesurments
    _ = docs.count()
    # Print N, NA, NB
    N = input_points.count()
    NA = input_points.filter(lambda x: x[-1] == 'A').count()
    NB = input_points.filter(lambda x: x[-1] == 'B').count()
    print(f'N = {N}, NA = {NA}, NB = {NB}')

    # Compute standard centroids (no labels)
    time_start = time.perf_counter() # measure start time
    C_stand = KMeans.train(input_points.map(lambda x: x[:-1]), K, maxIterations=M).clusterCenters
    time_end = time.perf_counter()   # measure end time
    C_stand_time = int((time_end - time_start) * 1000) # must be in ms (integer)
    
    # Compute fair centroids (use labels)
    time_start = time.perf_counter() # measure start time
    C_fair = MRFairLloyd(input_points, K, M)
    time_end = time.perf_counter()   # measure end time
    C_fair_time = int((time_end - time_start) * 1000)  # must be in ms (integer)

    # Compute fair objective for standard centroids
    time_start = time.perf_counter() # measure start time
    phi_stand = MRComputeFairObjective(input_points, C_stand)
    time_end = time.perf_counter()   # measure end time
    phi_stand_time =  int((time_end - time_start) * 1000) # must be in ms (integer)
    
    # Compute fair objective for fair centroids
    time_start = time.perf_counter() # measure start time
    phi_fair = MRComputeFairObjective(input_points, C_fair)
    time_end = time.perf_counter()   # measure end time
    phi_fair_time = int((time_end - time_start) * 1000) # must be in ms (integer)
    
    # Print objectives
    print(f'Fair Objective with Standard Centers = {phi_stand:.4f}')
    print(f'Fair Objective with Fair Centers = {phi_fair:.4f}')
    
    # Print time taken to compute centroids and objectives
    print(f'Time to compute standard centers = {C_stand_time} ms')
    print(f'Time to compute fair centers = {C_fair_time} ms')
    print(f'Time to compute objective with standard centers = {phi_stand_time} ms')
    print(f'Time to compute objective with fair centers = {phi_fair_time} ms')

    # Keep the Spark Web interface alive
    # input("Press <Enter> to exit and close the Spark UI…")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', type=str, help='Path to the input file')
    parser.add_argument('L', type=int, help='Number of partitions')
    parser.add_argument('K', type=int, help='Number of centroids')
    parser.add_argument('M', type=int, help='Number of iterations')

    args = parser.parse_args()
    main(args.data_path, args.L, args.K, args.M)
