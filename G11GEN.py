import argparse
import random
import os

def main(N: int, K: int, output: str, D: int, A_group_ratio: float, A_centroid_ratio: float, val_min: float, val_max: float, std_dev: float):
    points = []
        
    NA = int(N * A_group_ratio)    # Number of points in group A
    NB = N - NA                    # Number of points in group B
    
    CA = int(K * A_centroid_ratio) # Number of centroids for group A
    CB = K - CA                    # Number of centroids for group B
    
    # Generate K random centroids
    C = [[random.uniform(val_min, val_max) for _ in range(D)] for _ in range(K)]
    
    # Generate points for group A
    for i in range(NA):
        centroid = C[i % CA]
        point = [random.gauss(centroid[d], std_dev) for d in range(D)] + ['A']
        points += [point]
    
    # Generate points for group B
    for i in range(NB):
        centroid = C[CA + (i % CB)]
        point = [random.gauss(centroid[d], std_dev) for d in range(D)] + ['B']
        points += [point]
    
    # Shuffle the points
    random.shuffle(points)
    
    if not output:
        # Print the points to stdout
        for point in points:
            print(','.join(map(str, point)))
    else:
        # Ensure the output directory exists
        output_dir = os.path.dirname(output)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
        # Write points to the output file
        with open(output, 'w') as f:
            for point in points:
                f.write(','.join(map(str, point)) + '\n')
    
        print(f"Generated {N} points and saved to: {output}")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('N', type=int, help='Number of points')
    parser.add_argument('K', type=int, help='Number of centroids')
    # Optional arguments
    parser.add_argument('--output', type=str, default="", help='Output file path (default: print to stdout)')
    parser.add_argument('--D', type=int, default=2, help='Number of dimensions (default: 2)')
    parser.add_argument('--A_group_ratio', type=float, default=0.9, help='Group A ratio (default: 0.9)')
    parser.add_argument('--A_centroid_ratio', type=float, default=0.5, help='Centroid A ratio (default: 0.5)')
    parser.add_argument('--val_min', type=float, default=-1, help='Minimum value for centroids (default: -1)')
    parser.add_argument('--val_max', type=float, default=1, help='Maximum value for centroids (default: 1)')
    parser.add_argument('--std_dev', type=float, default=0.1, help='Standard deviation for Gaussian distribution (default: 0.1)')

    args = parser.parse_args()
    main(args.N, args.K, args.output, args.D, args.A_group_ratio, args.A_centroid_ratio, args.val_min, args.val_max, args.std_dev)
