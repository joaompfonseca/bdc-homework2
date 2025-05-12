# BDC Homework 2 - Fair K-Means Clustering

## Description

The purpose of the second homework is to implement the variant of Lloyd's algorithm for fair k-means clustering, proposed in the paper [Socially Fair k-Means Clustering (ACM FAccT'21)](https://dl.acm.org/doi/10.1145/3442188.3445906), and to compare its effectiveness against the standard variant, with respect to the new objective function introduced in that paper. Moreover, the scalability of the implementation should be tested on the CloudVeneto cluster available for the course.

**Course:** Big Data Computing (2024/2025).

## How to run `G11GEN.py`

This script generates an input dataset with `N` points and `K` centroids distributed in two demographic groups `A` and `B`.

- Run `G11GEN.py [OPTIONS] N K` to generate the dataset.
  - `N` Number of points.
  - `K` Number of centroids.
  > `OPTIONS` are the optional arguments
  - `-h` or `--help`: Show the help message and exit.
  - `--output OUTPUT`: Output file path (default: print to stdout).
  - `--D D`: Number of dimensions (default: 2).
  - `--A_group_ratio A_GROUP_RATIO`: Group A ratio (default: 0.9).
  - `--A_centroid_ratio A_CENTROID_RATIO`: Centroid A ratio (default: 0.5).
  - `--val_min VAL_MIN`: Minimum value for centroids (default: -1).
  - `--val_max VAL_MAX`: Maximum value for centroids (default: 1).
  - `--std_dev STD_DEV`: Standard deviation for Gaussian distribution (default: 0.1).

## How to run `G11HW2.py`

This script implements the fair k-means clustering algorithm.

- Run `python G11HW2.py [OPTIONS] data_path L K M` to run the script.
  - `data_path` is the path to the dataset. The dataset should be in the format `x1,x2,...,xn,label` with `x1,x2,...,xn` being the features coordinates (float) and `label` being the demographic group `A` or `B`.
  - `L` Number of partitions.
  - `K` Number of centroids.
  - `M` Number of iterations.
  > `OPTIONS` are the optional arguments
  - `-h` or `--help`: show the help message and exit.

- Run `./run.sh` in root to test the implementation on a small dataset.

## How to run `G11HW2.py` (CloudVeneto cluster)

### 1. Move the script

- Run `scp G11HW2.py <dei_username>@login.dei.unipd.it:.` to copy the file to the cluster.

- Run `ssh <dei_username>@login.dei.unipd.it` to connect to the cluster.

- Run `scp -P 2222 G11HW2.py group11@147.162.226.106:.` inside the cluster to move the file to the "frontend" machine. Password is `group11pwd`.
 
- Run `ssh -p 2222 group11@147.162.226.106` to connect to the "frontend" machine. Password is `group11pwd`.

### 2. Run the script

- Run `hdfs dfs -ls /data/BDC2425` to check the available datasets. The following are the available datasets:
  - `/data/BDC2425/artificial1M7D100K.txt`
  - `/data/BDC2425/artificial4M7D100K.txt`

- Run `spark-submit --num-executors X G11HW2.py argument-list` to run the script.

### 3. Run tests on `/data/BDC2425/artificial4M7D100K.txt`
  
- `spark-submit --num-executors 2 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
- `spark-submit --num-executors 4 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
- `spark-submit --num-executors 8 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
- `spark-submit --num-executors 16 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`

> If the script is too slow, run tests on `/data/BDC2425/artificial1M7D100K.txt` instead.

## Authors

- João Fonseca
- Milica Masic
