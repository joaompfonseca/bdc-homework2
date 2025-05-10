# BDC Homework 2 - Fair K-Means Clustering

## Description

The purpose of the second homework is to implement the variant of Lloyd's algorithm for fair k-means clustering, proposed in the paper [Socially Fair k-Means Clustering (ACM FAccT'21)](https://dl.acm.org/doi/10.1145/3442188.3445906), and to compare its effectiveness against the standard variant, with respect to the new objective function introduced in that paper. Moreover, the scalability of the implementation should be tested on the CloudVeneto cluster available for the course.

**Course:** Big Data Computing (2024/2025).

## Test the implementation locally

- Run `./run.sh` in root to test the implementation on a small dataset.

## Test the implementation on the cluster

### Moving the script

- Run `scp G11HW2.py <dei_username>@login.dei.unipd.it:.` to copy the file to the cluster.

- Run `scp -P 2222 G11HW2.py group11@147.162.226.106:.` inside the cluster to move the file to the "frontend" machine. Password is `group11pwd`.

### Running the script

- Run `ssh <dei_username>@login.dei.unipd.it` to connect to the cluster.

- Run `ssh -p 2222 group11@147.162.226.106` to connect to the "frontend" machine. Password is `group11pwd`.

- Run `hdfs dfs -ls /data/BDC2425` to check the available datasets. The following are the available datasets:
  - ~~`/data/BDC2425/HIGGS500K7Dclass.txt`~~
  - `/data/BDC2425/artificial1M7D100K.txt`
  - `/data/BDC2425/artificial4M7D100K.txt`
  - ~~`/data/BDC2425/artificial11M7D100K.txt`~~
  - ~~`/data/BDC2425/orkut1Mclass.txt`~~
  - ~~`/data/BDC2425/orkut4Mclass.txt`~~

- Run `spark-submit --num-executors X G11HW2.py argument-list` to run the script.

  - Perform the tests on `/data/BDC2425/artificial4M7D100K.txt`:
    - `spark-submit --num-executors 2 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 4 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 8 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 16 G11HW2.py /data/BDC2425/artificial4M7D100K.txt 16 100 10`

  - In case implementation is slow (>10 minutes) perform the tests on `/data/BDC2425/artificial1M7D100K.txt`:
    - `spark-submit --num-executors 2 G11HW2.py /data/BDC2425/artificial1M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 4 G11HW2.py /data/BDC2425/artificial1M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 8 G11HW2.py /data/BDC2425/artificial1M7D100K.txt 16 100 10`
    - `spark-submit --num-executors 16 G11HW2.py /data/BDC2425/artificial1M7D100K.txt 16 100 10`
## Authors

- João Fonseca
- Milica Masic
