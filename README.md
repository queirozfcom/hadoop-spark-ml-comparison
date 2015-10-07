# Hadoop X Spark - Comparing Performance for Workloads 
Code used for university coursework aimed at comparing Hadoop and Spark functionality, with Machine Learning (ML) tasks in mind

This project consists of 2 (plus one discarded task) tasks we ran in order to compare the performance of Hadoop MapReduce and Spark, as well as Mahout (running on top of Hadoop MapReduce) and Spark's MLLib machine learning library.

> All code used for the experiments are in this repository!

## The experiments

All experiments were executed on AWS infrastructure - more specifically, AWS ElasticMapReduce, on 1, 2, 4, 8 and 16 m3.xlarge nodes.

**Wordcount**

We ran a standard wordcount experiment over a large dataset using Hadoop and Spark, the results being as follows:

![results1](http://i.imgur.com/qvy6czI.png)

**Distributed KMeans**

We ran the Distributed KMeans algorithms on Mahout (on top of Hadoop MapReduce) and on Spark's MLLib:

![results2](http://i.imgur.com/HwTGUVh.png)

**Naïve Distributed KMeans**

We had also intended to include a naïve implementation of Distributed KMeans (as can be seen under `naive_kmeans`) for Hadoop MapReduce and Spark. While the Spark implementation went OK, the Hadoop version did not finish after a long wait so we decided against inluding it in the results.

## Findings

- For non-iterative tasks Spark starts off better than Hadoop (w.r.t. execution time) but Hadoop catches up with Spark

- For iterative tasks Spark performs much better than Hadoop.

- For all tasks, enabling Spark's `dynamicAllocation` led to massive performance gains.


Special thanks to [Julian McAuley](http://cseweb.ucsd.edu/~jmcauley/) for letting us use the dataset prepared by him.

