![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Lab: Dask

In this lab, you will learn how to use [Dask](dask.org) for data analytics. We will first see a demo of using Dask on a distributed cluster on [AWS Fargate](https://aws.amazon.com/fargate/) and then recreate the same demo on a local Dask cluster using SageMaker Studio Notebooks.

Note: remember you need to be on the **Saxanet** wifi if you are on campus

## Activity 1: Dask on AWS Fargate

1. *This will be a show and tell by your instructor*. We will be following this [blog post](https://aws.amazon.com/blogs/machine-learning/machine-learning-on-distributed-dask-using-amazon-sagemaker-and-aws-fargate/) for setting up a Dask cluster on Fargate (this part would already have been done before class).

   >Setting up a Dask cluster requires a lot of infrastructure to be setup including networking resources, IAM roles etc. all of which is handled by the cloud formation template provided by the blog post above. Just like setting up an EMR cluster, it takes a while (~20 minutes) to set this up. However, unlike an EMR cluster a cluster on Fargate can be scaled out and scaled in as needed and therefore does not need to be _terminated_ because we are not going to actually provision EC2 instances.

1. A SageMaker Notebook will be used to demonstrate connecting to the remote Dask cluster and running analytics an ML tasks. See notebook [dask_fargate_cluster_demo.ipynb](dask_fargate_cluster_demo.ipynb).

## Activity 2: Dask on a Local Cluster

1. This is the hands on activity we will all be doing in the lab, see notebook [dask_local_cluster_on_vm_demo.ipynb](dask_local_cluster_on_vm_demo.ipynb).

1. We will be cloning this repo on a SageMaker Studio Notebook as usual.

1. Running the  [dask_local_cluster_on_vm_demo.ipynb](dask_local_cluster_on_vm_demo.ipynb) cells will create and connect to a local cluster and then we will use this local cluster to run some analytics an ML tasks on the NYC taxi dataset available in S3 in the `s3://bigdatateaching/nyctaxi-yellow-tripdata/csv/2021/` folder.

## Submitting the Activity

Make sure you commit and push your repository to GitHub!


The files to be committed and pushed to the repository for this lab are:

* `dask_local_cluster_demo.ipynb`
* `dask-report-quantiles.html`
* `dask-report-lr.html`
* `fare_amount.png`
* `trip_distance.png`
* `q99.csv`
* `rmse.txt`


**Don't forget to stop SageMaker Studio after you are done with the lab.**
