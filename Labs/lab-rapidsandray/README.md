# Lab: RAPIDS

In this lab, you will learn how to use [RAPIDS](https://rapids.ai/) for data analytics. We will use a custom container which has `rapids`, `python 3.9` and the `cudatoolkit` installed on a SageMaker Studio Notebook and use the it for analyzing the NYC-Taxi dataset.

Note: remember you need to be on the **Saxanet** wifi if you are on campus

## Activity 1: Use RAPIDS (cuDF) to find daily average of fare amount, passenger count and other interesting fields

1. *This will be a show and tell by your instructor*. We will be reading the [parquet files for 2021](s3://bigdatateaching/nyctaxi-yellow-tripdata/2021/) from the NYC-taxi dataset. RAPIDS uses GPUs to accelerate analytics and ML workloads. See [this article](https://studiolab.sagemaker.aws/import/github/rapidsai-community/rapids-smsl/blob/main/rapids-smsl.ipynb) for creating a conda environment with rapids and cudatoolkit (this will not work on SageMaker Studio). See [this repo](https://github.com/aws-samples/sagemaker-studio-custom-image-samples/tree/main/examples/rapids-image) for creating a custom container for SageMaker Studio. We will be using the `ml.g4dn.xlarge` instance for this demo.

## Activity 2: Find out anomalies in `fare_amount` using RAPIDS

We will again be using the NYC-taxi dataset from `s3://bigdatateaching/nyctaxi-yellow-tripdata/2021/` and finding out anomalies in the `fare_amount` field. To find out the anomalies we can use the `IQR` method, so we find out the lower bound and upper bound and if a particular `fare_amount` is either lower than the Lower Bound or greater than the Upper Bound then it is an anomaly.

```
Lower Bound: (Q1 - 1.5 * IQR)  
Upper Bound: (Q3 + 1.5 * IQR)
```

Find out all the anomalies and store them in a `csv` file, check in the CSV file in your repo.

## Submitting the Activity

There is no submission for this lab!

**Don't forget to stop SageMaker Studio after you are done with the lab.**
