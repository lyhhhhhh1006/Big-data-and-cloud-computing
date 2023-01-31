# Lab: DuckDB

In this lab, you will learn how to use [DuckDB](https://duckdb.org/) for data analytics. We will install [DuckDB Python package](https://pypi.org/project/duckdb/) on a SageMaker Studio Notebook and use the it for analyzing the NYC-Taxi dataset.

Note: remember you need to be on the **Saxanet** wifi if you are on campus

## Activity 1: Use DuckDB and Apache Arrow to find daily average of fare amount, passenger count and other interesting fields

1. *This will be a show and tell by your instructor*. We will be reading the [parquet files for 2021](s3://bigdatateaching/nyctaxi-yellow-tripdata/2021/) from the NYC-taxis dataset using Apache Arrow and then use the zero-copy interface between arrow and DuckDB to have the data loaded in an `in-memory` DuckDB instance. See [this article](https://duckdb.org/2021/12/03/duck-arrow.html) on Apache Arrow and DuckDB. The analytics would be done using SQL.

## Activity 2: Find out anomalies in `fare_amount` using DuckDB

We will again be using the NYC-taxi dataset from ``s3://bigdatateaching/nyctaxi-yellow-tripdata/2021/` and finding out anomalies in the `fare_amount` field. To find out the anomalies we can use the `IQR` method, so we find out the lower bound and upper bound and if a particular `fare_amount` is either lower than the Lower Bound or greater than the Upper Bound then it is an anomaly.

```
Lower Bound: (Q1 - 1.5 * IQR)  
Upper Bound: (Q3 + 1.5 * IQR)
```

Find out all the anomalies and store them in a `csv` file, check in the CSV file in your repo.

## Submitting the Activity

Make sure you commit and push your repository to GitHub!


The files to be committed and pushed to the repository for this lab are:

* `dask_local_cluster_on_vm_demo.ipynb`
* `dask-report-quantiles.html`
* `dask-report-lr.html`
* `fare_amount.png`
* `trip_distance.png`
* `q99.csv`
* `rmse.txt`


**Don't forget to stop SageMaker Studio after you are done with the lab.**
