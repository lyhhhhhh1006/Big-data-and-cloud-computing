![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Assignment - Dask

**You should thoroughly read through the entire assignment and go over the contents of the lab before beginning your work.**

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message.

Make sure you commit **only the files requested**, and push your repository to GitHub!

* `dask.ipynb`
* `dask-report-task1.html`
* `task1.csv`
* `dask-report-task2.html`
* `task2.csv`

## Problem

Use the `dask.ipynb` notebook in this repo to get started. Add your code in the `dask.ipynb` notebook, it already has code to create a local dask cluster on your notebook. Make sure you run the notebook on a `ml.t3.2xlarge` instance (8 vCPU + 32GiB), this instance is visible once you disable _Fast Launch Only_ on SageMaker Studio Notebooks (see [this link](https://aws.amazon.com/blogs/machine-learning/learn-how-to-select-ml-instances-on-the-fly-in-amazon-sagemaker-studio/) for help).

### [TASK 1] The quazyilx scientific instrument (5 points)

For this problem, you will be working with data from the quazyilx instrument. The files you will use contain hypothetic measurements of a scientific instrument called a quazyilx that has been specially created for this class. Every few seconds the quazyilx makes four measurements: fnard, fnok, cark and gnuck. The output looks like this:

`YYYY-MM-DDTHH:MM:SSZ fnard:10 fnok:4 cark:2 gnuck:9`
(This time format is called ISO-8601 and it has the advantage that it is both unambiguous and that it sorts properly. The Z stands for Greenwich Mean Time or GMT, and is sometimes called Zulu Time because the NATO Phonetic Alphabet word for Z is Zulu.)

When one of the measurements is not present, the result is displayed as negative 1 (e.g. -1).

The quazyilx has been malfunctioning, and occasionally generates output with a -1 for all four measurements, like this:

`2015-12-10T08:40:10Z fnard:-1 fnok:-1 cark:-1 gnuck:-1`

Your job is to find all of the times where the four instruments malfunctioned together using grep with Hadoop Streaming.

You will run a Dask job using the 18GB file `s3://bigdatateaching/quazyilx/quazyilx2.txt` as input. **First, copy the 18GB file from the bigdatateaching S3 bucket into your own S3 bucket**

**<u>Submission Requirements</u>**

1. A file called `dask-report-task1.html` which contains the dask performance report generated via the [performance_report function](https://distributed.dask.org/en/stable/diagnosing-performance.html). You should include the final Dask operation that you do in this performance report.

1. A file called `task1.csv` that contains all the lines containing `fnard:-1,fnok:-1,cark:-1,gnuck:-1`.

### [TASK 2] Log file analysis (5 points)

The file `s3://bigdatateaching/forensicswiki/2012_logs.txt` is a year's worth of Apache logs for the forensicswiki website. Each line of the log file correspondents to a single HTTP GET command sent to the web server. The log file is in the Combined Log Format.

Start off by copying the file from bigdatateaching into your own S3 bucket! Use the lab materials to find the command to do this.

Your goal in this problem is to report the number of hits for each month. Your final job output should look like this:
```
2010-01,xxxxxx
2010-02,yyyyyy
...
...
```
Where xxxxxx and yyyyyy are replaced by the actual number of hits in each month. **First, copy the `s3://bigdatateaching/forensicswiki/2012_logs.txt` file from the bigdatateaching S3 bucket into your own S3 bucket**

**<u>Submission Requirements</u>**

1. A file called `dask-report-task2.html` which contains the dask performance report generated via the [performance_report function](https://distributed.dask.org/en/stable/diagnosing-performance.html).  You should include the final Dask operation that you do in this performance report.

1. A file called `task2.csv` that contains the output in the format:
```
2010-01,xxxxxx
2010-02,yyyyyy
...
...
```

