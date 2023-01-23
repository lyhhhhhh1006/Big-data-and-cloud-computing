# Assignment on Hadoop Streaming

**You should thoroughly read through the entire assignment before beginning your work! Don't start the cluster until you are ready.**

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

* `instance-metadata.json`
* `master-instance.json`
* `extra-master-instance.json`
* `quazyilx-streaming-command.bash`
* `quazyilx-failures.txt`
* `explain-mapper-only.txt`
* `log-streaming-command.bash`
* `mapper.py`
* `reducer.py`
* `logfile-counts.csv`


## Cluster Setup

1. Start an EMR cluster as outlined on the course website with **1** _master node_ and **7** _core nodes_ of `m5.xlarge` instance type. Once the cluster is up and running, connect to the master node using agent forwarding and install and configure `git`. Make sure to modify the master node security group if it's your first time starting a cluster.

2. Clone this repository to the master node using ssh repo url.

3. Change directory into the lab

### Reminders

* All files must be within the repository directory otherwise git will not see them.
* Commit frequently on the master node and push back to GitHub as you are doing your work. **If you terminate the cluster and you did not push to GitHub, you will lose all your work.**
* Data in the cluster's HDFS will be lost when the cluster terminates. If you want to keep data, store it in S3.

## Provide the Master Node and Cluster Metadata

Once you are ssh'd into the master node, query the instance metadata and write to a file:

```
curl http://169.254.169.254/latest/dynamic/instance-identity/document/ > instance-metadata.json
```

Also, since you are using a cluster, please provide some metadata files about your cluster. Run the following commands:

```
cat /mnt/var/lib/info/instance.json > master-instance.json
cat /mnt/var/lib/info/extraInstanceData.json > extra-master-instance.json
```

## Problem 1 - The _quazyilx_ scientific instrument (3 points)

For this problem, you will be working with data from the _quazyilx_ instrument. The files you will use contain hypothetic measurements of a scientific instrument called a _quazyilx_ that has been specially created for this class. Every few seconds the quazyilx makes four measurements: _fnard_, _fnok_, _cark_ and _gnuck_. The output looks like this:

    YYYY-MM-DDTHH:MM:SSZ fnard:10 fnok:4 cark:2 gnuck:9

(This time format is called [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) and it has the advantage that it is both unambiguous and that it sorts properly. The Z stands for _Greenwich Mean Time_ or GMT, and is sometimes called _Zulu Time_ because the [NATO Phonetic Alphabet](https://en.wikipedia.org/wiki/NATO_phonetic_alphabet) word for **Z** is _Zulu_.)

When one of the measurements is not present, the result is displayed as negative 1 (e.g. `-1`). 

The quazyilx has been malfunctioning, and occasionally generates output with a `-1` for all four measurements, like this:

    2015-12-10T08:40:10Z fnard:-1 fnok:-1 cark:-1 gnuck:-1

There are four different versions of the _quazyilx_ file, each of a different size. As you can see in the output below the file sizes are 50MB (1,000,000 rows), 4.8GB (100,000,000 rows), 18GB (369,865,098 rows) and 36.7GB (752,981,134 rows). The only difference is the length of the number of records, the file structure is the same. 

```
[hadoop@ip-172-31-1-240 ~]$ hadoop fs -ls s3://bigdatateaching/quazyilx/
Found 4 items
-rw-rw-rw-   1 hadoop hadoop    52443735 2018-01-25 15:37 s3://bigdatateaching/quazyilx/quazyilx0.txt
-rw-rw-rw-   1 hadoop hadoop  5244417004 2018-01-25 15:37 s3://bigdatateaching/quazyilx/quazyilx1.txt
-rw-rw-rw-   1 hadoop hadoop 19397230888 2018-01-25 15:38 s3://bigdatateaching/quazyilx/quazyilx2.txt
-rw-rw-rw-   1 hadoop hadoop 39489364082 2018-01-25 15:41 s3://bigdatateaching/quazyilx/quazyilx3.txt
```

Your job is to find all of the times where the four instruments malfunctioned together using `grep` with Hadoop Streaming. 

You will run a Hadoop Streaming job using the 18GB file as input. **First, copy the 18GB file from the bigdatateaching S3 bucket into your own S3 bucket**

Here are the requirements for this Hadoop Streaming job:

* The *mapper* is the `grep` function. 
* It is a map only job and must be run as such

You need to issue the command to submit the job with the appropriate parameters. [The reference for Hadoop Streaming commands is here.](https://hadoop.apache.org/docs/r3.2.1/hadoop-streaming/HadoopStreaming.html).

Paste the command you issued into a text file called `quazyilx-streaming-command.bash`.  

Once the Hadoop Streaming job finishes, create a text file called `quazyilx-failures.txt` with the results which **must be sorted by date and time.**

Explain why this is a map only job. Provide a short answer in the `explain-mapper-only.txt` file.

The files to be committed to the repository for this problem are `quazyilx-streaming-command.bash`, `quazyilx-failures.txt`, and `explain-mapper-only.txt`

## Problem 2 - Log file analysis (7 points)

The file `s3://bigdatateaching/forensicswiki/2012_logs.txt` is a year's worth of Apache logs for the [forensicswiki website](http://forensicswiki.org/wiki/Main_Page). Each line of the log file correspondents to a single `HTTP GET` command sent to the web server. The log file is in the [Combined Log Format](https://httpd.apache.org/docs/1.3/logs.html#combined).

**Start off by copying the file from bigdatateaching into your own S3 bucket!** Use the lab materials to find the command to do this.

Your goal in this problem is to report the number of hits for each month. Your final job output should look like this:

    2010-01,xxxxxx
    2010-02,yyyyyy
    ...

Where `xxxxxx` and `yyyyyy` are replaced by the actual number of hits in each month.

You need to write a Python `mapper.py` and `reducer.py` with the following requirements:

* You must use regular expressions to parse the logs and extract the date, and cannot hard code any date logic 
* Your mapper should read each line of the input file and output a key/value pair **tab separated format**
* Your reducer should tally up the number of hits for each key and output the results in a **comma separated format**

You need to run the Hadoop Streaming job with the appropriate parameters. Save the command into a bash file called `log-streaming-command.bash`

Once the Hadoop Streaming job finishes, create a text file called `logfile-counts.csv` with the results which **must be sorted by date.**

The files to be committed to the repository for this problem are `log-streaming-command.bash`, `mapper.py`, `reducer.py` and `logfile-counts.csv`.

## Grading Rubric

Many of the assignments you will work on are open-ended. Grading is generally holistic, meaning that there will not always be specific point value for individual elements of a deliverable. Each deliverable submission is unique and will be compared to all other submissions.

- If a deliverable exceeds the requirements and expectations, that is considered A level work.
- If a deliverable just meets the requirements and expectations, that is considered A-/B+ level work.
- If a deliverable does not meet the requirements, that is considered B or lesser level work.

All deliverables must meet the following general requirements, in addition to the specific requirements of each deliverable:

If your the submission meets or exceeds the requirements, is creative, is well thought-out, has proper presentation and grammar, and is at the graduate student level, then the submission will get full credit. Otherwise, partial credit will be given and deductions may be made for any of the following reasons:

Points will be deducted for any of the following reasons:

- Any instruction is not followed
- There are missing sections of the deliverable
- The overall presentation and/or writing is sloppy
- There are no comments in your code
- There are files in the repository other than those requested
- There are absolute filename links in your code
- The repository structure is altered in any way
- Files are named incorrectly (wrong extensions, wrong case, etc.)

