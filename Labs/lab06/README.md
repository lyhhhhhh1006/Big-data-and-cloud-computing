![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Lab: Intro to Spark

## Goals

-   Set up your Spark EMR cluster
-   Run Basic Spark Notebooks
-   Start using Spark RDDs

## Lab Steps

1. Start an EMR cluster as outlined in the Spark guide from this repo with 1 _master_ node and 2 _core_ nodes of `m5.xlarge` instance type up to cluster terminal access. Make sure you use port 8765 for local port forwarding and that you use ssh agent forwarding so that you can push files to GitHub!

2. Set up your git credentials using the suggested commands in the terminal.

3. Confirm Git access is set up by running the command `ssh git@github.com`

4. Clone this repository to the master node by running the command `git clone <repo ssh url>`

5. Log into Spark according to the steps outlined on the course website

6. Git add, commit, and push your notebooks from Lab

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

Your submission repo requires the following:

- `/reference/....`
- `.gitignore`
- `README.md`
- `SparkConnectionGuide.ipynb`
- `SparkRDDLab.ipynb`
- `soln.json`

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


## Reference Material

### Spark shell (using Scala)

We didn't discuss this much in class. One of the ways to have an interactive session with Spark is by using the spark-shell. This starts an interactive, text based environment where you interact with the cluster using Scala.

Try it out! Type `spark-shell` in the command line. You'll know its Scala because you will see a Scala prompt. You can exit the spark-shell by using the `Ctrl-D` key combination.

```
[hadoop@ip-172-31-62-160 ~]$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
17/06/27 21:18:02 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
17/06/27 21:18:03 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark context Web UI available at http://172.31.62.160:4041
Spark context available as 'sc' (master = yarn, app id = application_1498593832174_0002).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.0
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_121)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

### PySpark shell (regular Python shell)

You can connect to Spark using PySpark, which runs a copy of the Python interpreter that's connected to the Spark runtime. As you can see, you see the Python version. You can exit this shell by typing quit(). If you had not run the scripts you ran after you started the cluster, this is what you would have to use Python with PySpark and Spark. 

```
[hadoop@ip-172-31-30-120 ~]$ pyspark
Python 2.7.13 (default, Jan 31 2018, 00:17:36)
[GCC 4.8.5 20150623 (Red Hat 4.8.5-11)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/03/12 14:43:52 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.2.1
      /_/

Using Python version 2.7.13 (default, Jan 31 2018 00:17:36)
SparkSession available as 'spark'.
>>>
```

### PySpark shell (using iPython - Text Based)

Another way to use PySpark is by telling PySpark to use the iPython shell. Since we installed iPython on this cluster (not installed by default), you need to setup an environment variable before starting PySpark and you will see the difference:

```
[hadoop@ip-172-31-30-120 ~]$ PYSPARK_DRIVER_PYTHON=ipython pyspark
Python 2.7.13 (default, Jan 31 2018, 00:17:36)
Type "copyright", "credits" or "license" for more information.

IPython 5.4.0 -- An enhanced Interactive Python.
?         -> Introduction and overview of IPython's features.
%quickref -> Quick reference.
help      -> Python's own help system.
object?   -> Details about 'object', use 'object??' for extra details.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/03/12 14:58:56 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
18/03/12 14:58:59 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.2.1
      /_/

Using Python version 2.7.13 (default, Jan 31 2018 00:17:36)
SparkSession available as 'spark'.

In [1]: spark
Out[1]: <pyspark.sql.session.SparkSession at 0x7ff289c25bd0>

In [2]: sc
Out[2]: <SparkContext master=yarn appName=PySparkShell>

In [3]:
```

