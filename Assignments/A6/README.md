![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Assignment: Intro to Spark

**You should thoroughly read through the entire assignment before beginning your work! Don't start the cluster until you are ready.**

Start EMR Cluster with cluster configuration and bootstrap actions as described on the course website.

**Recommended setup:** 1 master and 5 core nodes of m5.xlarge

1. Start an EMR **Spark Cluster** as outlined on the course website with 1 (one) _master_ node and 5 (five) _core_ nodes of `m5.xlarge` instance type. **Make sure you follow the instructions carefully.** Once the cluster is in _Waiting_ state, connect to the master node using agent forwarding and configure `git` on the master node. (Do not install git on the master node, it's already installed.)

3. Clone this repository to the master node

4. Change directory into the lab

## Problem 1: Working with RDDs (3.5 points)

### Place the domains file in HDFS

* Put the file `top-1m.csv` into HDFS. Which command do you need to use for local file system -> HDFS???

Amazon maintained a list of the top 1 million Internet sites by traffic at the URL (they do not anymore). A recent copy is included in this repository which you will use for this problem. 

In this problem you will:

* Make an RDD where each record is a tuple with the (rank, site)
* Determine the number representation of top-level domains (TLDs) in the top 10,000 websites. Example TLDs are `com`, `edu` and `cn`. (Do not include the `.`). 
  * You will only look at the characters after the final period in the url

The rest of the work will be done within the [problem-1.ipynb](problem-1.ipynb) Jupyter notebook.

After you finish working on the problem, you will commit the Jupyter notebook `.ipynb` file called `problem-1.ipynb` and the results file `problem-1-soln.json`.


## Problem 2: Analytics with SparkRDDs (6.5 points)

You will perform the work for this problem within the [problem-2.ipynb](problem-2.ipynb) Jupyter notebook.

After you finish working on the problem, you will commit the Jupyter notebook `.ipynb` file called `problem-2.ipynb` and the results file `problem-2-soln.json`.


## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

The files to be committed and pushed to the repository for this assignment are:

- `README.md`
- `problem-1.ipynb`
- `problem-1-soln.json`
- `problem-2.ipynb`
- `problem-2-soln.json`
- `top-1m.csv`

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