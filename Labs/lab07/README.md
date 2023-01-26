# Lab: SparkSQL and Spark DataFrame

# Due: October 18, 2022

## Goals

-   Set up your Spark EMR cluster
-   Start using SparkSQL

## Lab Steps

1. Start an EMR cluster as outlined on the course website with 1 _master_ node and 2 _core_ nodes of `m5.xlarge` instance type up to cluster terminal access. Make sure you use port 8765 for local port forwarding and that you use ssh agent forwarding so that you can push files to GitHub!

2. Set up your git credentials using the suggested commands in the terminal.

3. Confirm Git access is set up by running the command `ssh git@github.com`

4. Clone this repository to the master node by running the command `git clone <repo ssh url>`

5. Log into Spark according to the steps outlined in the included html file.

6. Git add, commit, and push your notebook from the Lab

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

Your submission repo requires the following:

- `/reference/....`
- `README.md`
- `SparkSQLLab.ipynb`
- `lab-solution.json`

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
