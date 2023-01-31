# Lab: Spark Streaming

## Goals

- Registering for a data vendor API key
- Modifying permissions on AWS
- Sending data to a data stream
- Consuming data from a data stream

## Lab Steps

1. Start an EMR cluster as outlined on the course website with 1 _master_ node and 1 or 2 _core_ nodes of `m5.xlarge` instance type up to cluster terminal access. Make sure you use port 8765 for local port forwarding and that you use ssh agent forwarding so that you can push files to GitHub!

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

- `README.md`
- `.gitnignore`
- `soln.json`
- `SparkStreamingConsumer.ipynb`
- `SparkStreamingProducer.ipynb`
- `imgs/`


# Finnhub Signup

Finnhub.io is a a website that offers a fantastic free API to access stock and cryptocurrency trading data. The API releases data in realtime and can be accessed through a REST interface or a websocket interface. The REST API means that we can make a request through the internet to get data from the website. The websocket creates a more persistent connection to pass us data. For more information about REST APIs go [here](https://manyos.it/blog/2019/9/3/background-what-are-rest-apis-why-do-we-use-it). For more information about websockets go [here](https://www.fullstackpython.com/websockets.html). A deep understanding of these terms is not essential for doing well in the class.

## Step 1

Go to [finnhub.io](finnhub.io) and click on the green `Get free API key` button.

![](imgs/finnhub_0.PNG)

## Step 2

Enter your name, email, and password into the sign up box, then press `Sign Up`.

![](imgs/finnhub_1.PNG)

The registration process will take a few seconds and you will see a loading icon.

![](imgs/finnhub_2.PNG)

## Step 3

Meanwhile, go to the email you entered and verify your email address in the email you should have just received. It will look like the email below.

![](imgs/finnhub_4.PNG)

## Step 4

Now, go to the finnhub [dashboard](finnhub.io/dashboard) and copy the alphanumeric string in the API Key section into a notepad on your laptop so you have it ready for later.

![](imgs/finnhub_3.PNG)


# Amazon IAM Permissions

Amazon permissions are managed by the IAM (Identity and Access Management) Service. In order for us to use Amazon Kinesis Stream, we need to attach a security policy to the role that has been automatically created for us when we made our EMR clusters.

## Step 1

Start off by searching for `iam` in the AWS Console and then click on the IAM service as shown in the figure below.

![](imgs/iam_0.PNG)

## Step 2

Click on the number under the **Roles** section of IAM Resources (yellow arrow).

![](imgs/iam_1.PNG)

## Step 3

Look for the role name called `EMR_EC2_DefaultRole` and click on it (yellow arrow). You may have to scroll through multiple pages of roles to find it depending on how many roles you have in your account.

![](imgs/iam_2.PNG)

## Step 4

Click the `Add permissions` Button and then the `Attach policies` button on the right side of the screen.

![](imgs/iam_3.PNG)

## Step 5

Search for "kinesis" in the search bar (click `Enter` to search) and check the item `AmazonKinesisFullAccess` from the list. You only need to check that one item. Once you have done that click on the `Attach policy` blue button.

![](imgs/iam_4.PNG)

## Step 6

You know you modified the permissions successfully if you see the green bar at the top of the screen.

![](imgs/iam_5.PNG)

# Jupyter Lab

## SparkStreamingProducer.ipynb

In this notebook, you will query from the Finnhub websocket to collect live bitcoin trading data and send the data into an Amazon Kinesis Stream.

## SparkStreamingConsumer.ipynb

In this notebook, you will use PySpark Structured Streaming to process data from the Amazon Kinesis Stream and analyze it using essential PySparkSQL tools to gain analytical insights on the live price in realtime.


## MAKE SURE YOU DELETE KINESIS STREAM AND SHUT DOWN EMR CLUSTER

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
