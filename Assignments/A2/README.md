![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Assignment: Data Science using Shell

Due date: Thursday, September 15
5 points

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

  - `README.md`
  - `NCBirths2004.csv`
  - `headers.txt`
  - `smoker-yes-med.txt`
  - `smoker-no-med.txt`
  - `data-script.bash`
  - `bash-stdout.txt`
  - IF DOING BONUS: `alcohol-yes-avg.txt`, `alcohol-no-avg.txt`
  - `bash-history.txt`

## Shell Exercise

Goals:

1. After cloning this repository, launch GitHub Codespaces just like in lab. (follow steps from previous labs if you need a refresher). You will conduct this entire assignment in Codespaces.

2. Your Git repository contains a data file. **You will use "basic" BASH commands only** to create summary statistics about the dataset `NCBirths2004.csv`. Let us emphasize here - **BASIC** ONLY. That means no data science/csv/etc. packages that you install. If it is not part of Codespaces and is not "basic" then you cannot use it. If you are uncertain if you can use a certain command, please ask the instructional team.

All the work on this repo and environment need to be BASH commands. That means changing directories, listing files, moving files, making directories, searching, doing math, etc.

2. Examine the data file using BASH commands

3. Find the column names of the data file and save to a file called `headers.txt`

4. We want to make our BASH code reproducible so that we could run this process automatically if we wanted to. 

Save the command to accomplish step 5 into a file called `data-script.bash`. Start off by experimenting in the console, and eventually use the editor to put the code into the BASH file. Make sure you comment code appropriately!

5. Let's do a group-by and summarize using BASH commands only! Get the **median** `Weight` of babies for smokers and non-smokers in the dataset. 

This will require you to do some Googling since we have not explicitly gone through all the commands needed. That's OK because we have discussed help pages and command syntax so you should be able to find the answers needed to get to a result. We are not looking for a unique (re-invent the wheel) solution. I want you to use the skills you know, and then use Google, StackOverflow, etc. to learn the additional tools to solve the problem at hand. You do not have to complete each step in a single command. But you must be able to run your `data-script.bash` file to accomplish the process from scratch.

Here are the requirements:

1. Filter the dataset to only rows where the `Smoker` variable is `Yes`. There are many ways to accomplish this.
2. Select the Weight variable only 
3. Get the average of all the numbers - (make sure you do not include the variable name!)
4. Save the average into a file called `smoker-yes-med.txt`. We are expecting nothing but the numerical result and will take off points if you included prints into your text files.
5. Repeat the process where `Smoker` variable is `No` and change the file name accordingly.
6. Print the contents of both average files and include identifying info for each number so we know which is which!
7. In a comment, (`#` for comment), write a summary NON-TECHNICAL statement describing the relationship between average weight for smoking and non-smoking mothers.

**BONUS** (1 point): Follow step 5 but this time find the **average** `Weight` of babies for mothers where Alcohol==Yes and Alcohol==No.

6. Run the command `bash data-script.bash > bash-stdout.txt` to show what happens when you run the entire script from scratch. This will save the stdout to a text file for evaluation. **Make sure your bash file has comments and print statements**

7. Add, commit, and push your organized repo to GitHub. The files included must be:
  - `README.md`
  - `NCBirths2004.csv`
  - `headers.txt`
  - `smoker-yes-med.txt`
  - `smoker-no-med.txt`
  - `data-script.bash`
  - `bash-stdout.txt`
  - IF DOING BONUS: `alcohol-yes-avg.txt`, `alcohol-no-avg.txt`

8. Copy your bash history into your Git repo like in the lab and save it to `bash-history.txt`. Add, commit, and push this file to your remote GitHub repo. Make sure you follow the steps in order, we are expecting to see multiple commits!

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

