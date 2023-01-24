![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Lab

## Goals

-   Creating your public/private ssh key pair and knowing where to find the files
-   Using the Linux Shell and how it is critical for the cloud
-   BASH Exercise

## Submission

You will follow the submission process for all labs and assignments:

1. Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
2. Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit only the files requested.

The files to be committed and pushed to your repository for this assignment are:

- test-environment/ and its subfolders
- bash-history.txt
- temp-numfiles.txt
- other files that came with the repo


# BASH Exercise

1. There is a test environment folder in your Git repo that needs cleaning up. **You will use BASH commands only** to organize the files in this test environment into a structure like the following:

```
test-environment/
├── docs
├── models
├── data
│   ├── input
│   └── output
├── src
│   ├── notebooks
│   └── scripts
└── temp
```

All the work on this repo and environment need to be BASH commands. That means changing directories, listing files, moving files, making directories, etc.

2A. First, create all the folders within [git repo]/test-environment/ as shown in the tree diagram above

2B. Remove the files that are marked with an "old" or "DELETE". You must do this using **globbing**.

2C. Use pipes and arrows to find the number of files in `test-environment` folder and save into a file called `temp-numfiles.txt`. This step must be accomplished in one command.

2D. Now navigate through your directory structure or effectively use `..` to move all the files into the appropriate folders. If you have any concerns about where the files should end up, run a comment command in BASH to justify your approach. The comment character in BASH is `#`. **You must use globbing at least two twice during your moving operations.**
  
  - temp-related files go into the temp folder
  - .csv files go into the appropriate data subfolders
  - .onyx files go into the models folder
  - .md files go into the docs folder
  - .ipynb and .py go into the appropriate src subfolders
  

2E. List the contents of your `test-environment` to show all the contents within this folder tree. Hint: are there flags that could help you avoid listing 7 directories?

2F. Add, commit, and push your organized repo to GitHub

3. Save your history of commands into a file in your Git repo using the command `history > bash-history.txt`. Add, commit, and push this file to your remote GitHub repo. Make sure you follow the steps in order, we are expecting to see multiple commits!
