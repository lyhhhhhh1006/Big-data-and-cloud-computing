# Problem Set - Python Skills (10 points)

## DUE MONDAY, AUGUST 29 11:59PM

You will be performing several Python exercises in this assignment to build and/or reinforce skills that will be used later in the semester.

You will be doing all the exercises on your laptop with your preferred Python setup. We recommend you use an IDE like VSCode or Jupyter Lab so you can develop iteratively and interactively.

## Requirements:

* You may only use **core** Python libraries (including `re`, `os`, `argparse`, `sys`, etc.)  
* Thus your python environment does not really matter (as long as you are using Python3!)
* If you are using a Windows machine, the system commands included may be slightly different for you. Install [Git Bash](https://git-scm.com/downloads) and use the Git Bash application to execute your code.

## Submission

You will follow the submission process for all labs and assignments:

1. Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
2. Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. **Do not change your GitHub repo after submitting the "final-submission" commit message**

Make sure you commit **only the files requested**.

The files to be committed and pushed to your repository for this assignment are:

* `missing.py`
* `words.py`
* `babynames.py`
* `gutenberg-top50.txt`
* `babysummary-2006-top50.txt`

## Instructions

1. Clone this repository
2. Change your current working directory into the repository
3. Do the work. Remember, all files must be within the repository directory otherwise git will not see them.
4. Remember to commit and push often (this is important when working in the cloud.)

## Problem 1

Write a program `missing.py` that takes two arguments as a command line argument: an integer `N`, and a set of distinct integers `N - 1` separated by a space as a string. The program should print out the missing number in the set.

```
$ python missing.py 5 "1 3 2 5"
The missing number is 4.
```

The program should produce an error for any of the following conditions:

* a non-integer value is provided in the first or as part of the second argument
* the number of items provided in the second argument does not match `N-1`
* there are duplicate values in the second argument

For example:

```
$ python missing.py 5 "1 3 3 5"
Error: the second argument has duplicate values.
```

Start off by developing your code as either a Jupyter notebook or a python script. You can convert a jupyter notebook to a python script using the Linux command `jupyter nbconvert --to script [YOUR_NOTEBOOK].ipynb`. You should simulate the system arguments so that you can develop iteratively and quickly. Once the program is in good shape then add capability of running from the command line.

Open a Terminal in Jupyter Lab by going to `File` in the menu bar, then going to `New` then choosing `Terminal`.

## Problem 2

Write a program `words.py` that reads **either** a text file **or** text from [`stdin`](https://en.wikipedia.org/wiki/Standard_streams) and writes to [`stdout`](https://en.wikipedia.org/wiki/Standard_streams) all the unique words in the input in alphabetical order (one per line.)

The program should be setup in such a way that if the program gets a command line argument, it expects it to be a file name, and if not it reads `stdin`.

You can use the following command to make sure it works right: `$ cat filename | python words.py`. This should produce the same result as `$ python words.py filename`.

The `filename` file to be used for this problem is on the internet at this location:

`http://www.gutenberg.org/files/11/11-0.txt`

**You need to download the file into your repository directory, but you must not commit it.**

Output the top fifty lines from the script into a text file called `gutenberg-top50.txt`. You can accomplish this task by running a command like `$ python words.py filename | head -n 50 > gutenberg-top50.txt`

## Problem 3

For this problem, the source data files are in the babynames/ directory within this repository.

* In the `babynames.py` file, implement the *extract_names(filename)* function which takes the filename of a baby\<year\>.html file and returns the data from the file as a single list -- the year string at the start of the list followed by the name-rank strings in alphabetical order. ['2006', 'Aaliyah 91', 'Aaron 57', 'Abagail 895', ...]. 
* Modify *main()* so it calls your *extract_names()* function and prints what it returns (main already has the code for the command line argument parsing). You will be using regular expressions to parse the html. Note that for parsing webpages in general, regular expressions don't do a good job, but these webpages have a simple and consistent format.

Rather than treat the boy and girl names separately, just lump them all together. In some years, a name appears more than once in the html, but just use one number per name. Optional: make the algorithm smart about this case and choose whichever number is smaller.

Build the program as a series of small milestones, getting each step to run/print something before trying the next step. This is the pattern used by experienced programmers -- build a series of incremental milestones, each with some output to check, rather than building the whole program in one huge step.

Printing the data you have at the end of one milestone helps you think about how to re-structure that data for the next milestone. Python is well suited to this style of incremental development. For example, first get it to the point where it extracts and prints the year and calls `sys.exit(0)`. Here are some suggested milestones:

* Extract all the text from the file and print it
* Find and extract the year and print it
* Extract the names and rank numbers and print them
* Get the names data into a dict and print it
* Build the [year, 'name rank', ... ] list and print it
* Fix `main()` to use the ExtractNames list

Rather than have functions just print to standard out, it is more re-usable to have the function *return* the extracted data, so then the caller has the choice to print it or do something else with it. (You can still print directly from inside your functions for your little experiments during development.)

Have `main()` call `extract_names()` for each command line arg and print a text summary. To make the list into a reasonable looking summary text, here's a clever use of join: `text = '\n'.join(mylist) + '\n'`

The summary text should look like this for each file:

```
2006
Aaliyah 91
Aaron 57
Abagail 895
Abbey 695
Abbie 650
...
```

Run your script on the 2006 file only, and output the top 50 lines to a text file called `babysummary-2006-top50.txt` in your Git repo. You can accomplish this task by running a command like `$ python babynames.py babynames/baby2006.html | head -n 50 > babysummary-2006-top50.txt`

## Grading Rubric

Each deliverable submission is unique and will be compared to all other submissions:

* If a deliverable exceeds the requirements and expectations, that is considered A level work.
* If a deliverable just meets the requirements and expectations, that is considered A-/B+ level work.
* If a deliverable does not meet the requirements, that is considered B or lesser level work.

All deliverables must meet the following general requirements, in addition to the specific requirements of each deliverable:

If the submission meets or exceeds the requirements, is creative, is well thought-out, has proper presentation and grammar, and is at the graduate student level, then the submission will get full credit. Otherwise, partial credit will be given and deductions may be made for any of the following reasons:

Points will be deducted for any of the following reasons:

* Any instruction is not followed
* There are missing sections of the deliverable
* The overall presentation and/or writing is sloppy
* There are no comments in your code
* There are files in the repository other than those requested
* There are absolute filename links in your code
* The repository structure is altered in any way
* Files are named incorrectly (wrong extensions, wrong case, etc.)
