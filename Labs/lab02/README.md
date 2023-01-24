![Open in Codespaces](https://classroom.github.com/assets/open-in-codespaces-abfff4d4e15f9e1bd8274d9a39a0befe03a0632bb0f153d0ec72ff541cedbe34.svg)
# Lab: The Cloud

In this lab, you will setup your [Microsoft Azure](https://azure.microsoft.com/en-us/) and [Amazon Web Services (AWS)](https://aws.amazon.com/) cloud accounts. You will be using these going forward.

We will go step-by-step and slow to make everyone completes the steps they need.

Note: remember you need to be on the **Saxanet** wifi if you are on campus

## Activity 1: Setup your cloud accounts

1. Follow the [Amazon Web Services Setup Instructions](https://marckvaisman.georgetown.domains/anly502/labs/lab2/aws-setup.html). Do tasks 1 through 6. Tasks 7 and 8 are optional.
1. Follow the [Microsoft Azure Setup Instructions](https://marckvaisman.georgetown.domains/anly502/labs/lab2/azure-setup.html). Do tasks 1 through 5. 
1. Make sure that you start virtual machines in both platforms and that you are able to connect to them. 


## Activity 2: Practice `ssh` with agent forwarding

1. Get the IP addresses of **both** the VM on Azure, and the VM on AWS
1. Start the ssh agent forwarding process:
	- On MacOS just type `ssh-add` in the terminal. You will see something like this: `Identity added: /Users/xxxx/.ssh/id_rsa`
	- On Windows, please follow ...

2. SSH with agent forwarding from your terminal (local machine) into the **Azure virtual machine**:

	On MacOS
	
	```
	ssh-add
	ssh -A azureuser@xxx.xxx.xxx.xxx
	```

	On Windows
	
	```
	If `ssh-add` didn't work for you on Windows, try `Start-Service ssh-agent` first.
	```
	
	

3. SSH ssh with agent forwarding from the Azure VM into the AWS VM:


	```
	ssh -A ec2-user@yyy.yyy.yyy.yyy
	```
	
	**The next set of steps happen on the AWS VM.**

4. Install git:

	```
	sudo yum install git
	```

	Then enter `y` when you get a message like `Is this ok [y/d/N]:`

5. Clone your lab repo and change into that directory:

	```
	git clone <repo-ssh-link>
	cd <repo-name>
	```

6. Query the AWS metadata into the `aws-metadata.json` file by copy/pasting this command:

	```
	curl http://169.254.169.254/latest/dynamic/instance-identity/document/ > aws-metadata.json
	```

7. Commit (on AWS machine) and push to GitHub:

	```
	git add .
	git commit -m "Add AWS metadata file"
	git push
	```

8. Exit the AWS vm, so that you are back on the Azure vm:

	```
	exit
	```
	**The next few steps happen on the Azure VM**

9. Clone your repo and change into that directory (no need to install git on Azure):

	```
	git clone <repo-ssh-link>
	cd <repo-name>
	```

10. Double-check that you pushed `aws-metadata.json` successfully:

	```
	ls
	```

11. Query the Azure metadata into the `azure-metadata.json` file by copy/pasting this command:

	```
	curl -H Metadata:true 169.254.169.254/metadata/instance?api-version=2020-09-01 > azure-metadata.json
	```

12. Commit (on Azure machine) and push to GitHub:


	```
	git add .
	git commit -m "Add Azure metadata file"
	git push
	```

13. Exit the Azure vm, so that you are back local:

	```
	exit
	```

14. Double-check your repo on GitHub to make sure both files are there


## Activity 3 (Sagemaker, OPTIONAL)

Time permitting we would have a quick walkthrough of SageMaker Studio Notebooks. 


## Submitting the Activity
Make sure you commit and push your repository to GitHub!

The files to be committed and pushed to the repository for this lab are:

* `aws-metadata.json`
* `azure-metadata.json`

**Don't forget to stop your virtual machines on both Azure and AWS.**

