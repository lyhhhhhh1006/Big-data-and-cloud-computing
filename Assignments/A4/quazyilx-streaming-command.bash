git clone git@github.com:gu-anly502/fall-2022-a04-lyhhhhhh1006.git
git clone https://github.com/gu-anly502/fall-2022-a04-lyhhhhhh1006.git
cd fall-2022-a04-lyhhhhhh1006
curl http://169.254.169.254/latest/dynamic/instance-identity/document/ > instance-metadata.json
cat /mnt/var/lib/info/instance.json > master-instance.json
cat /mnt/var/lib/info/extraInstanceData.json > extra-master-instance.json
hadoop fs -ls s3://bigdatateaching/quazyilx/
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -D mapreduce.job.reduces=0 -D stream.non.zero.exit.is.failure=false -input s3://anly502-fall-2022-yl1353/quazyilx2.txt -output quazyilx-failure1 -mapper "/bin/grep\"fnard:-1 fnok:-1 cark:-1 gnuck:-1\""
hadoop fs -ls
hadoop fs -cat quazyilx-failure1/* |sort -k 1 -k 2 > quazyilx-failures.txt
git add .
git commit -m"final-submission"
git config --global user.email "yl1353@georgetown.edu"
git config --global user.name "lyhhhhhh1006"
git push
history | cut -c 8- >quazyilx-streaming-command.bash
