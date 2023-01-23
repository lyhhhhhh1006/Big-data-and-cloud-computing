git clone https://github.com/gu-anly502/fall-2022-a04-lyhhhhhh1006.git
cd fall-2022-a04-lyhhhhhh1006
git add .
git commit -m"f"
git pull
hadoop fs -cat s3://anly502-fall-2022-yl1353/2012_logs.txt | python mapper.py | python reducer.py
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -files mapper.py,reducer.py -input s3://anly502-fall-2022-yl1353/2012_logs.txt -output filelogcount -mapper mapper.py -reducer reducer.py
hadoop fs -cat filelogcount/* |sort -k 1 > logfile-counts.csv
history | cut -c 8- > log-streaming-command.bash
