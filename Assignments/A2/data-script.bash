awk -F ',' '(NR==1){ print $0}'  NCBirths2004.csv > headers.txt
awk -F ',' '(NR==1 || $8=="Yes"){ print $0}'  NCBirths2004.csv | cut -f 6 -d ',' | sort  -n | awk ' { a[i++]=$1; } END { print a[int(i/2)]; }' > smoker-yes-med.txt
echo 'Average weight for smoking mothers is:'
cat smoker-yes-med.txt
awk -F ',' '(NR==1 || $8=="No"){print $0}'  NCBirths2004.csv | cut -f 6 -d ',' | sort  -n | awk ' { a[i++]=$1; } END { print a[int(i/2)]; }' > smoker-no-med.txt
echo 'Average weight for non-smoking mothers is:'
cat smoker-no-med.txt
history | cut -c 8- > data-script.bash
