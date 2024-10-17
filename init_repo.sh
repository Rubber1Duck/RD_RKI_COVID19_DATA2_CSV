#!/bin/bash

#get arguments
SDATE=$(date -I -d "$1") || exit -1
EDATE=$(date -I -d "$2") || exit -1

STIME=$(date +%s.%N)

# print starting message
#LDATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
#echo "$LDATE : Start INITRUN with dates: $SDATE to $EDATE"

# download static 7zip
#LDATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
#echo "$LDATE : download static 7zip"
#VERSION7ZIP="2301"
#./get7Zip.sh ${VERSION7ZIP}

#CDATE="$SDATE"
#while [ "$CDATE" != "$EDATE" ]; do 
#LDATE=$(date '+%Y-%m-%dT%H:%M:%S,%3NZ')
#echo "$LDATE : running on $CDATE"
    
# Extract JSON data
#LDATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
#echo "$LDATE : extract all data"
#./extract.sh
 
# Run python  
#LDATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
#echo "$LDATE : executing python build_metanew.py $CDATE init"
python ./src/build_metanew.py $SDATE $EDATE "init"
  
# compress json files in history
#rm -f ./*.xz
#for FILE in `find dataStore/ -name "*.csv" -type f`; do
#  LDATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
#  SIZE1=$(stat -c%s $FILE)
#  printf "$LDATE : compressing $FILE ($SIZE1 bytes). "
#  ./7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "./$FILE.xz" "./$FILE"
#  SIZE2=$(stat -c%s $FILE.xz)
#  QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
#  printf "%s\n" "New Size: $SIZE2 = $QUOTE %"
#done
  
# commit changes
#git add .
#git commit -m"update on $CDATE"
#git tag -a "$CDATE" -m"update on $CDATE" 
#git push --follow-tags

# endmessage for currend date
#echo "**************************************************"
#CDATE=$(date -I -d "$CDATE + 1 day")
#done

#rm -rf ./7zzs

# print message update finished
LDATE=$(date '+%Y-%m-%dT%H:%M:%S,%3NZ')
ETIME=$(date +%s.%N)
TSEC=$(echo "$ETIME - $STIME" | bc)
TTIME=`date -d@$TSEC -u +%H:%M:%S,%3N | cut -b1-12`
echo "$LDATE : initrun finished. Last date is $EDATE Total execution time $TTIME."
