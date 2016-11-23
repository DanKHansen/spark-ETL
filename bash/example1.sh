#!/bin/bash
mkdir /data/zips                                               		#create a directory for staging *.zip files
mkdir /data/files                                              		#create a directory for staging *.csv files
hdfs dfs -copyToLocal $1/*.zip /data/zips              				#copy zip files from HDFS to this node
for file in /data/zips/*.zip; do
    unzip -a -j $file -d /data/files/                          		#for each *.zip file copied, unzip it to ./files
done
sed -i '1d' /data/files/*                                       	#remove the first line from each file in ./files
cat /data/files/* > /data/files/result.csv                        	#concatenate all of the files in ./files
hdfs dfs -put /data/files/result.csv /user/example/quoteTable_csv/ 	#upload the concatenated file to HDFS
rm -rf /data/zips                                              		#remove staging directory for *.zip files
rm -rf /data/files                                             		#remove staging directory for *.csv files
