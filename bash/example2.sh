#!/bin/bash
mkdir /data/zips                                               		#create a directory for staging *.zip files
mkdir /data/files                                              		#create a directory for staging *.csv files
hdfs dfs -copyToLocal $1/*.zip /data/zips              			#copy zip files from HDFS to this node
for file in /data/zips/*.zip; do
unzip -a -j $file -d /data/files/                          		#for each *.zip file copied, unzip it to ./files
done
sed -i '1d' /data/files/*                                       	#remove the first line from each file in ./files
cat /data/files/* > /data/files/result.csv                        	#concatenate all of the files in ./files
hdfs dfs -rm -skipTrash /user/example/quoteTable_csv/*                  #remove any old files in hdfs #for benchmark only
hdfs dfs -put /data/files/result.csv /user/example/quoteTable_csv/ 	#upload the concatenated file to HDFS
rm -rf /data/zips                                              		#remove staging directory for *.zip files
rm -rf /data/files                                             		#remove staging directory for *.csv files

#For convenience, we perform the second step in the same script:
impala-shell -q "
  DROP TABLE IF EXISTS quoteTable_csv;
  DROP TABLE IF EXISTS quoteTable_one;
  CREATE EXTERNAL TABLE quoteTable_csv 
  (quote STRING)
  row format delimited fields terminated by ','
  LOCATION '/user/example/quoteTable_csv';
  CREATE EXTERNAL TABLE quoteTable_one 
  (quote STRING)
  STORED AS PARQUET
  LOCATION '/user/example/quoteTable';
  INSERT INTO quoteTable_one SELECT * FROM quoteTable_csv WHERE quote != 'EMPTY';
  DROP TABLE quoteTable_csv;
  DROP TABLE quoteTable_one;
"
