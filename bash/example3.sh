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

impala-shell -q "
  DROP TABLE IF EXISTS quoteTable_csv;
  CREATE EXTERNAL TABLE quoteTable_csv (
  name STRING,
  score INT,
  confidence DOUBLE,
  comm STRING
  )
  row format delimited fields terminated by ','
  LOCATION '/user/example/quoteTable_csv'; 
  INVALIDATE METADATA quoteTable_csv; 

  DROP TABLE IF EXISTS quoteTable_temp;
  CREATE TABLE quoteTable_temp 
  STORED AS PARQUET
  AS
  SELECT 
  substr(name, 2, char_length(name) - 2) as name,
  COALESCE(
  CASE WHEN cast(score AS INT) > 100  THEN  100 ELSE NULL END,
  CASE WHEN cast(score AS INT) < -100 THEN -100 ELSE NULL END,
  cast(score AS INT)
  ) AS score,
  cast(confidence AS DOUBLE) * cast(confidence AS DOUBLE) AS confidence,
  substr(comm, 2, char_length(comm) - 2) as comm
  FROM quoteTable_csv;
  
  INSERT INTO quoteTable 
  SELECT * FROM quoteTable_temp
  WHERE quoteTable_temp.name != 'EMPTY' 
  AND (quoteTable_temp.score > 50 OR quoteTable_temp.score < -50)
  AND confidence > 3.0;
  
  DROP TABLE quoteTable_csv;
  DROP TABLE quoteTable_temp;
"
