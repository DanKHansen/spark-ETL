#!/bin/bash
export HADOOP_USER_NAME=hdfs
for script in example1.sh example2.sh example3.sh
do
for splits in 10 50
do
for size in 1000000 10000000 100000000
do
time bash/$script /user/example/zips/data_${splits}_${size}
echo "real bash time for ${script} ${splits} ${size}"
hdfs dfs -rm -skipTrash /user/example/quoteTable_csv/*
hdfs dfs -rm -skipTrash /user/example/quoteTable/*
hdfs dfs -rm -skipTrash /user/example/quoteTable_one/*
done
done
done
