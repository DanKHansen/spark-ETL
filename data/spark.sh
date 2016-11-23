export HADOOP_USER_NAME=hdfs
for mode in 1 2 3
do
for splits in 10 50
do
for size in 1000000 10000000 100000000
do
time spark-submit --master yarn  --deploy-mode client --num-executors 10  --class blog  scalaETL-1.0.jar ${mode} /user/example/zips/data_${splits}_${size} /user/example/quoteTable ${splits}
echo "real spark time for ${mode} ${splits} ${size}"
hdfs dfs -rm -r -skipTrash /user/example/quoteTable_csv/*
hdfs dfs -rm -r -skipTrash /user/example/quoteTable/*
done
done
done
