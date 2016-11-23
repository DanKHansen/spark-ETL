for run in 1 2 3 4 5
do
echo "really starting ${run}"
bash spark.sh 
echo "really done with spark for ${run}"
bash allbash.sh
echo "really finished with run ${run}"
done
