#!/bin/bash

#bash ./check_dax_fs.sh

cd ../build

## Disable Nap
rm CMakeCache.txt
cmake -DENABLE_NAP_FLAG=OFF -DRANGE_BENCH_FLAG=ON .. && make -j4

threads=(2 6 12 18 24 30 36 42 48 54 60 63)

exe=./fastfair_nap

echo "start raw_index scan"
file_name=Fig9_scan_Raw
echo "" > $file_name
for t in ${threads[@]}
do  
    sleep 5
    echo "running... #thread=${t}"
    ./$exe /mnt/pmem0/ycsb ../dataset/load ../dataset/load-read95-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done


## Enable Nap
rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=ON -DRANGE_BENCH_FLAG=ON .. && make -j4

echo "start nap_index scan"
file_name=Fig9_scan_Nap
echo "" > $file_name
for t in ${threads[@]}
do
    echo "running... #thread=${t}"
    ./$exe /mnt/pmem0/ycsb ../dataset/load ../dataset/load-read95-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done