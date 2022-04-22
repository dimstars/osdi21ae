#!/bin/bash

umount /mnt/pmem0
umount /mnt/pmem1
umount /mnt/pmem2
umount /mnt/pmem3

ndctl create-namespace --mode=devdax -e namespace0.0 -f

cd ../build
cmake .. && make -j

threads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18)

echo "========= write local ========="
for t in ${threads[@]}
do
./aep_raw ${t} 1 0 
done

echo "========= write remote ========="
for t in ${threads[@]}
do
./aep_raw ${t} 1 1 
done

echo "========= read local ========="
for t in ${threads[@]}
do
./aep_raw ${t} 0 0 
done

echo "========= read remote ========="
for t in ${threads[@]}
do
./aep_raw ${t} 0 1 
done
