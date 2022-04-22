#!/bin/bash

umount /mnt/pmem0
umount /mnt/pmem1
umount /mnt/pmem2
umount /mnt/pmem3

ndctl create-namespace --mode=fsdax -e namespace0.0 -f

yes | mkfs.ext4 /dev/pmem0
yes | mkfs.ext4 /dev/pmem1
yes | mkfs.ext4 /dev/pmem2
yes | mkfs.ext4 /dev/pmem3

mount -o dax /dev/pmem0 /mnt/pmem0
mount -o dax /dev/pmem1 /mnt/pmem1
mount -o dax /dev/pmem2 /mnt/pmem2
mount -o dax /dev/pmem3 /mnt/pmem3