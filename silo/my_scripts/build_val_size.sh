#!/bin/bash


val_size_list="8 16 32 64 128"


for val_size in $val_size_list
do
    source default_params.sh
    build_path=$build_path$val_size
    echo $build_path
    rm -rf $build_path
    mkdir $build_path
    cd $build_path; cmake .. -DCMAKE_BUILD_TYPE=Release -DCORO=1 -DDAX=1 -DVAL_SIZE=$val_size; make -j 8; cd -
done
