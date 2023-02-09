#!/bin/bash


val_size_list="16 64 256 1024"


for val_size in $val_size_list
do
    source default_params.sh
    build_path=$build_path$val_size
    echo $build_path
    rm -rf $build_path
    mkdir $build_path
    cd $build_path; cmake .. -DCMAKE_BUILD_TYPE=Release -DVAL_SIZE=$val_size; make -j 8; cd -
done
