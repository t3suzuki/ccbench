#!/bin/bash


val_size_list="16 64 256 1024"

for val_size in $val_size_list
do
    source default_params.sh
    build_path=$build_path$val_size
    source run.sh silo_original
    source run.sh silo_dax
    source run_coro.sh ptx_dax
done

