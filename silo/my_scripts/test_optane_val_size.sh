#!/bin/bash


#val_size_list="4 16 64"
val_size_list="8 32 128"

for val_size in $val_size_list
do
    source default_params.sh
    build_path="../build4_"$val_size
    source run.sh silo_original
    source run.sh silo_dax
    source run_coro.sh ptx
    build_path="../build3_"$val_size
    source run_coro.sh ptx_dax
done

