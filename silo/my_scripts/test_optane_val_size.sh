#!/bin/bash

val_size_list="4 8 16 32 64 128 256 512"


for val_size in $val_size_list
do
    source default_params.sh
    ncoro_list="16"
    build_path="../build$val_size"
    source run.sh silo_original
    source run.sh silo_dax
    source run_coro.sh ptx_dax
    ncoro_list="8"
    source run_coro.sh ptx
done

