#!/bin/bash

build_path="../build"

#skew_list="0 0.7 0.9"
skew_list="0.9"
#rratio_list="0 25 50 75 100"
rratio_list="25"
nth_list="1 2 4 8 12 16 20"
#nth_list="12 16 20"
ntrial=1
ncoro_list="16"
tuple_num_list="100000000"
taskset="taskset -c 0-19"
extime=10
max_ope=10

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh corobase_dax
source run_coro.sh ptx_dax

