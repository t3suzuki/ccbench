#!/bin/bash

source default_params.sh

#skew_list=""
#rratio_list="100"
ncoro_list="1 2 4 8 16 32"
#ncoro_list="256 512 1024"
#ncoro_list="2048 4096"
#ncoro_list="128"
nth_list="20"

#source run.sh silo_original
#source run.sh silo_dax
#source run_coro.sh corobase_dax
#source run_coro.sh ptx_dax
source run_coro.sh ptx

