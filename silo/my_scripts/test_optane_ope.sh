#!/bin/bash

source default_params.sh

rratio_list="50"
skew_list="0.9"
max_ope_list="20 80 320 1280 5120"
#max_ope_list="5120"
#max_ope_list="20 320"
nth_list="1"

source run.sh silo_original
source run.sh silo_dax
#source run_coro.sh corobase_dax
source run_coro.sh ptx_dax

ncoro_list="8"
#source run_coro.sh ptx

