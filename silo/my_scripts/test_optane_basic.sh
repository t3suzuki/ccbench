#!/bin/bash

source default_params.sh

skew_list="0 0.9"
rratio_list="100 50"
ncoro_list="1 2 4 8 16 32 64 128"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh corobase_dax
source run_coro.sh ptx_dax

