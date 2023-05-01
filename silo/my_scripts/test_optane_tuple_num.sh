#!/bin/bash

source default_params.sh

#tuple_num_list="1562500 3125000 6250000 12500000 25000000 50000000 100000000 200000000 400000000 800000000"
tuple_num_list="250000 500000 1000000 2000000 4000000 8000000 16000000 32000000 64000000 128000000 256000000 512000000"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh ptx_dax

ncoro_list="8"
source run_coro.sh ptx
