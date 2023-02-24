#!/bin/bash

source default_params.sh

tuple_num_list="12500000 25000000 50000000 100000000 200000000 400000000 800000000"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh ptx

build_path="../build3"
source run_coro.sh ptx_dax
