#!/bin/bash

source default_params.sh

tuple_num_list="50000000 100000000 200000000 400000000 800000000"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh ptx
source run_coro.sh ptx_dax

