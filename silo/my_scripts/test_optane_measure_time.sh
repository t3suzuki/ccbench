#!/bin/bash

source default_params.sh

nth_list="1"
ncoro_list="1 2 4 8 16 32 64 128"

source run.sh silo_original
source run_coro.sh ptx_dax

