#!/bin/bash

source default_params.sh

rratio_list="100 50"
nth_list="1 2 4 8 12 16 20"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh ptx

build_path="../build3"
source run_coro.sh ptx_dax

