#!/bin/bash

source default_params.sh

ncoro_list="1 2 4 8 16 32 64 128"

source run.sh silo_original
source run.sh silo_dax

build_path="../build3"
source run_coro.sh corobase_dax
source run_coro.sh ptx_dax

