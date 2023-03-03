#!/bin/bash

source default_params.sh

skew_list="0.9"
rratio_list="95 50"
nth_list="16 32 48 64"

taskset="taskset -c 0-63"
source run.sh silo_original
source run_coro.sh corobase
source run_coro.sh ptx

