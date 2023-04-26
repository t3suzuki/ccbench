#!/bin/bash

source default_params.sh

rratio_list="100 50"
nth_list="1 2 4 8 12 16 20"

source run.sh cicada_original
source run.sh cicada_dax
source run_coro.sh ptx_dax

ncoro_list="8"
source run_coro.sh ptx
