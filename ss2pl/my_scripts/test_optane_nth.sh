#!/bin/bash

source default_params.sh

rratio_list="100 50"
nth_list="1 2 4 8 12 16 20"

source run.sh ss2pl_original
source run.sh ss2pl_dax
source run_coro.sh ptx
source run_coro.sh ptx_dax
