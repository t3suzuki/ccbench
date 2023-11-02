#!/bin/bash

source default_params.sh

skew_list="0 0.2 0.5 0.7 0.8 0.9 0.95 0.99"
max_ope_list="9999"

source run.sh silo_original
source run.sh silo_dax
source run_coro.sh corobase_dax
source run_coro.sh ptx_dax

