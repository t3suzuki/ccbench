#!/bin/bash

source default_params.sh

skew_list="0 0.2 0.5 0.7 0.8 0.9 0.95 0.99"

source run.sh ss2pl_original
source run.sh ss2pl_dax

source run_coro.sh ptx_dax

