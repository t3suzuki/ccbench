#!/bin/bash

source default_params.sh


max_ope_list="9999"
ncoro_list="1 2 4 8 16 32"
skew_list="0.7 0.8 0.9 0.95 0.99"

source run_coro.sh corobase_dax

