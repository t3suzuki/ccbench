#!/bin/sh

nth_list="1 8"
ntrial=3
trus_list="1 4 16 64"
ncoro_list="1 4 16 64 256"
opts="-perc_payment 0"

for nth in $nth_list
do
    for itrial in `seq $ntrial`
    do
	echo "mode=original nth=$nth itrial=$itrial"
	taskset -c 0-9 ./silo_original -thread_num $nth -extime 10 $opts
    done
done

for trus in $trus_list
do
    for nth in $nth_list
    do
	for ncoro in $ncoro_list
	do
	    for itrial in `seq $ntrial`
	    do
		echo "mode=corobase+new_pref trus=$trus nth=$nth ncoro=$ncoro itrial=$itrial"
		taskset -c 0-9 ./corobase_${trus}us_${ncoro}coro -thread_num $nth -extime 10 $opts
	    done
	done
    done
done


