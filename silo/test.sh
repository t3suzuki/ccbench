#!/bin/sh

skew_list="0 0.7 0.9"
#skew_list="0.9"
rratio_list="0 25 50 75 100"
nth_list="1 2 4 8"
ntrial=3
trus_list="1 4 16 64"
ncoro_list="1 2 4 8 16 32 64 128 256 512"


for skew in $skew_list
do
    for rratio in $rratio_list
    do
	for nth in $nth_list
	do
	    for itrial in `seq $ntrial`
	    do
		echo "skew=$skew rratio=$rratio nth=$nth itrial=$itrial"
		taskset -c 0-9 ./silo_original -tuple_num 10000000 -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
	    done
	done
    done
done

for trus in $trus_list
do
    for skew in $skew_list
    do
	for rratio in $rratio_list
	do
	    for nth in $nth_list
	    do
		for ncoro in $ncoro_list
		do
		    for itrial in `seq $ntrial`
		    do
			echo "trus=$trus skew=$skew rratio=$rratio nth=$nth ncoro=$ncoro itrial=$itrial"
			taskset -c 0-9 ./silo_${trus}us_${ncoro}coro -tuple_num 10000000 -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
		    done
		done
	    done
	done
    done
done


for trus in $trus_list
do
    for skew in $skew_list
    do
	for rratio in $rratio_list
	do
	    for nth in $nth_list
	    do
		for ncoro in $ncoro_list
		do
		    for itrial in `seq $ntrial`
		    do
			echo "trus=$trus skew=$skew rratio=$rratio nth=$nth ncoro=$ncoro itrial=$itrial"
			taskset -c 0-9 ./pilo_${trus}us_${ncoro}coro -tuple_num 10000000 -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
		    done
		done
	    done
	done
    done
done
