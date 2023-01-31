#!/bin/sh

#skew_list="0 0.7 0.9"
skew_list="0.9"
#rratio_list="0 25 50 75 100"
rratio_list="25"
nth_list="1"
ntrial=1
trus_list="1 2 4 8 16 32 64"
ncoro_list="1 2 4 8 16 32 64 128 256 512 1024"
tuple_num=40000000
taskset="taskset -c 0-18"


## original
for skew in $skew_list
do
    for rratio in $rratio_list
    do
	for nth in $nth_list
	do
	    for itrial in `seq $ntrial`
	    do
		echo "mode=original skew=$skew rratio=$rratio nth=$nth itrial=$itrial"
		$taskset ./silo_original -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
	    done
	done
    done
done

## DAX
for skew in $skew_list
do
    for rratio in $rratio_list
    do
	for nth in $nth_list
	do
	    for itrial in `seq $ntrial`
	    do
		echo "mode=dax skew=$skew rratio=$rratio nth=$nth itrial=$itrial"
		$taskset ./silo_dax -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
	    done
	done
    done
done

## Corobase DAX
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
		    echo "mode=corobase_dax skew=$skew rratio=$rratio nth=$nth itrial=$itrial"
		    $taskset ./corobase_${ncoro}coro_dax -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
		done
	    done
	done
    done
done

## CoDB DAX
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
		    echo "mode=pilo2_dax skew=$skew rratio=$rratio nth=$nth itrial=$itrial"
		    $taskset ./pilo2_${ncoro}coro_dax -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
		done
	    done
	done
    done
done



# ## corobase
# for trus in $trus_list
# do
#     for skew in $skew_list
#     do
# 	for rratio in $rratio_list
# 	do
# 	    for nth in $nth_list
# 	    do
# 		for ncoro in $ncoro_list
# 		do
# 		    for itrial in `seq $ntrial`
# 		    do
# 			echo "mode=corobase trus=$trus skew=$skew rratio=$rratio nth=$nth ncoro=$ncoro itrial=$itrial"
# 			$taskset ./corobase_${trus}us_${ncoro}coro -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
# 		    done
# 		done
# 	    done
# 	done
#     done
# done

# ## pilo
# for trus in $trus_list
# do
#     for skew in $skew_list
#     do
# 	for rratio in $rratio_list
# 	do
# 	    for nth in $nth_list
# 	    do
# 		for ncoro in $ncoro_list
# 		do
# 		    for itrial in `seq $ntrial`
# 		    do
# 			echo "mode=pilo2 trus=$trus skew=$skew rratio=$rratio nth=$nth ncoro=$ncoro itrial=$itrial"
# 			$taskset ./pilo2_${trus}us_${ncoro}coro -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope 10 -extime 10 -zipf_skew $skew
# 		    done
# 		done
# 	    done
# 	done
#     done
# done
