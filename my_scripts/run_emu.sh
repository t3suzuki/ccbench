for max_ope in $max_ope_list
do
    for tuple_num in $tuple_num_list
    do
	for skew in $skew_list
	do
	    for rratio in $rratio_list
	    do
		for nth in $nth_list
		do
		    for us in $us_list
		    do
 			for ncoro in $ncoro_list
 			do
			    for itrial in `seq $ntrial`
			    do
				exe=$build_path/$1_${us}us_${ncoro}coro
				echo "mode=$exe tuple_num=$tuple_num nth=$nth rratio=$rratio skew=$skew itrial=$itrial $rmw"
				echo "$taskset $exe -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope $max_ope -extime $extime -zipf_skew $skew $rmw"
				$taskset $exe -tuple_num $tuple_num -thread_num $nth -rratio $rratio -max_ope $max_ope -extime $extime -zipf_skew $skew $rmw
			    done
			done
		    done
		done
	    done
	done
    done
done
