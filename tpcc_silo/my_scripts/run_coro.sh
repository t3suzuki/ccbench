for wh in $wh_list
do
    for nth in $nth_list
    do
 	for ncoro in $ncoro_list
 	do
	    for itrial in `seq $ntrial`
	    do
		exe=$build_path/$1_${ncoro}coro
		echo "mode=$exe nth=$nth wh=$wh itrial=$itrial"
		echo "$taskset $exe -thread_num $nth -num_wh $wh -extime $extime"
		$taskset $exe -thread_num $nth -num_wh $wh -extime $extime
	    done
	done
    done
done
