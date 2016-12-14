#!/bin/bash


dir=..
output=${dir}/output_goliath/memory/


benchs="kiwi.KiWiMap trees.lockfree.LockFreeKSTRQ kiwisplit.KiWiMap kiwilocal.KiWiMap trees.lockfree.LockFreeJavaSkipList"
thread="32"

l="120000"
warmup="1"
writeall="0"
iterations="1"

declare -a writes
writes=("100")

declare -a snapshots
snapshots=("0")

chunks="4500"
ranges="10"

distr="uniform"

declare -a keysRange
keysRange=("1000000")
declare -a initSize
initSize=("500000")

shouldRange="false"


ds="memory_result"


out=${output}/data/${ds}.csv
rm $out
echo ${out}

function printHeader {
	local out=$1
	
	printf "mem_after_GC_full(K)" >> ${out}
	printf '\t' >> ${out}
	
	printf "mem_after_GC(K)" >> ${out}
	printf '\t' >> ${out}
	
	printf "put_threads" >> ${out}
	printf '\t' >> ${out}
	
	printf "scan_threads" >> ${out}
	printf '\t' >> ${out}
	
	printf "Get_thpt(ops/s)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Get_count(ops)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Get_time_avg(ms)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Put_thpt(ops/sec)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Put_count(ops)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Scan_thpt(ops/sec)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Scan_count(ops)" >> ${out}
	printf '\t' >> ${out}
	
		    printf "Put_stdev(ms)" >> ${out}
            printf '\t' >> ${out}
           
            printf "Scan_stdev(ms)" >> ${out}
            printf '\t' >> ${out}

			printf "Range_size" >> ${out}
			printf '\t' >> ${out}
			
			printf "Keys_per_scan_avg" >> ${out}
			printf '\t' >> ${out}
			
			printf "Key_scan_avg(ms)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Operations" >> ${out}
			printf '\t' >> ${out}
			
			printf "Chunk_size" >> ${out}
			printf '\t' >> ${out}
			
			printf "Threads" >> ${out}
			printf '\t' >> ${out}
			
			printf "Put_time_avg(ms)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Put_time_max(ms)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Scan_time_avg(ms)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Scan_time_max(ms)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Thpt_(total_ops/total_time)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Writes(%%)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Scans(%%)" >> ${out}
			printf '\t' >> ${out}
			
			printf "Scan_range" >> ${out}
			printf '\t' >> ${out}

			printf "DataStructure" >> ${out}
   		    printf '\n' >> ${out}
}			


function parseFile {
	local in=$1
	#echo "Input file: ${in}"
	if [ ! -f "${in}" ] 
	then
		return
	fi
	
	
	#							echo "Input file: ${in}"
	echo "Before memGC: file: ${in}"
	local mem_GC_full=`grep "Full GC" ${in} | awk -F'->' '{print $2}' | awk -F'(' '{print $1}' | awk -F'K' '{s+=$1; ns++} END {printf "%f", s/ns}'`
	printf "%f" ${mem_GC_full} >> ${out}
	printf '\t' >> ${out}
	
	local memGC=`grep "GC" ${in} | awk -F'->' '{print $2}' | awk -F'(' '{print $1}' | awk -F'K' '{s+=$1; ns++} END {printf "%f", s/ns}'`
	printf "%f" ${memGC} >> ${out}
	printf '\t' >> ${out}
	
	echo "MemGC: ${memGC}"
	
	local putThreads=`grep "Num of put threads:" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
	printf "%.0f" ${putThreads} >> ${out}
	printf '\t' >> ${out}
	
	local scanThreads=`grep "Num of scan threads:" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
	printf "%.0f" $scanThreads >> ${out}
	printf '\t' >> ${out}
	
	local getThpt=`grep "Get throughput (ops/s):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" ${getThpt} >> ${out}
	printf '\t' >> ${out}
	
	local getCount=`grep "Get count (ops):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" ${getCount} >> ${out}
	printf '\t' >> ${out}
	
	local getTimeAvg=`grep "Get time avg (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
	printf "%f" ${getTimeAvg} >> ${out}
	printf '\t' >> ${out}
	
	local putThpt=`grep "Put throughput (ops/s):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" ${putThpt} >> ${out}
	printf '\t' >> ${out}
	
	local putCount=`grep "Put count (ops):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" ${putCount} >> ${out}
	printf '\t' >> ${out}
	
	local scanThpt=`grep "Scan throughput (scans/s):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" $scanThpt >> ${out}
	printf '\t' >> ${out}
	
	local scanCount=`grep "Scan count (ops):" ${in} | awk '{ s += $4;  ns++} END { printf "%f", s/ns }'`
	printf "%f" $scanCount >> ${out}
	printf '\t' >> ${out}
	
	
	
	
	                            scanStdev=`grep "Scan time stdev (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
	                            printf "%f" ${scanStdev} >> ${out}
	                            printf '\t' >> ${out}

	                            putStdev=`grep "Put time stdev (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
	                            printf "%f" ${putStdev} >> ${out}
	                            printf '\t' >> ${out}

	                            printf "%f" ${i} >> ${out}
								printf '\t' >> ${out}
							
								keysPerScan=`grep "Scan keys num avg:" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${keysPerScan} >> ${out}
								printf '\t' >> ${out}
							
								keyTimeAvg=`grep "Scan per key time avg (ms):" ${in} | awk '{ s += $7;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${keyTimeAvg} >> ${out}
								printf '\t' >> ${out}
							
								operations=`grep "Operations:" ${in} | awk '{ s += $2;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${operations} >> ${out}
								printf '\t' >> ${out}
							
								printf "%f" ${it} >> ${out}
								printf '\t' >> ${out}
							
								printf "%d" ${t} >> ${out}
								printf '\t' >> ${out}

								putTimeAvg=`grep "Put time avg (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${putTimeAvg} >> ${out}
								printf '\t' >> ${out}
														
								putTimeMax=`grep "Put time max (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${putTimeMax} >> ${out}
								printf '\t' >> ${out}
							
								scanTimeAvg=`grep "Scan time avg (ms):" ${in} | awk '{ s += $5; ns++ } END { printf "%f", s/ns }'`
								printf "%f" ${scanTimeAvg} >> ${out}
								printf '\t' >> ${out}
							
								scanTimeMax=`grep "Scan time max (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
								printf "%f" ${scanTimeMax} >> ${out}
								printf '\t' >> ${out}
							
								thavg=`grep "Throughput" ${in} | awk '{ s += $3; nb++ } END { printf "%f", s/nb }'`
								printf "%f" ${thavg} >> ${out}
								printf '\t' >> ${out}
							
								printf "%d" ${write} >> ${out}
								printf '\t' >> ${out}
							
								printf "%d" ${snap} >> ${out}
								printf '\t' >> ${out}
							
								printf "%d" ${mr} >> ${out}
								printf '\t' >> ${out}
							
								printf ${bench} >> ${out}
								printf '\t' >> ${out}
							
								printf '\n' >> ${out}
	
	
} 

printHeader ${out}

	for bench in ${benchs}; do

		for opIdx in ${!writes[@]}; do
					let write=${writes[$opIdx]}
					let snap=${snapshots[$opIdx]}
		
					for t in ${thread}; do
						for it in ${chunks}; do
							for rIdx in ${!keysRange[@]}; do
							
									let r=${keysRange[$rIdx]} #`echo "2*${i}" | bc`
									let i=${initSize[$rIdx]}	

										
									for mr in ${ranges}; do
										in=${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sepThreads_${shouldRange}.log
										parseFile $in
									done
   					done		
				done
			done
			done
	done
################################################################################################


