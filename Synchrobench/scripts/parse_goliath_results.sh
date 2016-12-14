#!/bin/bash

dir=..
output=${dir}/output_goliath

thread="2 4 8 16 32 40 48 64"

l="5000"
warmup="5"
writeall="0"
iterations="3"

writes="50 90 10 100 0"
snapshots="50 10 90 0 100"
chunks="4500"
ranges="2 10 100 1000 100000"
shouldRange="false"

keysRange="1000000"
declare -a ratios
ratios=(4)

# benchmarks
benchs="kiwi.KiWiMap kiwisplit.KiWiMap trees.lockfree.LockFreeJavaSkipList trees.lockfree.update.LockFreeKSTRQ"


###############################


ds="goliath_result"

# write header
#echo ${writes}
#echo ${size}
#echo ${snapshots}

out=${output}/data/${ds}-temp.csv
rm $out

echo ${out}
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
			




for write in ${writes}; do
	for k in ${ratios[@]}; do
        #	for snap in ${snapshots}; do
			let snap=100-$write
			r=$keysRange #`echo "2*${i}" | bc`
            let i="$(($r*2/$k))"


			for it in ${chunks}; do

				for bench in ${benchs}; do                     
					for mr in ${ranges}; do
						for t in ${thread}; do
							in=${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}.log
								
							#echo "Input file: ${in}"
if [ ! -f "${in}" ] 
then
	continue
fi

#							echo "Input file: ${in}"
                            scanStdev=`grep "Scan time stdev (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
                            printf $scanStdev >> ${out}
                            printf '\t' >> ${out}

                            putStdev=`grep "Put time stdev (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
                            printf $putStdev >> ${out}
                            printf '\t' >> ${out}

                            printf $i >> ${out}
							printf '\t' >> ${out}
							
							keysPerScan=`grep "Scan keys num avg:" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
							printf $keysPerScan >> ${out}
							printf '\t' >> ${out}
							
							keyTimeAvg=`grep "Scan per key time avg (ms):" ${in} | awk '{ s += $7;  ns++} END { printf "%f", s/ns }'`
							printf $keyTimeAvg >> ${out}
							printf '\t' >> ${out}
							
							operations=`grep "Operations:" ${in} | awk '{ s += $2;  ns++} END { printf "%f", s/ns }'`
							printf $operations >> ${out}
							printf '\t' >> ${out}
							
							printf $it >> ${out}
							printf '\t' >> ${out}
							
							printf $t >> ${out}
							printf '\t' >> ${out}

							putTimeAvg=`grep "Put time avg (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
							printf ${putTimeAvg} >> ${out}
							printf '\t' >> ${out}
														
							putTimeMax=`grep "Put time max (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
							printf $putTimeMax >> ${out}
							printf '\t' >> ${out}
							
							scanTimeAvg=`grep "Scan time avg (ms):" ${in} | awk '{ s += $5; ns++ } END { printf "%f", s/ns }'`
							printf ${scanTimeAvg} >> ${out}
							printf '\t' >> ${out}
							
							scanTimeMax=`grep "Scan time max (ms):" ${in} | awk '{ s += $5;  ns++} END { printf "%f", s/ns }'`
							printf ${scanTimeMax} >> ${out}
							printf '\t' >> ${out}
							
							thavg=`grep "Throughput" ${in} | awk '{ s += $3; nb++ } END { printf "%f", s/nb }'`
							printf ${thavg} >> ${out}
							printf '\t' >> ${out}
							
							printf ${write} >> ${out}
							printf '\t' >> ${out}
							
							printf ${snap} >> ${out}
							printf '\t' >> ${out}
							
							printf ${mr} >> ${out}
							printf '\t' >> ${out}
							
							printf ${bench} >> ${out}
							printf '\t' >> ${out}
							
							printf '\n' >> ${out}
							
   						done
   					done		
				done

			done
	done
done
################################################################################################

