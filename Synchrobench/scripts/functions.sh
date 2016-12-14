#!/bin/bash

function resetDir
{
	local dir=$1
	if [ ! -d "${dir}" ]; then
		mkdir $dir
	else
		rm -rf ${dir}/*
	fi

	mkdir ${dir}/log ${dir}/data ${dir}/plot ${dir}/ps
}

function iterateAndRun
{
	local func=$1

echo "Debug: iterateAndrun func: ${func}"
	
	for bench in ${benchs}; do
echo "Debug: bench : ${bench}"
	#for write in ${writes}; do
	for opIdx in ${!writes[@]}; do
echo "Debug: opIdx: ${opIdx}"
		let write=${writes[$opIdx]}
		let snap=${snapshots[$opIdx]}
		for shouldRange in ${sepThreadMode[@]}; do
		for t in ${thread}; do
echo "Debug: thread: ${t}"
			for prob in ${rebProb}; do
echo "Debug: prob ${prob}"
				for ratio in ${rebRatio}; do
echo "Ratio: ${ratio}"
			for it in ${chunks}; do
echo "Debug: chunks: ${chunks}"
				for rIdx in ${!keysRange[@]}; do
echo "Debug: rIcx: ${rIdx}"
					for distr in ${distribution}; do
echo "Debug: distr: ${distr}"
						for ft in ${fillType}; do
echo "Debug: ft:  ${ft}"
							let r=${keysRange[$rIdx]} #`echo "2*${i}" | bc`
							let i=${initSize[$rIdx]}	

					for mr in ${ranges}; do
echo "Debug: mr: ${mr}"
						for pMerge in ${mergeRatio}; do
echo "Debug: pMerge: ${pMerge}"
							$func
						done
						done
							done
						done
					done
				done
					done
				done
					done
				done
			done
		done

}

function benchCommand
{
	local out=`buildLogName` #${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
	cmd="${java} ${javaopt} -cp ${CP} ${MAINCLASS} -W ${warmup} -fillType ${ft} -afterMergeRatio ${pMerge} -u ${write} -s ${snap} -it ${it} -MR ${mr} -mR ${mr} -d ${l} -t ${t} -i ${i} -r ${r} -R ${shouldRange} -rebProb ${prob} -rebRatio ${ratio} -b ${bench} -n ${iterations}"
								
	echo ${cmd} 
	echo ${out}
	${cmd} 2>&1 >> ${out}
}

function printDataCommand
{
	local in=`buildLogName` #${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
	parseFile $in
}

function buildLogName
{
	echo ${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
}

function runJMapMemoryCommand
{
	local out=`buildLogName` #${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
	out=${out}.mem
	
	cmd="${java} ${javaopt} -cp ${CP} ${MAINCLASS} -W ${warmup} -fillType ${ft} -afterMergeRatio ${pMerge} -u ${write} -s ${snap} -it ${it} -MR ${mr} -mR ${mr} -d ${l} -t ${t} -i ${i} -r ${r} -R ${shouldRange} -rebProb ${prob} -rebRatio ${ratio} -b ${bench} -n ${iterations}"
								
	echo ${cmd} 
	echo ${out}
	${cmd} 2>&1 >> ${out}&
	
	local pid=$!
	let timeToSleep=${warmup}
	let intervalSleep=${l}/10000
	
	
	sleep $timeToSleep
	
	
	for i in `seq 1 5`; do 
		sleep ${intervalSleep}
		
		echo "Making snapshot"
		
		${javaBin}/jmap -heap $pid >> ${out}.jmap.heap

		echo "" >> ${out}.jmap.heap
		echo "------------------------------------------------" >> ${out}.jmap.heap
		echo "" >> ${out}.jmap.heap
		
		${javaBin}/jmap -histo $pid | head -50 >> ${out}.jmap.histo.all
		
		echo "" >> ${out}.jmap.histo.all
		echo "------------------------------------------------" >> ${out}.jmap.histo.all
		echo "" >> ${out}.jmap.histo.all
		
		${javaBin}/jmap -histo:live $pid | head -50 >> ${out}.jmap.histo.live
		
		echo "" >> ${out}.jmap.histo.live
		echo "------------------------------------------------" >> ${out}.jmap.histo.live
		echo "" >> ${out}.jmap.histo.live
#		echo "Going to sleep for ${intervalSleep}"
	done

	wait $pid
}

function runGCMemoryCommand
{
        local out=`buildLogName` #${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
        out=${out}.mem

        cmd="${java} -verbose:gc ${javaopt} -cp ${CP} ${MAINCLASS} -W ${warmup} -fillType ${ft} -kDistr ${distr} -afterMergeRatio ${pMerge} -u ${write} -s ${snap} -it ${it} -MR ${mr} -mR ${mr} -d ${l} -t ${t} -i ${i} -r ${r} -R ${shouldRange} -rebProb ${prob} -rebRatio ${ratio} -b ${bench} -n ${iterations}"

        echo ${cmd} 
        echo ${out}
        ${cmd} 2>&1 >> ${out}
	
}

function runJStatMemoryCommand
{
        local out=`buildLogName` #${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sep${shouldRange}-prob${prob}-ratio${ratio}-ft${ft}-mRatio${pMerge}.log
        out=${out}.mem

        cmd="${java} ${javaopt} -cp ${CP} ${MAINCLASS} -W ${warmup} -fillType ${ft} -afterMergeRatio ${pMerge} -u ${write} -s ${snap} -it ${it} -MR ${mr} -mR ${mr} -d ${l} -t ${t} -i ${i} -r ${r} -R ${shouldRange} -rebProb ${prob} -rebRatio ${ratio} -b ${bench} -n ${iterations}"

        echo ${cmd} 
        echo ${out}
        ${cmd} 2>&1 >> ${out}&

        local pid=$!
        let timeToSleep=${warmup}
        let intervalSleep=${l}/10000


        sleep $timeToSleep


	jstat -gc -t $pid $memSnapInterval 50 >> ${out}.jstat

        wait $pid
}

	
function printHeader {
	local out=$1

	printf "MergeRatio" >> ${out}
	printf '\t' >> ${out}

	printf "FillType" >> ${out}
	printf '\t' >> ${out}
	
	printf "RebProb" >> ${out}
	printf '\t' >> ${out}
	
	printf "RebRatio" >> ${out}
	printf '\t' >> ${out}
	
	printf "Distribution" >> ${out}
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
	
	#echo "Looking for the file: ${in}" 
	#echo "Input file: ${in}"
	if [ ! -f "${in}" ] 
	then
		echo "The file ${in} does not exists."
		return
	fi
	
	
	#echo "The file ${in} exists"
	printf ${pMerge} >> ${out}
	printf '\t' >> ${out}

	printf ${ft} >> ${out}
	printf '\t' >> ${out}

	printf ${prob} >> ${out}
	printf '\t' >> ${out}
	
	printf ${ratio} >> ${out}
	printf '\t' >> ${out}
	
	printf ${distr} >> ${out}
	printf '\t' >> ${out}
	
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

function printMemoryHeader
{
	printf "DataStructure" >> ${out}
    printf '\t' >> ${out}
	
	printf "ProfType" >> ${out}
	printf '\t' >> ${out}
	
	printf "Num" >> ${out}
	printf '\t' >> ${out}
	
	printf "Instances" >> ${out}
	printf '\t' >> ${out}
	
	printf "Size(Bytes)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Class" >> ${out}
	printf '\t' >> ${out}
	
	printf "Fill_Type" >> ${out}
	printf '\t' >> ${out}
	
	printf "Threads" >> ${out}
	printf '\t' >> ${out}
	
	printf "Writes(%%)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Scans(%%)" >> ${out}
	printf '\t' >> ${out}
	
	printf "Scan_Range" >> ${out}
	printf '\t' >> ${out}
	
	printf "RebProb" >> ${out}
	printf '\t' >> ${out}
	
	printf "RebRatio" >> ${out}
	printf '\t' >> ${out}
	
	printf "KeyDistr" >> ${out}
	printf '\t' >> ${out}
	
	printf "MergeRatio" >> ${out}
	printf '\t' >> ${out}
	
	printf '\n' >> ${out}
}

function parseMemoryFile
{
	local logPrefix=`buildLogName`
	logPrefix=${logPrefix}.mem.jmap.histo
	
	local mapType="live all"

	local in=${logPrefix}
	
	#echo "Input file: ${in}"
	for suffix in ${mapType}; do
		in=$logPrefix.${suffix}

		for i in `seq 1 10`; do
			local iType=`grep "${i}:" ${in} | head -n 1 | awk '{ printf "%s" , $4 }'`
			
			
			local instances=`grep -F "${iType}" ${in} | awk '{ s+= $2; ns++} END { printf "%f", s/ns}'`
			local bytes=`grep -F "${iType}" ${in} | awk '{ s+= $3; ns++} END { printf "%f", s/ns}'`
		
			printf ${bench} >> ${out}
			printf '\t' >> ${out}
		
			printf ${suffix} >> ${out}
			printf '\t' >> ${out}
		
			printf ${i} >> ${out}
			printf '\t' >> ${out}

			printf ${instances} >> ${out}
			printf '\t' >> ${out}
		 		
			printf ${bytes} >> ${out}
			printf '\t' >> ${out}
			
			printf ${iType} >> ${out}
			printf '\t' >> ${out}
		
			printf ${t} >> ${out}
			printf '\t' >> ${out}
			
			printf ${ft} >> ${out}
			printf '\t' >> ${out}

			printf "%d" ${write} >> ${out}
			printf '\t' >> ${out}
	
			printf "%d" ${snap} >> ${out}
			printf '\t' >> ${out}
	
			printf "%d" ${mr} >> ${out}
			printf '\t' >> ${out}

			printf ${prob} >> ${out}
			printf '\t' >> ${out}
	
			printf ${ratio} >> ${out}
			printf '\t' >> ${out}
	
			printf ${distr} >> ${out}
			printf '\t' >> ${out}
		
			printf ${pMerge} >> ${out}
			printf '\t' >> ${out}
			
			printf '\n' >> ${out}
		done
		

	done
}

function parseGCHeader
{
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

function parseGCFile {

	in=`buildLogName`
	in="${in}.mem"

        echo "Input file: ${in}"
        
	if [ ! -f "${in}" ]
        then
                return
        fi

        echo "Input file exists"

        #                                                       echo "Input file: ${in}"
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


function printMemoryData
{
	iterateAndRun parseMemoryFile
}

function runBench
{
	iterateAndRun benchCommand
}

function printData
{
	iterateAndRun printDataCommand
}

function printGCData
{
	out="${output}/gc.csv"
	parseGCHeader "${out}"
	
	iterateAndRun parseGCFile
}

function runMemory
{
#	iterateAndRun runJMapMemoryCommand
#	iterateAndRun runJStatMemoryCommand
	iterateAndRun runGCMemoryCommand
}
