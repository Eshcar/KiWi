#!/bin/bash

dir=..
deuce="${dir}/lib/mydeuce.jar"
agent=${dir}/lib/deuceAgent-1.3.0.jar
bin=${dir}/bin
java=/home/dbasin/Java/jdk1.8.0_65/bin/java
javaopt=-server

CP=${dir}/lib/compositional-deucestm-0.1.jar:${dir}/lib/mydeuce.jar:${dir}/bin
MAINCLASS=contention.benchmark.Test

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

function runBench
{
	for bench in ${benchs}; do
	#for write in ${writes}; do
	for opIdx in ${!writes[@]}; do
		let write=${writes[$opIdx]}
		let snap=${snapshots[$opIdx]}
		
		for t in ${thread}; do
			for it in ${chunks}; do
				for rIdx in ${!keysRange[@]}; do
					for distr in ${distribution}; do
						
					let r=${keysRange[$rIdx]} #`echo "2*${i}" | bc`
					let i=${initSize[$rIdx]}	

					for mr in ${ranges}; do
						#for snap in ${snapshots}; do
						
							out=${output}/log/${bench}-r${mr}_c${it}-i${i}-u${write}-s${snap}-t${t}-d${distr}-sepThreads_${shouldRange}.log
							for (( j=1; j<=${iterations}; j++ )); do
								cmd="${java} ${javaopt} -cp ${CP} ${MAINCLASS} -W ${warmup} -u ${write} -s ${snap} -it ${it} -MR ${mr} -mR ${mr} -d ${l} -t ${t} -i ${i} -r ${r} -R ${shouldRange} -b ${bench}"
								
								echo ${cmd} 
								echo ${out}
								${cmd} 2>&1 >> ${out}
							done
						done
					done
				done
				done
			done
		done
	done

}

# main params


l="5000"
warmup="5"
writeall="0"
iterations="3"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

distribution="uniform"
benchs="kiwi.KiWiMap trees.lockfree.LockFreeKSTRQ kiwisplit.KiWiMap trees.lockfree.LockFreeJavaSkipList"

output=${dir}/output_oracle_symmetric
resetDir ${output}


shouldRange="false"
thread="8 16 32"
writes=("100" "50" "0")
snapshots=("0" "0" "0")
chunks="4500"
ranges="0"

runBench
distribution="uniform Churn10Perc"
shouldRange="true"
writes=("50" "75" "0")
snapshots=("50" "25" "100")
ranges="10 1000 10000 25000 100000"

runBench

output=${dir}/output_oracle_non_symmetric
resetDir ${output}

distribution="uniform"
thread="8 16 32"
shouldRange="false"
writes="100 50"
snapshots="0 0"
ranges="0"
benchs="kiwi.KiWiMap kiwisplit.KiWiMap"
chunks="2000 3000 4000 5000 6000 7000 8000 9000 1000"

runBench

output=${dir}/output_oracle_memory
resetDir ${output}

thread="32"
shouldRange="false"
writes="100"
snapshots="0"
ranges="0"
chunks="4500"
javaopt="-server -verbose:gc"
benchs="kiwi.KiWiMap trees.lockfree.LockFreeKSTRQ kiwisplit.KiWiMap trees.lockfree.LockFreeJavaSkipList"
l="120000"
warmup="0"

runBench


