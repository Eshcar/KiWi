#!/bin/bash
source ./functions.sh

dir=..
deuce="${dir}/lib/mydeuce.jar"
agent=${dir}/lib/deuceAgent-1.3.0.jar
bin=${dir}/bin
java=/usr/bin/java
#java=/home/dbasin/Java/jdk1.8.0_65/bin/java
javaBin=/usr/bin/
#javaBin=/home/dbasin/Java/jdk1.8.0_65/bin/
javaopt=-server

CP=${dir}/lib/compositional-deucestm-0.1.jar:${dir}/lib/mydeuce.jar:${dir}/bin
MAINCLASS=contention.benchmark.Test



l="120000"
warmup="0"
writeall="0"
iterations="1"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

declare -a sepThreadMode

distribution="uniform"

output=${dir}/ocl_mem_rand
resetDir ${output}

#benchs="kiwi.KiWiMap trees.lockfree.update.LockFreeKSTRQ"
benchs="trees.lockfree.update.LockFreeKSTRQ"
thread="16"
writes=("50")
snapshots=("50")
chunks="1024"
rebProb="15"
rebRatio="1.6"
ranges="2 2000 200000 2000000"
sepThreadMode=("true")
memSnapInterval=2000
#fillType="drop90"
fillType="random"
mergeRatio="0.7"

echo "Running memory eval"

runMemory
printGCData


