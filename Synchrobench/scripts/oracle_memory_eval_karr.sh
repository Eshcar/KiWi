#!/bin/bash
source ./functions.sh

dir=..
deuce="${dir}/lib/mydeuce.jar"
agent=${dir}/lib/deuceAgent-1.3.0.jar
bin=${dir}/bin
java=/home/dbasin/Java/jdk1.8.0_65/bin/java
javaopt=-server

CP=${dir}/lib/compositional-deucestm-0.1.jar:${dir}/lib/mydeuce.jar:${dir}/bin
MAINCLASS=contention.benchmark.Test



l="60000"
warmup="5"
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

output=${dir}/output_oracle_memory_karr
resetDir ${output}


benchs="trees.lockfree.update.LockFreeKSTRQ"
thread="32"
writes=("100")
snapshots=("0")
chunks="4096"
rebProb="0"
rebRatio="0"
ranges="0"
sepThreadMode=("false")
memSnapInterval=2000

runMemory



