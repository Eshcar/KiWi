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



l="5000"
warmup="20"
writeall="0"
iterations="3"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

declare -a sepThreadMode

distribution="uniform"

output=${dir}/output_oracle_eval_karr
resetDir ${output}


benchs="trees.lockfree.update.LockFreeKSTRQ"
thread="2 4 8 16 24 32"
writes=("50")
snapshots=("50")
chunks="0"
rebProb="0"
rebRatio="0"
ranges="10 100 1000 10000 25000"
sepThreadMode=("true")

runBench


benchs="trees.lockfree.update.LockFreeKSTRQ"
thread="1 2 4 8 16 24 32"
writes=("100" "0" "50")
snapshots=("0" "0" "0")
chunks="0"
rebProb="0"
rebRatio="0"
ranges="0"
sepThreadMode=("false")

runBench

benchs="trees.lockfree.update.LockFreeKSTRQ"
thread="1 2 4 8 16 24 32"
writes=("0")
snapshots=("100")
chunks="0"
rebProb="0"
rebRatio="0"
ranges="10 100 1000 10000 25000"
sepThreadMode=("false")

runBench
