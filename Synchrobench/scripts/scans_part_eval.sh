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
iterations="30"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

declare -a sepThreadMode

distribution="uniform"

output=${dir}/output_scans_eval_p2
resetDir ${output}


benchs="kiwi.KiWiMap"
thread="32"
writes=("50")
snapshots=("50")
chunks="2500"
rebProb="15"
rebRatio="1.6"
ranges="8192 32768"
mergeRatio="0.7"
sepThreadMode=("true")
fillType="random"

runBench

benchs="kiwi.KiWiMap"
thread="32"
writes=("0")
snapshots=("100")
chunks="2500"
rebProb="15"
rebRatio="1.6"
ranges="8192 32768"
mergeRatio="0.7"
sepThreadMode=("false")
fillType="random"

runBench

benchs="trees.lockfree.LockFreeJavaSkipList"
thread="32"
writes=("50")
snapshots=("50")
chunks="0"
rebProb="0"
rebRatio="0"
mergeRatio="0"
ranges="2048"
sepThreadMode=("true")
fillType="random"

runBench


benchs="trees.lockfree.LockFreeJavaSkipList"
thread="32"
writes=("0")
snapshots=("100")
chunks="0"
rebProb="0"
rebRatio="0"
mergeRatio="0"
ranges="2048"
sepThreadMode=("false")
fillType="random"

runBench
