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

output=${dir}/output_st_ordered_eval
resetDir ${output}

benchs="trees.lockbased.LockBasedSnapTree"
thread="32"
writes=("100")
snapshots=("0")
chunks="2500"
rebProb="15"
rebRatio="1.6"
ranges="0"
mergeRatio="0.7"
sepThreadMode=("false")
fillType="ordered"

runBench

