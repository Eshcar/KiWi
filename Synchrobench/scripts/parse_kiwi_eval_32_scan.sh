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
iterations="15"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

declare -a sepThreadMode

distribution="uniform"

output=${dir}/output_kiwi_eval_so
out=${output}/data/kiwi_eval_so.csv

rm ${out}

printHeader ${out}

benchs="kiwi.KiWiMap trees.lockfree.update.LockFreeKSTRQ trees.lockfree.LockFreeJavaSkipList"
thread="16 24 32"
writes=("0")
snapshots=("100")
chunks="2500"
rebProb="15"
rebRatio="1.6"
mergeRatio="0.7"
ranges="1024"
sepThreadMode=("false")
fillType="random"

printData


