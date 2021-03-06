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
warmup="5"
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

output=${dir}/output_puts_optimization
resetDir ${output}

benchs="kiwi.KiWiMap"
thread="32"
writes=("100" "0" "50")
snapshots=("0" "0" "0")
chunks="2048 1024"
rebProb="2 5 10"
rebRatio="1.5 1.8"
ranges="0"
sepThreadMode=("false")


runBench

benchs="kiwi.KiWiMap"
thread="32"
writes=("50")
snapshots=("50")
chunks="2048 1024"
rebProb="2 5 10"
rebRatio="1.5 1.8"
ranges="10 100 1000"
sepThreadMode=("true")


runBench


