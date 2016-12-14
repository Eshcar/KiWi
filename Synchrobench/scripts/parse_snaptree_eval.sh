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
iterations="10"

declare -a writes
declare -a snapshots

declare -a keysRange
keysRange=("2000000")

declare -a initSize
initSize=("1000000")

declare -a sepThreadMode

distribution="uniform"

output=${dir}/output_snaptree_eval
out=${output}/snaptree_eval.csv

rm ${out}

printHeader ${out}

benchs="trees.lockbased.LockBasedSnapTree"
thread="2 4 8 16 24 32"
writes=("50")
snapshots=("50")
chunks="2500"
rebProb="0"
rebRatio="0"
mergeRatio="0"
ranges="8 32 1024 32768"
sepThreadMode=("true")
fillType="random"

printData


benchs="trees.lockbased.LockBasedSnapTree"
thread="2 4 8 16 24 32"
writes=("100" "0" "50")
snapshots=("0" "0" "0")
chunks="2500"
rebProb="0"
rebRatio="0"
ranges="0"
mergeRatio="0"
sepThreadMode=("false")
fillType="random"

printData

benchs="trees.lockbased.LockBasedSnapTree"
thread="2 4 8 16 24 32"
writes=("0")
snapshots=("100")
chunks="2500"
rebProb="0"
rebRatio="0"
mergeRatio="0"
ranges="8 32 1024 32768"
sepThreadMode=("false")
fillType="random"

printData


l="5000"
warmup="0"
writeall="0"
iterations="3"
benchs="trees.lockbased.LockBasedSnapTree"
thread="32"
chunks="2500"
rebProb="0"
rebRatio="0"
mergeRatio="0"
ranges="0"
writes=("100" "0" "50")
snapshots=("0" "0" "0")
sepThreadMode=("false")
fillType="ordered"
#printData


