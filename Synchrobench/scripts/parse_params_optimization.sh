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

output=${dir}/output_oracle_params_optimization
out=${output}/data/params_optimization.csv

rm ${out}
printHeader ${out}

benchs="kiwi.KiWiMap"
thread="32"
writes=("100" "0" "50")
snapshots=("0" "0" "0")
chunks="3500 3000 2500"
rebProb="10 12 15"
rebRatio="1.6 1.7 1.8"
ranges="0"
sepThreadMode=("false")
fillType="random"
mergeRatio="0.5 0.7"

printData


benchs="kiwi.KiWiMap"
thread="32"
writes=("0")
snapshots=("100")
chunks="3500 3000 2500"
rebProb="10 12 15"
rebRatio="1.6 1.7 1.8"
ranges="1024"
sepThreadMode=("false")
fillType="random"
mergeRatio="0.5 0.7"


printData

benchs="kiwi.KiWiMap"
thread="32"
writes=("50")
snapshots=("50")
chunks="3500 3000 2500"
rebProb="10 12 15"
rebRatio="1.6 1.7 1.8"
ranges="1024"
sepThreadMode=("true")
fillType="random"
mergeRatio="0.5 0.7"


printData


