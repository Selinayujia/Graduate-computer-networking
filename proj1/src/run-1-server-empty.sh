#!/bin/bash

##Remove old netsort executable
rm -f netsort

##Build netsort
go build -o netsort netsort.go


INPUT_FILE_PATH='testcases/testcase5/input-0.dat'
OUTPUT_FILE_PATH='testcases/testcase5/output-0.dat'
CONFIG_FILE_PATH='testcases/testcase5/config.yaml'
nohup ./netsort 0 ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH} &
wait
