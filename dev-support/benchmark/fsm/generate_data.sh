#!/usr/bin/env bash

$HADOOP_PREFIX/bin/hdfs dfs -rm -r /user/hduser/input

./generate_predicatable_data.sh
#./duplicate_tlf_data.sh
#./generate_graphgen_data.sh