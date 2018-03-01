#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..
#work_dir="/Users/paul/Documents/code_dir/doodod/mall_info"

mall_id=64
#output_dir="/var/lib/hadoop-hdfs/code/mall_info/data/tmp"
output_dir="$work_dir/data"
hostname="http://apiv2.palmap.cn"
class="com.doodod.mall.data.shopPolygon"
main_jar="$work_dir/target/mall_info-1.0-SNAPSHOT-jar-with-dependencies.jar"
json_jar="$work_dir/../local_lib/json-1.0.jar"

cmd="java -cp $main_jar:$json_jar $class $mall_id $output_dir $hostname"
eval $cmd
