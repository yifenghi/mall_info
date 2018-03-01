#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.statistic.ShopLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`

##add output
input_total="$statistic/day/$dir_name/part*"

output="$statistic/shop/$dir_name"
hrmr $output 

public_conf="$work_dir/data/publicServicePoint"
store_conf="$work_dir/data/shop"
frame_conf="$work_dir/data/frame"

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $public_conf,$store_conf,$frame_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input_total" \
-D shop.conf.public="`basename $public_conf`" \
-D shop.conf.store="`basename $store_conf`" \
-D shop.conf.frame="`basename $frame_conf`" \

exit 0;
