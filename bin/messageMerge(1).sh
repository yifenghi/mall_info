#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.statistic.MergeLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
date_5mins_ago=`date -d "$date 5 minutes ago" "+%Y-%m-%d %H:%M:%S"`
dir_name_5min=`date -d "$date_5mins_ago" "+%Y%m%d/%H/%M"`
input_part="$statistic/min/$dir_name"
input_total="$statistic/day/$dir_name_5min"
machine_conf="$work_dir/data/machine_list"
employee_conf="$work_dir/data/employee_list"
frame_conf="$work_dir/data/frame"
mallid_conf="$work_dir/data/floormall_map"
#for test
#input_total="$statistic/min/$dir_name_5min"
hexist $input_total
if [ $? != 0 ]
then
  input_total=$input_empty
fi

hour_tag=`date -d "$date" "+%H%M"`
if [ $hour_tag == $hour_empty_tag ]
then
  input_total=$input_empty
fi
output="$statistic/day/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $machine_conf,$employee_conf,$frame_conf,$mallid_conf \
-D mapreduce.job.name="$job_name" \
-D mall.system.bizdate="$date" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D merge.input.part="$input_part" \
-D merge.input.total="$input_total" \
-D conf.machine.list="`basename $machine_conf`" \
-D conf.employee.list="`basename $employee_conf`" \
-D shop.conf.frame="`basename $frame_conf`" \
-D conf.floormall.map="`basename $mallid_conf`" \
-D conf.filter.dwell.percent="$filter_frame_dwell_percent" \
-D conf.floor.filter="$filter_floors" \
-D conf.location.filter="$filter_locations" \
-D conf.passenger.filter="$filter_dwell" \

exit 0;
