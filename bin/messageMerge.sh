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
machine="$work_dir/data/machine_list"
employee="$work_dir/data/employee_list"
frame_conf="$work_dir/data/frame"
#for test
#input_total="$statistic/min/$dir_name_5min"
hexist $input_total
if [ $? != 0 ]
then
  input_total=$input_empty
fi

hour_tag=`date -d "$date" "+%H%M"`
#if [ $hour_tag == $hour_empty_tag ]
#then
#  input_total=$input_empty
#fi
input_part="$input_empty"
output="$statistic/day/test/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $machine,$employee,$frame_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mall.system.bizdate="$date" \
-D merge.input.part="$input_part" \
-D merge.input.total="$input_total" \
-D conf.machine.list="`basename $machine`" \
-D conf.employee.list="`basename $employee`" \
-D conf.passenger.filter="$filter_passenger" \
-D shop.conf.frame="`basename $frame_conf`" \
-D conf.filter.dwell.percent="$filter_frame_dwell_percent" \
-D conf.floor.filter="1" \
-D conf.location.filter="4" \
-D conf.passenger.filter="11" \

exit 0;
