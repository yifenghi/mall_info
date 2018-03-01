#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.visit.VisitMallLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
fi
today=`date -d "$date 1 second ago" "+%Y-%m-%d 00:00:00"`
dir_hour=`date -d "$date" "+%Y%m%d/%H/00"`
dir_today=`date -d "$date" "+%Y%m%d"`
dir_yesterday=`date -d "$today 1 day ago" "+%Y%m%d"`
dir_today_last=`date -d "$today" "+%Y%m%d"`
hour_tag=`date -d "$date" "+%H:%M"`

input_floor="$statistic/day/$dir_hour"
input_total="$visit_mall/$dir_yesterday"

hexist $input_total
if [ $? != 0 ]
then
  input_total=$input_empty
fi

output="$visit_mall/$dir_today"

hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D merge.input.part="$input_floor" \
-D merge.input.total="$input_total" \
-D mall.system.today="$today" \

if [ $hour_tag == "00:00" ]
then
   hmv "$visit_mall/$dir_today_last" "$visit_mall/$dir_today_last.bak"
   hcp "$output" "$visit_mall/$dir_today_last" 
fi
exit 0;
