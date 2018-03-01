#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.visit.VisitFloorInfoLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
fi
#dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`
#date=`date -d "$date_now 1 second ago" "+%Y-%m-%d 00:00:00"`
today=`date -d "$date" "+%Y-%m-%d 00:00:00"`
dir_today=`date -d "$date" "+%Y%m%d"`

#input_floor="$statistic/floor/$dir_name"
input="$visit_floor/$dir_today"
output="$visit_floor/info/$dir_today"

hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D mall.system.today="$today" \
-D mall.system.columns="$columns" \

exit 0;
