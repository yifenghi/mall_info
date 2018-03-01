#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.analyse.MachineCalculatorLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`
date_today=`date -d "$date_now" "+%Y%m%d"`

#input="$statistic/day/$dir_name"
input="$statistic/machine/$date_today"
for((i=1;i<$machine_input_num;i++))
do
  date_num=`date -d "$date_today $i days ago" "+%Y%m%d"`
  input="$input,$statistic/machine/$date_num"
done
output="$statistic/machine/info/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D dwell.per.location.filter="24" \
-D visit.times.filter="3" \

exit 0;
