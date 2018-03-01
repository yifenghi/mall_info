#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.trace.CustomerTarceLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`
today=`date -d "$date_now 1 second ago" "+%Y-%m-%d 00:00:00"`

input_floor="$statistic/day/$dir_name"
output="$statistic/trace/trace/$dir_name"
customer_list="$work_dir/data/customer_list"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $customer_list \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input_floor" \
-D mall.system.today="$today" \
-D customer.conf.vip="`basename $customer_list`" \

exit 0;

