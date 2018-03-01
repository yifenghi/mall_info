#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.visit.VisitPhoneBrandLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
fi
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
today=`date -d "$date 1 second ago" "+%Y-%m-%d 00:00:00"`
dir_today=`date -d "$date 1 second ago" "+%Y%m%d"`
brand_list="$work_dir/data/phonebrand_list"

input="$statistic/day/$dir_name"
output="$visit_brand/$dir_today"

hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $brand_list \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D mall.system.today="$today" \
-D conf.brand.list="`basename $brand_list`" \

exit 0;
