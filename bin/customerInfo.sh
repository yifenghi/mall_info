#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.analyse.CustomerInfoLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
fi
today=`date -d "$date 1 second ago" "+%Y-%m-%d 00:00:00"`
dir_today=`date -d "$date" "+%Y%m%d"`
customer_list="$work_dir/data/testCustomer"

#input_shop="$statistic/shop/$dir_hour"
input="/group/doodod/mall/statistic/day/20141123/00/00*"

output="$hdfs_home/test/customer/$dir_today"

hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $customer_list \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D mall.system.today="$today" \
-D customer.mac.path="`basename $customer_list`" \

exit 0;
