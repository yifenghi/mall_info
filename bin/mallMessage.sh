#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.mall.statistic.MallInfoLauncher
#class=com.doodod.mall.common.TestLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

#date=`date "+%Y-%m-%d %H:%M:%S"`
if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

date_5mins_ago=`date -d "$date 5 minutes ago" "+%Y-%m-%d %H:%M:%S"`
date_today=`date -d "$date_5mins_ago" "+%Y%m%d"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
brand_conf="$work_dir/data/mac_brand"

output="$statistic/min/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $brand_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D store.businesstime.start="$date_5mins_ago" \
-D store.businesstime.now="$date" \
-D conf.mac.brand="`basename $brand_conf`" \

exit 0;
