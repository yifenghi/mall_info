#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

date_today=`date -d "$date" "+%Y%m%d"`
date_one_day=`date -d "$date 1 day ago" "+%Y%m%d"`
date_one_week=`date -d "$date 7 days ago" "+%Y%m%d"`
date_one_month=`date -d "$date 30 days ago" "+%Y%m%d"`

hrmr "$statistic/min/$date_one_day"
hrmr "$statistic/trace/$date_one_day"

hrmr "$dwell_floor/$date_one_day"
hrmr "$dwell_shop/$date_one_day"
hrmr "$dwell_mall/$date_one_day"
hrmr "$flow_floor/$date_one_day"
hrmr "$flow_shop/$date_one_day"
hrmr "$flow_mall/$date_one_day"
hrmr "$flow_mall_shop/$date_one_day"

hrmr "$visit_floor/$date_one_week"
hrmr "$visit_shop/$date_one_week"
hrmr "$visit_mall/$date_one_week"
hrmr "$visit_floor/$date_one_week.bak"
hrmr "$visit_shop/$date_one_week.bak"
hrmr "$visit_mall/$date_one_week.bak"
hrmr "$visit_floor/info/$date_one_day"
hrmr "$visit_shop/info/$date_one_day"
hrmr "$visit_mall/info/$date_one_day"
hrmr "$visit_mall/customer/$date_one_day"
hrmr "$visit_brand/$date_one_day"

#hrmr "$statistic/day/$date_one_week"
hrmr "$statistic/shop/$date_one_week"
hrmr "$statistic/machine/$date_one_week"
hrmr "$statistic/machine/info/$date_one_week"
hrmr "$statistic/machine/night/$date_one_week"

hrmr "$statistic/day/$date_one_day/[12]*"
hrmr "$statistic/day/$date_one_day/0[!0]*"
hrmr "$statistic/day/$date_one_day/00/[!0]*"
hrmr "$statistic/day/$date_one_day/00/05"
hrmr "$statistic/day/$date_one_month"

hrmr "$statistic/shop/$date_one_day/[12]*"
hrmr "$statistic/shop/$date_one_day/0[!0]*"
hrmr "$statistic/shop/$date_one_day/00/[!0]*"
hrmr "$statistic/shop/$date_one_day/00/05"
hrmr "$statistic/shop/$date_one_month"

work_dir="/var/lib/hadoop-hdfs/code/mall_info/bin/.."
cat $work_dir/log/mall_info.$date_one_day* > "$work_dir/log/mall_info.merge.$date_one_day.log"
rm $work_dir/log/mall_info.$date_one_day* 
rm "$work_dir/log/mall_visit.$date_one_week.log"
rm "$work_dir/log/mall_info.merge.$date_one_week.log"
rm "$work_dir/log/mall_day.$date_one_week.log"

