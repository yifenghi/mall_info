#!/bin/sh 

set -ux
#work_dir=$(readlink -f $(dirname $0))
work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf

if [ $# -eq 1 ]
then
  date=$1
else
  exit 1
fi

log_date=`date -d "$date" +%Y%m%d`
log_path="$work_dir/log/mall_day.$log_date.log"

sh $work_dir/bin/visitCustomer.sh "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/machineInfo.sh "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/machineCal.sh "$date" 2>&1 | tee -a "$log_path"
