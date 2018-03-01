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

log_date=`date -d "$date" +%Y%m%d%H%M%S`
log_path="$work_dir/log/mall_info.$log_date.log"

sh $work_dir/bin/mallMessage.sh  "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/messageMerge.sh "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/floorFlow.sh    "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/floorDwell.sh   "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/shopMessage.sh  "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/shopFlow.sh     "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/shopDwell.sh    "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/mallFlow.sh     "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/mallDwell.sh    "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/flowMallShop.sh "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/customerAtPoint.sh "$date" 2>&1 | tee -a "$log_path"
sh $work_dir/bin/customerTarce.sh   "$date" 2>&1 | tee -a "$log_path"
