#!/bin/bash

readonly DB=$1
readonly TEST_SETTING=$2
readonly OUTPUT_PATH=scripts/throughput-latency/${TEST_SETTING}
readonly TMP_OUTPUT_PATH=${OUTPUT_PATH}/tmp
mkdir -p $TMP_OUTPUT_PATH

readonly WARMUP_DUARTION=10
readonly RUN_DURATION=130
readonly RECORD_COUNT=2000000
readonly OP_COUNT_PER_CLIENT=3000000
readonly CLIENTS=(32 64 128 192 256)

for i in ${!CLIENTS[@]}; do
  ./bin/ycsb load $DB -P workloads/workloada \
  -p recordcount=$RECORD_COUNT \
  -p fieldcount=5 \
  -threads 32 -s

  sleep 10
  client=${CLIENTS[$i]}
  target_op_count=$((client * OP_COUNT_PER_CLIENT))
  log_path=${TMP_OUTPUT_PATH}/${client}_client_wa.dat
  ./bin/ycsb run $DB -P workloads/workloada \
    -p recordcount=$RECORD_COUNT \
    -p operationcount=$target_op_count \
    -p maxexecutiontime=$RUN_DURATION \
    -p fieldcount=5 \
    -p writeallfields=true \
    -p combineop=true \
    -p warmup=10 \
    -p exportercdf=true \
    -threads $client -s | tee $log_path

  mv OVERALL-latency-cdf.dat ${OUTPUT_PATH}/${client}-clients.dat

  dat_file=${OUTPUT_PATH}/result.dat
  if [ ! -f $dat_file ]; then
    echo "Clients Throughput Average 99th" >$dat_file
  fi

  ops=`awk 'NR==2' ${log_path} | awk -F", " '{print $3}'`
  avg=`awk 'NR==4' ${log_path} | awk -F", " '{print $3}'`
  tail_latency=`awk 'NR==8' ${log_path} | awk -F", " '{print $3}'`
  echo "${client} ${ops} ${avg} ${tail_latency}" >>$dat_file
  sleep 60
done
