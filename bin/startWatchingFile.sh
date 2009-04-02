#!/bin/bash

if [ $# = 0 -o $# -gt 2 ]; then
  echo "usage: startWatchingFile <filename> [datatype]"
  exit
fi

filename=$1

if [ $# -ge 2 ]; then
datatype=$2
else
datatype=raw
fi
nc localhost 9093 <<END
add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8  $datatype 0  $filename  0
close
END
