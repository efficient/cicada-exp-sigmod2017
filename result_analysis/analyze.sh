#!/bin/sh

if [ -z "$1" ]; then
  echo Usage: $0 EXP_DATA_PATH
  exit 1
fi

EXP_DATA=$(readlink -f "$1")
OUTPUT_PATH=$(basename "$1" | sed -e 's/^exp_data_/output_/')

echo "EXP_DATA=$EXP_DATA"
echo "OUTPUT_PATH=$OUTPUT_PATH"

[ ! -d "$OUTPUT_PATH" ] && mkdir "$OUTPUT_PATH"
cd "$OUTPUT_PATH"

rm exp_data 2> /dev/null
rm output_*.pdf 2> /dev/null
rm output_*.png 2> /dev/null
rm output_*.txt 2> /dev/null
rm query.cache 2> /dev/null

ln -s "$EXP_DATA" exp_data

python3 -u ../query.py | tee output_query.txt

python3 ../plot_ycsb.py
python3 ../plot_tpcc.py
# python3 ../plot_tatp.py

python3 ../plot_inlining.py
python3 ../plot_backoff.py
python3 ../plot_gc.py
python3 ../plot_factor.py
#python3 ../plot_singlekey.py

python3 ../plot_latency.py
