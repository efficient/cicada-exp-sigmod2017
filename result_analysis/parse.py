import pandas as pd
from pandasql import sqldf
import os
import re
import sys
import pprint

prefix = ''
suffix = ''

def parse_filename(filename):
  assert filename.startswith(prefix)
  assert filename.endswith(suffix)
  d = {}
  filename = filename[len(prefix):]
  if len(suffix) != 0:
    filename = filename[:-len(suffix)]
  for entry in filename.split('__'):
    key, _, value = entry.partition('@')
    if key in ('thread_count', 'total_count', 'record_size', 'req_per_query', 'tx_count', 'seq', 'warehouse_count', 'slow_gc', 'column_count', 'max_scan_len', 'scale_factor'):
      p_value = int(value)
    elif key in ('read_ratio', 'zipf_theta', 'fixed_backoff'):
      p_value = float(value)
    elif key in ('no_tsc', 'no_preval', 'no_newest', 'no_wsort', 'no_tscboost',
        'no_wait', 'no_inlining', 'no_backoff', 'full_table_scan',
        'simple_index_update'):
      p_value = 1
    elif key in ('bench', 'alg', 'tag'):
      p_value = value
    else: assert False, key
    assert value == str(p_value), key
    d[key] = p_value
  return d


# DBx1000
dbx1000_tput_pat = re.compile(r'^\[summary\] tput=(\d+)$', re.MULTILINE)
dbx1000_sim_time_pat = re.compile(r'^PASS! SimTime = (\d+)$', re.MULTILINE)
dbx1000_tx_count_pat = re.compile(r'^\[summary\] txn_cnt=(\d+), abort_cnt=(\d+), run_time=', re.MULTILINE)
dbx1000_latency_pat = re.compile(r'inter-commit latency \(us\): min=(\d+), max=(\d+), avg=(\d+); 50-th=(\d+), 95-th=(\d+), 99-th=(\d+), 99.9-th=(\d+)', re.MULTILINE)

def parse_dbx1000(s, d):
  dbx1000_tput_mat = dbx1000_tput_pat.search(s)
  dbx1000_sim_time_mat = dbx1000_sim_time_pat.search(s)
  dbx1000_tx_count_mat = dbx1000_tx_count_pat.search(s)
  dbx1000_latency_mat = dbx1000_latency_pat.search(s)

  d['tput'] = float(dbx1000_tput_mat.group(1))
  d['sim_time'] = float(dbx1000_sim_time_mat.group(1)) / 1000. / 1000. / 1000.
  d['committed_count'] = int(dbx1000_tx_count_mat.group(1))
  d['aborted_count'] = int(dbx1000_tx_count_mat.group(2))

  d['latency_min'] = int(dbx1000_latency_mat.group(1))
  d['latency_max'] = int(dbx1000_latency_mat.group(2))
  d['latency_avg'] = int(dbx1000_latency_mat.group(3))
  d['latency_50'] = int(dbx1000_latency_mat.group(4))
  d['latency_95'] = int(dbx1000_latency_mat.group(5))
  d['latency_99'] = int(dbx1000_latency_mat.group(6))
  d['latency_999'] = int(dbx1000_latency_mat.group(7))


# MICA
mica_tput_pat = re.compile(r'^throughput: \s*([\d.]+) M/sec$', re.MULTILINE)
mica_scan_tput_pat = re.compile(r'^scan throughput: \s*([\d.]+) M/sec$', re.MULTILINE)
mica_sim_time_pat = re.compile(r'^elapsed: \s*([\d.]+) sec$', re.MULTILINE)

mica_transactions_pat = re.compile(r'^transactions: \s*(\d+) .*$', re.MULTILINE)
mica_committed_pat = re.compile(r'^committed: \s*(\d+) .* ([\d.]+)% time\)$', re.MULTILINE)
mica_aborted_by_get_row_pat = re.compile(r'^aborted: get_row: \s*(\d+) .* ([\d.]+)% time\)$', re.MULTILINE)
mica_aborted_by_pre_validation_pat = re.compile(r'^aborted: pre_validation: \s*(\d+) .* ([\d.]+)% time\)$', re.MULTILINE)
mica_aborted_by_d_version_insert_pat = re.compile(r'^aborted: d_version_insert: \s*(\d+) .* ([\d.]+)% time\)$', re.MULTILINE)
mica_aborted_by_main_validation_pat = re.compile(r'^aborted: main_validation: \s*(\d+) .* ([\d.]+)% time\)$', re.MULTILINE)
mica_latency_pat = dbx1000_latency_pat

mica_net_row_count = re.compile(r'^net row count: \s*(\d+)$', re.MULTILINE)
mica_pending_iv = re.compile(r'^   i\s+pending: \s*(\d+)$', re.MULTILINE)
mica_committed_iv = re.compile(r'^   i\s+committed: \s*(\d+)$', re.MULTILINE)
mica_aborted_iv = re.compile(r'^   i\s+aborted: \s*(\d+)$', re.MULTILINE)
mica_unused_iv = re.compile(r'^   i\s+unused: \s*(\d+)$', re.MULTILINE)
mica_pending_niv = re.compile(r'^  ni\s+pending: \s*(\d+)$', re.MULTILINE)
mica_committed_niv = re.compile(r'^  ni\s+committed: \s*(\d+)$', re.MULTILINE)
mica_aborted_niv = re.compile(r'^  ni\s+aborted: \s*(\d+)$', re.MULTILINE)

def parse_mica(s, d):
  mica_tput_mat = mica_tput_pat.search(s)
  mica_scan_tput_mat = mica_scan_tput_pat.search(s)
  mica_sim_time_mat = mica_sim_time_pat.search(s)
  mica_latency_mat = mica_latency_pat.search(s)

  mica_transactions_mat = mica_transactions_pat.search(s)
  mica_committed_mat = mica_committed_pat.search(s)
  mica_aborted_by_get_row_mat = mica_aborted_by_get_row_pat.search(s)
  mica_aborted_by_pre_validation_mat = mica_aborted_by_pre_validation_pat.search(s)
  mica_aborted_by_d_version_insert_mat = mica_aborted_by_d_version_insert_pat.search(s)
  mica_aborted_by_main_validation_mat = mica_aborted_by_main_validation_pat.search(s)

  if 'tput' not in d:
    # Respect DBx1000's parsed output if exists
    d['tput'] = float(mica_tput_mat.group(1)) * 1000. * 1000.
    d['sim_time'] = float(mica_sim_time_mat.group(1))
    d['committed_count'] = mica_committed_count = int(mica_committed_mat.group(1))
    d['aborted_count'] = int(mica_transactions_mat.group(1)) - mica_committed_count
  if mica_scan_tput_mat:
    d['mica_scan_tput'] = float(mica_scan_tput_mat.group(1)) * 1000. * 1000.

    d['latency_min'] = int(mica_latency_mat.group(1))
    d['latency_max'] = int(mica_latency_mat.group(2))
    d['latency_avg'] = int(mica_latency_mat.group(3))
    d['latency_50'] = int(mica_latency_mat.group(4))
    d['latency_95'] = int(mica_latency_mat.group(5))
    d['latency_99'] = int(mica_latency_mat.group(6))
    d['latency_999'] = int(mica_latency_mat.group(7))

  d['mica_transactions_count'] = int(mica_transactions_mat.group(1))
  d['mica_committed_count'] = int(mica_committed_mat.group(1))
  d['mica_committed_time_perc'] = float(mica_committed_mat.group(2))
  d['mica_aborted_by_get_row_count'] = int(mica_aborted_by_get_row_mat.group(1))
  d['mica_aborted_by_get_row_time_perc'] = float(mica_aborted_by_get_row_mat.group(2))
  d['mica_aborted_by_pre_validation_count'] = int(mica_aborted_by_pre_validation_mat.group(1))
  d['mica_aborted_by_pre_validation_time_perc'] = float(mica_aborted_by_pre_validation_mat.group(2))
  d['mica_aborted_by_d_version_insert_count'] = int(mica_aborted_by_d_version_insert_mat.group(1))
  d['mica_aborted_by_d_version_insert_time_perc'] = float(mica_aborted_by_d_version_insert_mat.group(2))
  d['mica_aborted_by_main_validation_count'] = int(mica_aborted_by_main_validation_mat.group(1))
  d['mica_aborted_by_main_validation_time_perc'] = float(mica_aborted_by_main_validation_mat.group(2))

  d['mica_net_row_count'] = sum([int(v) for v in mica_net_row_count.findall(s)])
  d['mica_pending_iv'] = sum([int(v) for v in mica_pending_iv.findall(s)])
  d['mica_committed_iv'] = sum([int(v) for v in mica_committed_iv.findall(s)])
  d['mica_aborted_iv'] = sum([int(v) for v in mica_aborted_iv.findall(s)])
  d['mica_unused_iv'] = sum([int(v) for v in mica_unused_iv.findall(s)])
  d['mica_pending_niv'] = sum([int(v) for v in mica_pending_niv.findall(s)])
  d['mica_committed_niv'] = sum([int(v) for v in mica_committed_niv.findall(s)])
  d['mica_aborted_niv'] = sum([int(v) for v in mica_aborted_niv.findall(s)])


# SILO-REF/ERMIA-REF
silo_tput_pat = re.compile(r'agg_throughput: (.+) ops/sec$', re.MULTILINE)
#silo_latency_pat = re.compile(r'avg_latency: (.+) ms$', re.MULTILINE)
silo_sim_time_pat = re.compile(r'runtime: (.+) sec$', re.MULTILINE)
silo_abort_rate_pat = re.compile(r'agg_abort_rate: (.+) aborts/sec$', re.MULTILINE)

def parse_silo(s, d):
  silo_tput_mat = silo_tput_pat.search(s)
  #silo_latency_mat = silo_latency_pat.search(s)
  silo_sim_time_mat = silo_sim_time_pat.search(s)
  silo_abort_rate_mat = silo_abort_rate_pat.search(s)

  d['tput'] = silo_tput = float(silo_tput_mat.group(1))
  d['sim_time'] = silo_sim_time = float(silo_sim_time_mat.group(1))
  # approximations
  d['committed_count'] = silo_sim_time * silo_tput
  d['aborted_count'] = silo_sim_time * float(silo_abort_rate_mat.group(1))

  d['latency_min'] = 10000
  d['latency_max'] = 10000
  # the below is not inter-commit latency, so it is not compatible with DBx1000's
  #d['latency_avg'] = int(float(silo_latency_mat.group(1)) * 1000.) # ms to us
  d['latency_avg'] = 10000
  d['latency_50'] = 10000
  d['latency_95'] = 10000
  d['latency_99'] = 10000
  d['latency_999'] = 10000

# FOEDUS-REF/MOCC-REF output patterns
foedus_sim_time_pat = re.compile(r'final result:.*<duration_sec_>(.+)</duration_sec_>', re.MULTILINE)
foedus_processed_pat = re.compile(r'final result:.*<processed_>(.+)</processed_>', re.MULTILINE)
# foedus_user_aborts_pat = re.compile(r'final result:.*<user_requested_aborts_>(.+)</user_requested_aborts_>', re.MULTILINE)
foedus_race_aborts_pat = re.compile(r'final result:.*<race_aborts_>(.+)</race_aborts_>', re.MULTILINE)
foedus_largereadset_aborts_pat = re.compile(r'final result:.*<largereadset_aborts_>(.+)</largereadset_aborts_>', re.MULTILINE)
foedus_unexpected_aborts_pat = re.compile(r'final result:.*<unexpected_aborts_>(.+)</unexpected_aborts_>', re.MULTILINE)

def parse_foedus(s, d):
  foedus_sim_time_mat = foedus_sim_time_pat.search(s)
  foedus_processed_mat = foedus_processed_pat.search(s)
  # foedus_user_aborts_mat = foedus_user_aborts_pat.search(s)
  foedus_race_aborts_mat = foedus_race_aborts_pat.search(s)
  foedus_largereadset_aborts_mat = foedus_largereadset_aborts_pat.search(s)
  foedus_unexpected_aborts_mat = foedus_unexpected_aborts_pat.search(s)

  d['sim_time'] = foedus_sim_time = float(foedus_sim_time_mat.group(1))
  d['tput'] = int(foedus_processed_mat.group(1)) / foedus_sim_time
  d['committed_count'] = int(foedus_processed_mat.group(1))
  # we do not include user_aborts as race aborts
  d['aborted_count'] = int(foedus_race_aborts_mat.group(1)) + int(foedus_largereadset_aborts_mat.group(1)) + int(foedus_unexpected_aborts_mat.group(1))

  d['latency_min'] = 10000
  d['latency_max'] = 10000
  d['latency_avg'] = 10000
  d['latency_50'] = 10000
  d['latency_95'] = 10000
  d['latency_99'] = 10000
  d['latency_999'] = 10000


def load_data(path):
  query_cache_path = 'query.cache'

  if os.path.exists(query_cache_path):
    df = pd.read_pickle(query_cache_path)
    return df

  rows = []


  for filename in os.listdir(path):
    if not (filename.startswith(prefix) and filename.endswith(suffix)):
      continue
    if filename.endswith('.old') or filename.find('.failed-') != -1:
      continue

    exp = parse_filename(filename)
    d = exp

    exp_path = os.path.join(path, filename)
    s = open(exp_path).read()
    # print(s)

    d['path'] = exp_path

    output_type = []

    if filename.find('-REF') == -1 and filename.find('tag@native') == -1:
      output_type.append('DBx1000')
    if filename.find('MICA') != -1:
      output_type.append('MICA')
    if filename.find('SILO-') != -1 or filename.find('ERMIA-') != -1:
      output_type.append('SILO')
    if filename.find('FOEDUS-') != -1:
      output_type.append('FOEDUS')

    assert len(output_type) == 1 or output_type == ['DBx1000', 'MICA'], (exp_path, output_type)

    if 'DBx1000' in output_type:
      parse_dbx1000(s, d)
    if 'MICA' in output_type:
      parse_mica(s, d)
    if 'SILO' in output_type:
      parse_silo(s, d)
    if 'FOEDUS' in output_type:
      parse_foedus(s, d)
      #pprint.pprint(d)
      #assert False

    d['abort_rate'] = float(d['aborted_count']) / (d['committed_count'] + d['aborted_count'])
    rows.append(d)

  df = pd.DataFrame(rows)
  # df = df.sort_values()

  df.to_pickle(query_cache_path)

  return df


def read_latency_dist(filename, latency_bins):
  latency_line = False
  for line in open(filename).readlines():
    line = line.strip()
    if line == 'LatencyStart':
      latency_line = True
    elif line == 'LatencyEnd':
      latency_line = False
    elif latency_line:
      us, _, count = line.partition(' ')
      us = int(us.strip())
      count = int(count.strip())
      latency_bins[us] += count
