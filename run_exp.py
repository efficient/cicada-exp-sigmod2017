#!/usr/bin/python

# vim: set expandtab softtabstop=2 shiftwidth=2:

import glob
import os
import sys
import re
import time
import shutil
import subprocess
import pprint


COLOR_RED = '\x1b[31m'
COLOR_GREEN = '\x1b[32m'
COLOR_YELLOW = '\x1b[33m'
COLOR_BLUE = '\x1b[34m'
COLOR_MAGENTA = '\x1b[35m'
COLOR_CYAN ='\x1b[36m'
COLOR_RESET = '\x1b[0m'


def replace_def(conf, name, value):
  pattern = r'^#define %s\s+.+$' % re.escape(name)
  repl = r'#define %s %s' % (name, value)
  s, n = re.subn(pattern, repl, conf, flags=re.MULTILINE)
  assert n == 1, 'failed to replace def: %s=%s' % (name, value)
  return s


def set_alg(conf, alg, **kwargs):
  conf = replace_def(conf, 'CC_ALG', alg.partition('-')[0].partition('+')[0])
  conf = replace_def(conf, 'ISOLATION_LEVEL', 'SERIALIZABLE')

  if alg == 'SILO':
    conf = replace_def(conf, 'VALIDATION_LOCK', '"waiting"')
    conf = replace_def(conf, 'PRE_ABORT', '"false"')
  else:
    conf = replace_def(conf, 'VALIDATION_LOCK', '"no-wait"')
    conf = replace_def(conf, 'PRE_ABORT', '"true"')

  if alg == 'MICA':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')
  elif alg == 'MICA+INDEX':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')
  elif alg == 'MICA+FULLINDEX':
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_MICA')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'true')
  else:
    conf = replace_def(conf, 'INDEX_STRUCT', 'IDX_HASH')
    conf = replace_def(conf, 'MICA_FULLINDEX', 'false')

  # conf = replace_def(conf, 'RCU_ALLOC', 'false')
  conf = replace_def(conf, 'RCU_ALLOC', 'true')
  if alg.startswith('MICA'):
      # ~8192 huge pages (16 GiB) for RCU
    conf = replace_def(conf, 'RCU_ALLOC_SIZE', str(int(8192 * 0.99) * 2 * 1048576) + 'UL')
  else:
    conf = replace_def(conf, 'RCU_ALLOC_SIZE', str(int(hugepage_count[alg] * 0.99) * 2 * 1048576) + 'UL')

  return conf


def set_ycsb(conf, thread_count, total_count, record_size, req_per_query, read_ratio, zipf_theta, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'YCSB')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(record_size))
  conf = replace_def(conf, 'INIT_PARALLELISM', str(thread_count))
  conf = replace_def(conf, 'PART_CNT', str(min(2, thread_count))) # try to use both NUMA node, but do not create too many partitions

  conf = replace_def(conf, 'SYNTH_TABLE_SIZE', str(total_count))
  conf = replace_def(conf, 'REQ_PER_QUERY', str(req_per_query))
  conf = replace_def(conf, 'READ_PERC', str(read_ratio))
  conf = replace_def(conf, 'WRITE_PERC', str(1. - read_ratio))
  conf = replace_def(conf, 'SCAN_PERC', 0)
  conf = replace_def(conf, 'ZIPF_THETA', str(zipf_theta))

  return conf


def set_tpcc(conf, thread_count, bench, warehouse_count, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'TPCC')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(704))
  conf = replace_def(conf, 'NUM_WH', str(warehouse_count))
  # INIT_PARALLELISM does not affect tpcc initialization
  conf = replace_def(conf, 'INIT_PARALLELISM', str(warehouse_count))
  conf = replace_def(conf, 'PART_CNT', str(warehouse_count))

  if bench == 'TPCC':
    conf = replace_def(conf, 'TPCC_INSERT_ROWS', 'false')
    conf = replace_def(conf, 'TPCC_DELETE_ROWS', 'false')
    conf = replace_def(conf, 'TPCC_INSERT_INDEX', 'false')
    conf = replace_def(conf, 'TPCC_DELETE_INDEX', 'false')
    conf = replace_def(conf, 'TPCC_FULL', 'false')
  elif bench == 'TPCC-FULL':
    conf = replace_def(conf, 'TPCC_INSERT_ROWS', 'true')
    conf = replace_def(conf, 'TPCC_DELETE_ROWS', 'true')
    conf = replace_def(conf, 'TPCC_INSERT_INDEX', 'true')
    conf = replace_def(conf, 'TPCC_DELETE_INDEX', 'true')
    conf = replace_def(conf, 'TPCC_FULL', 'true')
  else:
    assert False

  if 'simple_index_update' in kwargs:
    conf = replace_def(conf, 'TPCC_VALIDATE_GAP', 'false')
    conf = replace_def(conf, 'TPCC_VALIDATE_NODE', 'false')
    conf = replace_def(conf, 'SIMPLE_INDEX_UPDATE', 'true')

  return conf


def set_tatp(conf, thread_count, scale_factor, tx_count, **kwargs):
  conf = replace_def(conf, 'WORKLOAD', 'TATP')
  conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
  conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
  conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(67))
  conf = replace_def(conf, 'TATP_SCALE_FACTOR', str(scale_factor))
  conf = replace_def(conf, 'INIT_PARALLELISM', str(thread_count))
  conf = replace_def(conf, 'PART_CNT', str(min(2, thread_count))) # try to use both NUMA node, but do not create too many partitions

  return conf


def set_threads(conf, thread_count, **kwargs):
  return replace_def(conf, 'THREAD_CNT', thread_count)


def set_mica_confs(conf, **kwargs):
  if 'no_tsc' in kwargs:
    conf = replace_def(conf, 'MICA_NO_TSC', 'true')
  if 'no_preval' in kwargs:
    conf = replace_def(conf, 'MICA_NO_PRE_VALIDATION', 'true')
  if 'no_newest' in kwargs:
    conf = replace_def(conf, 'MICA_NO_INSERT_NEWEST_VERSION_ONLY', 'true')
  if 'no_wsort' in kwargs:
    conf = replace_def(conf, 'MICA_NO_SORT_WRITE_SET_BY_CONTENTION', 'true')
  if 'no_tscboost' in kwargs:
    conf = replace_def(conf, 'MICA_NO_STRAGGLER_AVOIDANCE', 'true')
  if 'no_wait' in kwargs:
    conf = replace_def(conf, 'MICA_NO_WAIT_FOR_PENDING', 'true')
  if 'no_inlining' in kwargs:
    conf = replace_def(conf, 'MICA_NO_INLINING', 'true')
  if 'no_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_NO_BACKOFF', 'true')
  if 'fixed_backoff' in kwargs:
    conf = replace_def(conf, 'MICA_USE_FIXED_BACKOFF', 'true')
    conf = replace_def(conf, 'MICA_FIXED_BACKOFF', str(kwargs['fixed_backoff']))
  if 'slow_gc' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SLOW_GC', 'true')
    conf = replace_def(conf, 'MICA_SLOW_GC', str(kwargs['slow_gc']))
  #  if 'column_count' in kwargs:
  #    conf = replace_def(conf, 'MICA_COLUMN_COUNT', str(kwargs['column_count']))
  if 'max_scan_len' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SCAN', 'true')
    conf = replace_def(conf, 'MICA_MAX_SCAN_LEN', str(kwargs['max_scan_len']))
  if 'full_table_scan' in kwargs:
    conf = replace_def(conf, 'MICA_USE_SCAN', 'true')
    conf = replace_def(conf, 'MICA_USE_FULL_TABLE_SCAN', 'true')
  return conf


dir_name = None
old_dir_name = None

node_count = None
max_thread_count = None

prefix = ''
suffix = ''
total_seqs = 5
max_retries = 3

hugepage_count = {
  # 32 GiB
  'SILO': 32 * 1024 / 2,
  'TICTOC': 32 * 1024 / 2,
  'NO_WAIT': 32 * 1024 / 2,
  # 32 GiB + (16 GiB for RCU)
  'MICA': (32 + 16) * 1024 / 2,
  'MICA+INDEX': (32 + 16) * 1024 / 2,
  # 96 GiB
  'HEKATON': 96 * 1024 / 2,

  # 48 GiB (16 threads, 28 warehouses use much more memory for some reason)
  'SILO-REF': 48 * 1024 / 2,
  'SILO-REF-BACKOFF': 48 * 1024 / 2,

  # 80 GiB (using too much hugepages can reduce tput)
  'ERMIA-SI-REF': 80 * 1024 / 2,
  'ERMIA-SI-REF-BACKOFF': 80 * 1024 / 2,
  'ERMIA-SSI-REF': 80 * 1024 / 2,
  'ERMIA-SSI-REF-BACKOFF': 80 * 1024 / 2,
  'ERMIA-SI_SSN-REF': 80 * 1024 / 2,
  'ERMIA-SI_SSN-REF-BACKOFF': 80 * 1024 / 2,

  # 112 GiB
  'FOEDUS-MOCC-REF': 112 * 1024 / 2,
  'FOEDUS-OCC-REF': 112 * 1024 / 2,
}

def gen_filename(exp):
  s = ''
  for key in sorted(exp.keys()):
    s += key
    s += '@'
    s += str(exp[key])
    s += '__'
  return prefix + s.rstrip('__') + suffix


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


def remove_stale():
  exps = []
  for seq in range(total_seqs):
    exps += list(enum_exps(seq))

  valid_filenames = set([gen_filename(exp) for exp in exps])

  for filename in os.listdir(dir_name):
    if filename.endswith('.old'):
      continue
    if filename.find('.failed-') != -1:
      continue
    if not (filename.startswith(prefix) and filename.endswith(suffix)):
      continue
    if filename in valid_filenames:
      continue

    if not os.path.exists(old_dir_name):
      os.mkdir(old_dir_name)
    print('stale file: %s' % filename)
    os.rename(dir_name + '/' + filename, old_dir_name + '/' + filename)


def comb_dict(*dicts):
  d = {}
  for dict in dicts:
    d.update(dict)
  return d


def format_exp(exp):
  return pprint.pformat(exp).replace('\n', '')


def enum_exps(seq):
  all_algs = ['MICA', 'MICA+INDEX', #'MICA+FULLINDEX',
              'SILO', 'TICTOC', 'HEKATON', 'NO_WAIT',
              'SILO-REF',
              # 'SILO-REF-BACKOFF',
              #'ERMIA-SI-REF',
              #'ERMIA-SSI-REF',
              'ERMIA-SI_SSN-REF',
              # 'ERMIA-SI-REF-BACKOFF',
              # 'ERMIA-SSI-REF-BACKOFF',
              # 'ERMIA-SI_SSN-REF-BACKOFF',
              'FOEDUS-MOCC-REF',
              'FOEDUS-OCC-REF',
             ]

  macrobenchs = ['macrobench']
  factors = ['factor']
  # macrobenchs = ['macrobench', 'native-macrobench']
  # factors = ['factor', 'native-factor']

  for tag in macrobenchs:
    for alg in all_algs:
      if tag == 'macrobench' and alg in ('MICA+FULLINDEX',):
      # if tag == 'macrobench' and alg in ('MICA+INDEX', 'MICA+FULLINDEX'):
        continue
      if tag == 'native-macrobench' and alg not in ('MICA', 'MICA+INDEX', 'MICA+FULLINDEX'):
        continue

      for thread_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        if alg in ('FOEDUS-MOCC-REF', 'FOEDUS-OCC-REF') and thread_count < 4:
          # Broken in FOEDUS
          continue

        # YCSB
        # if alg.find('-REF') == -1:
        if True:
          ycsb = dict(common)
          total_count = 10 * 1000 * 1000
          ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

          # record_size = 1000
          record_size = 100
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            # for zipf_theta in [0.00, 0.90, 0.99]:
            for zipf_theta in [0.00, 0.99]:
              #if zipf_theta >= 0.95:
              #  if read_ratio == 0.50 and alg == 'NO_WAIT': continue
              #  if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          #record_size = 1000
          record_size = 100
          req_per_query = 1
          tx_count = 2000000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            # for zipf_theta in [0.00, 0.90, 0.99]:
            for zipf_theta in [0.00, 0.99]:
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

        # TPCC
        if alg.find('-REF') == -1:
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

          # for warehouse_count in [1, 4, 16, max_thread_count]:
          for warehouse_count in [1, 4, max_thread_count]:
            if tag != 'macrobench': continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

          for warehouse_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
            if tag != 'macrobench': continue
            if thread_count not in [max_thread_count, warehouse_count]: continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        # full TPCC
        if alg not in ('MICA',):  # MICA must use the native index
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC-FULL', 'tx_count': tx_count })

          # for warehouse_count in [1, 4, 16, max_thread_count]:
          for warehouse_count in [1, 4, max_thread_count]:
            if tag != 'macrobench': continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

          for warehouse_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
            if tag != 'macrobench': continue
            if thread_count not in [max_thread_count, warehouse_count]: continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        # full TPCC with simple index update
        # (delayed index update, no phantom avoidance)
        if alg not in ('MICA',) and alg.find('-REF') == -1:
          tpcc = dict(common)
          tx_count = 200000
          tpcc.update({ 'bench': 'TPCC-FULL', 'tx_count': tx_count,
            'simple_index_update': 1 })

          # for warehouse_count in [1, 4, 16, max_thread_count]:
          for warehouse_count in [1, 4, max_thread_count]:
            if tag != 'macrobench': continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

          for warehouse_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
            if tag != 'macrobench': continue
            if thread_count not in [max_thread_count, warehouse_count]: continue
            tpcc.update({ 'warehouse_count': warehouse_count })
            yield dict(tpcc)

        # TATP
        # if alg.find('-REF') == -1:
        #   tatp = dict(common)
        #   tx_count = 200000
        #   tatp.update({ 'bench': 'TATP', 'tx_count': tx_count })
        #
        #   # for scale_factor in [1, 2, 5, 10, 20, 50, 100]:
        #   # for scale_factor in [1, 10]:
        #   for scale_factor in [1]:
        #     if tag != 'macrobench': continue
        #     tatp.update({ 'scale_factor': scale_factor })
        #     yield dict(tatp)

      for thread_count in [max_thread_count]:
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # YCSB
        # if alg.find('-REF') == -1:
        if True:
          ycsb = dict(common)
          total_count = 10 * 1000 * 1000
          ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

          # record_size = 1000
          record_size = 100
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            for zipf_theta in [0.00, 0.40, 0.60, 0.80, 0.90, 0.95, 0.99]:
              #if zipf_theta >= 0.95:
              #  if read_ratio == 0.50 and alg == 'NO_WAIT': continue
              #  if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

          # record_size = 1000
          record_size = 100
          req_per_query = 1
          tx_count = 2000000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          for read_ratio in [0.50, 0.95]:
            for zipf_theta in [0.00, 0.40, 0.60, 0.80, 0.90, 0.95, 0.99]:
              #if zipf_theta >= 0.95:
              #  if read_ratio == 0.50 and alg == 'NO_WAIT': continue
              #  if read_ratio == 0.50 and alg == 'HEKATON': continue
              ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
              yield dict(ycsb)

  tag = 'inlining'
  for alg in all_algs:
  # for alg in ['MICA', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
    for thread_count in [max_thread_count]:
      if alg.find('-REF') == -1:
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # YCSB
        ycsb = dict(common)
        total_count = 10 * 1000 * 1000
        ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

        for record_size in [10, 20, 40, 100, 200, 400, 1000, 2000]:
          req_per_query = 16
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          read_ratio = 0.95
          zipf_theta = 0.00
          ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
          yield dict(ycsb)

          if alg in ['MICA', 'MICA+INDEX']:
            ycsb.update({ 'no_inlining': 1 })
            yield dict(ycsb)
            del ycsb['no_inlining']

  tag = 'singlekey'
  # for alg in all_algs:
  # others disabled because Silo/TicToc makes too much skewed throughput across threads
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in []:
    for thread_count in [1, 2] + list(range(4, max_thread_count + 1, 4)):
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 1
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      record_size = 16
      req_per_query = 1
      if thread_count <= 1:
        tx_count = 8000000
      elif thread_count <= 4:
        tx_count = 4000000
      else:
        tx_count = 2000000

      ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

      read_ratio = 0.00
      zipf_theta = 0.00
      ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      yield dict(ycsb)

  def _common_exps(common):
    if common['tag'] in ('backoff', 'factor', 'native-factor'):
      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      # record_size = 1000
      record_size = 100
      req_per_query = 16
      tx_count = 200000
      ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

      # if common['tag'] == 'backoff':
      #   # for read_ratio in [0.50, 0.95]:
      #   # for zipf_theta in [0.00, 0.99]:
      #   read_ratio = 0.50
      #   zipf_theta = 0.90
      #   ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      #   yield dict(ycsb)

      read_ratio = 0.50
      zipf_theta = 0.99
      ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
      yield dict(ycsb)

      if common['tag'] == 'backoff':
        # record_size = 1000
        record_size = 100
        req_per_query = 1
        tx_count = 2000000
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        # for read_ratio in [0.50, 0.95]:
        # for zipf_theta in [0.00, 0.99]:
        read_ratio = 0.50
        zipf_theta = 0.99
        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

      if common['tag'] in ('factor', 'native-factor'):
        # record_size = 1000
        record_size = 100
        req_per_query = 1
        tx_count = 2000000
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        # for read_ratio in [0.50, 0.95]:
        # for zipf_theta in [0.00, 0.99]:
        # read_ratio = 0.95
        # zipf_theta = 0.00
        # ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        # yield dict(ycsb)

        read_ratio = 0.95
        zipf_theta = 0.99
        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

    if common['tag'] in ('gc', 'backoff', 'factor'):
      # TPCC
      tpcc = dict(common)
      tx_count = 200000
      tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

      warehouse_count = 1
      tpcc.update({ 'warehouse_count': warehouse_count })
      yield dict(tpcc)

      warehouse_count = 4
      tpcc.update({ 'warehouse_count': warehouse_count })
      yield dict(tpcc)

      # if common['tag'] == 'gc':
      #   warehouse_count = 1
      #   tpcc.update({ 'warehouse_count': warehouse_count })
      #   yield dict(tpcc)

      # if common['tag'] in ('gc', 'factor'):
      if common['tag'] in ('gc',):
        warehouse_count = max_thread_count
        tpcc.update({ 'warehouse_count': warehouse_count })
        yield dict(tpcc)

      # TPCC-FULL
      tpcc = dict(common)
      tx_count = 200000
      tpcc.update({ 'bench': 'TPCC-FULL', 'tx_count': tx_count })

      warehouse_count = 1
      tpcc.update({ 'warehouse_count': warehouse_count })
      yield dict(tpcc)

      warehouse_count = 4
      tpcc.update({ 'warehouse_count': warehouse_count })
      yield dict(tpcc)

      # if common['tag'] == 'gc':
      #   warehouse_count = 1
      #   tpcc.update({ 'warehouse_count': warehouse_count })
      #   yield dict(tpcc)

      # if common['tag'] in ('gc', 'factor'):
      if common['tag'] in ('gc',):
        warehouse_count = max_thread_count
        tpcc.update({ 'warehouse_count': warehouse_count })
        yield dict(tpcc)


  tag = 'backoff'
  # for alg in ['MICA', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX', 'SILO', 'TICTOC']:
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in ['MICA+INDEX']:
    thread_count = max_thread_count
    #for backoff in [round(1.25 ** v - 1.0, 2) for v in range(24)]:
    for backoff in [round(1.25 ** v - 1.0, 2) for v in range(16)]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'fixed_backoff': backoff }

      for exp in _common_exps(common): yield exp


  for tag in factors:
    # for alg in ['MICA', 'MICA+INDEX']:
    for alg in ['MICA+INDEX']:
      thread_count = max_thread_count
      for i in range(7):
        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        # if i >= 1: common['no_wsort'] = 1
        # if i >= 2: common['no_preval'] = 1
        # if i >= 3: common['no_newest'] = 1
        # if i >= 4: common['no_wait'] = 1
        # if i >= 5: common['no_tscboost'] = 1
        # if i >= 6: common['no_tsc'] = 1
        #
        # for exp in _common_exps(common): yield exp

        common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

        if i == 1: common['no_wsort'] = 1
        if i == 2: common['no_preval'] = 1
        if i == 3: common['no_newest'] = 1
        if i == 4: common['no_wait'] = 1
        if i == 5: common['no_tscboost'] = 1
        if i == 6: common['no_tsc'] = 1

        for exp in _common_exps(common): yield exp


  tag = 'gc'
  # for alg in ['MICA', 'MICA+INDEX']:
  for alg in ['MICA+INDEX']:
    thread_count = max_thread_count
    for slow_gc in [1, 2, 4,
                    10, 20, 40,
                    100, 200, 400,
                    1000, 2000, 4000,
                    10000, 20000, 40000,
                    100000]:

      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count, 'slow_gc': slow_gc }

      for exp in _common_exps(common): yield exp


  tag = 'native-scan'
  for alg in ['MICA+INDEX']:
    for thread_count in [max_thread_count]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      for max_scan_len in [100]:
        for record_size in [10, 100, 1000]:
          req_per_query = 1
          tx_count = 200000
          ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

          ycsb.update({ 'max_scan_len': max_scan_len })
          #  if record_size in [10, 100]:
          #    ycsb.update({ 'column_count': 1 })

          read_ratio = 0.95
          zipf_theta = 0.99

          ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
          yield dict(ycsb)

          ycsb.update({ 'no_inlining': 1 })
          yield dict(ycsb)
          del ycsb['no_inlining']

          #  if record_size in [10, 100]:
          #    del ycsb['column_count']


  tag = 'native-full-table-scan'
  for alg in ['MICA+INDEX']:
    for thread_count in [max_thread_count]:
      common = { 'seq': seq, 'tag': tag, 'alg': alg, 'thread_count': thread_count }

      # YCSB
      ycsb = dict(common)
      total_count = 10 * 1000 * 1000
      ycsb.update({ 'bench': 'YCSB', 'total_count': total_count })

      for record_size in [10, 100, 1000]:
        req_per_query = 1
        tx_count = 20
        ycsb.update({ 'record_size': record_size, 'req_per_query': req_per_query, 'tx_count': tx_count })

        ycsb.update({ 'full_table_scan': 1 })
        #  if record_size in [10, 100]:
        #    ycsb.update({ 'column_count': 1 })

        read_ratio = 0.95
        zipf_theta = 0.99

        ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta })
        yield dict(ycsb)

        ycsb.update({ 'no_inlining': 1 })
        yield dict(ycsb)
        del ycsb['no_inlining']

        #  if record_size in [10, 100]:
        #    del ycsb['column_count']


def update_conf(conf, exp):
  conf = set_alg(conf, **exp)
  conf = set_threads(conf, **exp)
  if exp['bench'] == 'YCSB':
    conf = set_ycsb(conf, **exp)
  elif exp['bench'] in ('TPCC', 'TPCC-FULL'):
    conf = set_tpcc(conf, **exp)
  elif exp['bench'] == 'TATP':
    conf = set_tatp(conf, **exp)
  else: assert False
  if exp['alg'].startswith('MICA') or exp['tag'] == 'backoff':
    conf = set_mica_confs(conf, **exp)
  return conf


def sort_exps(exps):
  def _exp_pri(exp):
    pri = 0

    # prefer microbench
    if exp['tag'] == 'gc': pri -= 2
    if exp['tag'] == 'backoff': pri -= 2
    # then macrobench
    if exp['tag'] == 'macrobench': pri -= 1
    # factor analysis is not prioritized

    # prefer fast schemes
    if exp['alg'] == 'MICA': pri -= 2
    if exp['alg'] == 'MICA+INDEX': pri -= 2
    # if exp['alg'].startswith('MICA'): pri -= 2
    if exp['alg'] == 'SILO' or exp['alg'] == 'TICTOC': pri -= 1

    # prefer max cores
    if exp['thread_count'] in (max_thread_count, max_thread_count * 2): pri -= 1

    # prefer write-intensive workloads
    if exp['bench'] == 'YCSB' and exp['read_ratio'] == 0.50: pri -= 1
    # prefer standard skew
    if exp['bench'] == 'YCSB' and exp['zipf_theta'] in (0.00, 0.90, 0.99): pri -= 1

    # prefer (warehouse count) = (thread count)
    if exp['bench'].startswith('TPCC') and exp['thread_count'] == exp['warehouse_count']: pri -= 1
    # prefer standard warehouse counts
    if exp['bench'].startswith('TPCC') and exp['warehouse_count'] in (1, 4, 16, max_thread_count, max_thread_count * 2): pri -= 1

    # run exps in their sequence number
    return (exp['seq'], pri)

  exps = list(exps)
  exps.sort(key=_exp_pri)
  return exps


def unique_exps(exps):
  l = []
  for exp in exps:
    if exp in l: continue
    assert exp['alg'] in hugepage_count
    l.append(exp)
  return l

def skip_done(exps):
  for exp in exps:
    if os.path.exists(dir_name + '/' + gen_filename(exp)): continue
    if os.path.exists(dir_name + '/' + gen_filename(exp) + '.failed-0'): continue
    # if exp['alg'] == 'MICA': continue
    yield exp

def find_exps_to_run(exps, pats):
  for exp in exps:
    if pats:
      for pat in pats:
        key, _, value = pat.partition('@')
        if key not in exp or str(exp[key]) != value:
          break
      else:
        yield exp
    else:
      yield exp


def validate_result(exp, output):
  if exp['alg'].find('-REF') != -1:
    if exp['alg'].startswith('SILO') or exp['alg'].startswith('ERMIA'):
      return output.find('txn breakdown: ') != -1
    elif exp['alg'].startswith('FOEDUS'):
      return output.find('final result:') != -1 and \
             output.find('total pages, 0 free pages') == -1
    else:
      assert False
  elif not exp['tag'].startswith('native-'):
    return output.find('[summary] tput=') != -1
  else:
    return output.find('cleaning up') != -1

def killall():
  # DBx1000
  os.system('sudo killall rundb 2> /dev/null')
  # native MICA
  os.system('sudo killall test_tx 2> /dev/null')
  # SILO-REF
  os.system('sudo killall dbtest 2> /dev/null')
  # ERMIA-REF
  os.system('sudo killall dbtest-SI 2> /dev/null')
  os.system('sudo killall dbtest-SSI 2> /dev/null')
  os.system('sudo killall dbtest-SI_SSN 2> /dev/null')
  # FOEDUS-REF
  os.system('sudo killall tpcc 2> /dev/null')

def make_silo_cmd(exp):
  cmd = 'silo/out-perf.masstree/benchmarks/dbtest'
  cmd += ' --verbose'
  cmd += ' --parallel-loading'
  cmd += ' --pin-cpus'
  cmd += ' --retry-aborted-transactions'
  if exp['alg'].find('-BACKOFF') != -1:
    cmd += ' --backoff-aborted-transactions'  # Better for 1 warehouse (> 1000 Tps), worse for 4+ warehouses for TPC-C
  cmd += ' --num-threads %d' % exp['thread_count']
  # cmd += ' --ops-per-worker %d' % exp['tx_count']
  cmd += ' --numa-memory %dG' % int(int(hugepage_count[exp['alg']] * 0.99) * 2 / 1024)

  if exp['bench'] == 'TPCC-FULL':
    cmd += ' --runtime 20'  # Can run for 30 seconds but for consistency
    cmd += ' --bench tpcc'
    cmd += ' --scale-factor %d' % exp['warehouse_count']
    cmd += ' --bench-opts="--enable-separate-tree-per-partition"'

  elif exp['bench'] == 'YCSB':
    assert exp['record_size'] == 100

    cmd += ' --max-runtime 20'  # Same as above
    cmd += ' --ops-per-worker %d' % exp['tx_count']
    cmd += ' --bench ycsb'
    cmd += ' --bench-opts='
    cmd += '"--workload=0,0,100,0'
    cmd += ' --initial-table-size=%d' % exp['total_count']
    cmd += ' --zipf-theta=%f' % exp['zipf_theta']
    cmd += ' --reps-per-tx=%d' % exp['req_per_query']
    cmd += ' --rmw-additional-reads=0'
    cmd += ' --rmw-read-ratio=%f' % exp['read_ratio']
    cmd += '"'

  else: assert False

  return cmd

def make_ermia_cmd(exp):
  tmpfs_dir = '/tmpfs'
  log_dir = '/tmpfs/ermia-log'

  if not os.path.exists(tmpfs_dir):
    os.system('sudo ln -s /run/shm /tmpfs')
  if os.path.exists(log_dir):
    shutil.rmtree(log_dir)
  os.mkdir(log_dir)

   # --parallel-loading seems to be broken
   # --ops-per-worker is somehow very slow

  if exp['alg'].find('-SI_SSN') != -1:
    cmd = 'ermia/dbtest-SI_SSN'
  elif exp['alg'].find('-SSI') != -1:
    cmd = 'ermia/dbtest-SSI'
  elif exp['alg'].find('-SI') != -1:
    cmd = 'ermia/dbtest-SI'
  else: assert False
  cmd += ' --verbose'
  # cmd += ' --parallel-loading'  # Broken
  cmd += ' --retry-aborted-transactions'
  if exp['alg'].find('-BACKOFF') != -1:
    # Broken; this creates zero throughput when warehouse count < thread count
    # cmd += ' --backoff-aborted-transactions'
    pass
    assert False
  # cmd += ' --ops-per-worker %d' % exp['tx_count']
  cmd += ' --node-memory-gb %d' % int(hugepage_count[exp['alg']] * 2 / 1024 / node_count * 0.99)
  # cmd += ' --enable-gc' # throughput decreases substantially (down to 20-50%) if the experiment runs more than 10 seconds
  cmd += ' --tmpfs-dir %s' % tmpfs_dir
  cmd += ' --log-dir %s' % log_dir
  cmd += ' --log-buffer-mb 512'
  cmd += ' --log-segment-mb 8192'
  cmd += ' --null-log-device'
  cmd += ' --num-threads %d' % exp['thread_count']

  if exp['bench'] == 'TPCC-FULL':
    cmd += ' --runtime 15'  # ERMIA requires more memory than Silo, so it is unreliable to run it for 30 seconds
    cmd += ' --bench tpcc'
    cmd += ' --scale-factor %d' % exp['warehouse_count']
    # safe snapshots seem to decrease tput a lot even though R/O transactions
    # are not aborted
    #if exp['alg'].find('SSN') != -1 or exp['alg'].find('SSI') != -1:
    #  cmd += ' --safesnap'
    if exp['warehouse_count'] > exp['thread_count']:
      # cmd += ' --bench-opts="--enable-separate-tree-per-partition --warehouse-spread=100"'  # Causes zero throughput
      cmd += ' --bench-opts="--warehouse-spread=100"'
    else:
      # cmd += ' --bench-opts="--enable-separate-tree-per-partition"' # Disabled for consistency (also not supported by devs)
      pass

  elif exp['bench'] == 'YCSB':
    assert exp['record_size'] == 100

    cmd += ' --max-runtime 20'  # Same as Silo; YCSB uses less memory
    cmd += ' --ops-per-worker %d' % exp['tx_count']
    cmd += ' --bench ycsb'
    cmd += ' --bench-opts='
    cmd += '"--workload=F'
    cmd += ' --initial-table-size=%d' % exp['total_count']
    cmd += ' --zipf-theta=%f' % exp['zipf_theta']
    cmd += ' --reps-per-tx=%d' % exp['req_per_query']
    cmd += ' --rmw-additional-reads=0'
    cmd += ' --rmw-read-ratio=%f' % exp['read_ratio']
    cmd += '"'

  else: assert False

  return cmd

def make_foedus_cmd(exp):
  assert exp['thread_count'] >= 4

  os.system('sudo sysctl -q -w kernel.shmmax=9223372036854775807')
  os.system('sudo sysctl -q -w kernel.shmall=1152921504606846720')
  os.system('sudo sysctl -q -w kernel.shmmni=409600')
  os.system('sudo sysctl -q -w vm.max_map_count=2147483647')
  os.system('sudo sh -c "grep \"^hugeshm\" /etc/group|cut -d: -f3| cat > /proc/sys/vm/hugetlb_shm_group"')

  os.system('rm -rf /dev/shm/foedus_tpcc/')
  os.system('rm -rf /tmp/libfoedus.*')

  # based on
  #  foedus_code/build/experiments-core/src/foedus/tpcc/run_common.sh
  #   .../run_dl580.sh
  #  foedus_code/build/experiments-core/src/foedus/ycsb/run_common.sh
  #   .../run_dl580.sh

  # see tpcc_driver.cpp, ycsb_driver.cpp for options

  cmd = 'env CPUPROFILE_FREQUENCY=1'
  if exp['bench'] == 'TPCC-FULL':
     cmd += ' foedus_code/build/experiments-core/src/foedus/tpcc/tpcc'
  elif exp['bench'] == 'YCSB':
     cmd += ' foedus_code/build/experiments-core/src/foedus/ycsb/ycsb_hash'
  else: assert False
  cmd += ' -fork_workers=false' # forking seems to ignore thread count limit
  cmd += ' -nvm_folder=/dev/shm'
  cmd += ' -volatile_pool_size=20'  # this determines overall memory use
  cmd += ' -snapshot_pool_size=2'
  cmd += ' -reducer_buffer_size=1'
  cmd += ' -loggers_per_node=2'
  if exp['thread_count'] == 1:
    cmd += ' -thread_per_node=1'
    cmd += ' -numa_nodes=1'
  else:
    cmd += ' -thread_per_node=%d' % (exp['thread_count'] // node_count)
    cmd += ' -numa_nodes=%d' % node_count
  cmd += ' -log_buffer_mb=1024'
  cmd += ' -null_log_device=true' # no logging
  cmd += ' -high_priority=false'
  cmd += ' -duration_micro=%d' % (20 * 1000000) # FOEDUS requires a ton of memory, so we cannot run it for 30 seconds

  if exp['bench'] == 'TPCC-FULL':
    cmd += ' -take_snapshot=false'
    cmd += ' -warehouses=%d' % exp['warehouse_count']
    cmd += ' -neworder_remote_percent=1'
    cmd += ' -payment_remote_percent=15'

    if exp['alg'] == 'FOEDUS-MOCC-REF':
      cmd += ' -hcc_policy=0' # MOCC
    elif exp['alg'] == 'FOEDUS-OCC-REF':
      cmd += ' -hcc_policy=1' # OCC
    else: assert False

  elif exp['bench'] == 'YCSB':
    assert exp['record_size'] == 100

    cmd += ' -ops_per_worker=%d' % exp['tx_count']

    # porting settings from tpcc_driver.cpp
    # cmd += ' -take_snapshot=false'  # XXX: Unable to turn this off?
    cmd += ' -workload=F' # The only workload that supports Zipf
    cmd += ' -read_all_fields=true'
    cmd += ' -write_all_fields=true'
    cmd += ' -initial_table_size=%d' % exp['total_count']
    cmd += ' -simple_int_keys=true'
    cmd += ' -zipfian_theta=%f' % exp['zipf_theta']
    # cmd += ' -rmw_additional_reads=%d' % round(exp['read_ratio'] * exp['req_per_query'])
    # cmd += ' -reps_per_tx=%d' % (exp['req_per_query'] - round(exp['read_ratio'] * exp['req_per_query']))
    cmd += ' -rmw_additional_reads=0'
    cmd += ' -reps_per_tx=%d' % exp['req_per_query']
    cmd += ' -rmw_read_ratio=%f' % exp['read_ratio']
    cmd += ' -sort_keys=false'
    cmd += ' -distinct_keys=true'

    if exp['alg'] == 'FOEDUS-MOCC-REF': # MOCC
      cmd += ' -hot_threshold=10'
      cmd += ' -enable_retrospective_lock_list=true'
      cmd += ' -force_canonical_xlocks_in_precommit=true'
    elif exp['alg'] == 'FOEDUS-OCC-REF': # OCC
      cmd += ' -hot_threshold=256'
      cmd += ' -enable_retrospective_lock_list=false'
      cmd += ' -force_canonical_xlocks_in_precommit=true'
    else: assert False

  cmd += ' 2>&1'
  #cmd += ' | grep -m1 "Experiment ended"' # to force stop runs with MOCC
  return cmd


hugepage_status = None

def run(exp, prepare_only):
  global hugepage_status

  # update config
  if not exp['tag'].startswith('native-'):
    conf = open('config-std.h').read()
    conf = update_conf(conf, exp)
    open('config.h', 'w').write(conf)

    shutil.copy('mica/src/mica/test/test_tx_conf_org.h',
                'mica/src/mica/test/test_tx_conf.h')
  else:
    shutil.copy('config-std.h', 'config.h')

    conf = open('mica/src/mica/test/test_tx_conf_org.h').read()
    conf = update_conf(conf, exp)
    open('mica/src/mica/test/test_tx_conf.h', 'w').write(conf)

  # clean up
  os.system('make clean -j > /dev/null')
  os.system('rm -f ./rundb')

  if hugepage_status != (hugepage_count[exp['alg']], exp['alg']):
    os.system('mica/script/setup.sh %d %d > /dev/null' % \
      (hugepage_count[exp['alg']] / 2, hugepage_count[exp['alg']] / 2))
    hugepage_status = (hugepage_count[exp['alg']], exp['alg'])
  # else:
  #   if hugepage_status != (0, ''):
  #     os.system('mica/script/setup.sh 0 0 > /dev/null')
  #     hugepage_status = (0, '')

  # os.system('sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"')
  os.system('sudo bash -c "echo always > /sys/kernel/mm/transparent_hugepage/enabled"')
  os.system('sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/defrag"')

  # cmd
  filename = dir_name + '/' + gen_filename(exp)

  if exp['alg'].find('-REF') != -1:
    if exp['alg'].startswith('SILO'):
      cmd = make_silo_cmd(exp)
    elif exp['alg'].startswith('ERMIA'):
      cmd = make_ermia_cmd(exp)
    elif exp['alg'].startswith('FOEDUS'):
      cmd = make_foedus_cmd(exp)
    else:
      assert False
  elif not exp['tag'].startswith('native-'):
    # cmd = 'sudo ./rundb | tee %s' % (filename + '.tmp')
    cmd = 'sudo ./rundb'
  else:
    cmd = 'sudo mica/build/test_tx 0 0 0 0 0 0'

  print('  ' + cmd)


  if prepare_only: return

  # compile
  if exp['alg'].find('-REF') != -1:
    ret = 0
  elif not exp['tag'].startswith('native-'):
    ret = os.system('make -j > /dev/null')
  else:
    pdir = os.getcwd()
    os.chdir('mica/build')
    ret = os.system('make -j > /dev/null')
    os.chdir(pdir)
  assert ret == 0, 'failed to compile for %s' % exp
  os.system('date')
  os.system('sudo sync')
  os.system('sudo sync')
  time.sleep(1)

  # run
  for trial in range(max_retries):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
      stdout, stderr = p.communicate(timeout=120)
      killed = False
    except subprocess.TimeoutExpired:
      killall()
      stdout, stderr = p.communicate(timeout=10)
      killed = True

    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    output = stdout + '\n\n' + stderr

    failed = False
    if p.returncode != 0 or killed:
      error_s = 'failed to run exp for %s (status=%s, killed=%s)' % (format_exp(exp), p.returncode, killed)
      failed = True
    elif not validate_result(exp, output):
      error_s = 'validation failed for %s' % format_exp(exp)
      failed = True

    if not failed:
      # compact the log
      if exp['alg'].startswith('FOEDUS'):
        new_output = ''
        for line in output.splitlines(True):
          if line.find('<DirectIoFile>') != -1 or line.find('O_DIRECT') != -1 or line.find('<AlignedMemory>') != -1:
            continue
          new_output += line
        output = new_output
      # finalize
      open(filename, 'w').write(output)
      break

    print(COLOR_RED + error_s + COLOR_RESET)

    failed_filename = filename + '.failed-' + str(trial)
    open(failed_filename, 'w').write(output)

def run_all(pats, prepare_only):
  exps = []
  for seq in range(total_seqs):
    exps += list(enum_exps(seq))
  exps = list(unique_exps(exps))

  total_count = len(exps)
  print('total %d exps' % total_count)

  count_per_tag = {}
  for exp in exps:
    count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
  for tag in sorted(count_per_tag.keys()):
    print('  %s: %d' % (tag, count_per_tag[tag]))
  print('')

  if not prepare_only:
    exps = list(skip_done(exps))
  exps = list(find_exps_to_run(exps, pats))
  skip_count = total_count - len(exps)
  print('%d exps skipped' % skip_count)
  print('')

  exps = list(sort_exps(exps))
  print('total %d exps to run' % len(exps))

  count_per_tag = {}
  for exp in exps:
    count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
  for tag in sorted(count_per_tag.keys()):
    print('  %s: %d' % (tag, count_per_tag[tag]))
  print('')

  first = time.time()

  for i, exp in enumerate(exps):
    start = time.time()
    failed = len(glob.glob(os.path.join(dir_name, '*.failed')))
    s = 'exp %d/%d (%d failed): %s' % (i + 1, len(exps), failed, format_exp(exp))
    print(COLOR_BLUE + s + COLOR_RESET)

    run(exp, prepare_only)
    if prepare_only: break

    now = time.time()
    print('elapsed = %.2f seconds' % (now - start))
    print('remaining = %.2f hours' % ((now - first) / (i + 1) * (len(exps) - i - 1) / 3600))
    print('')


def update_filenames():
  for filename in os.listdir(dir_name):
    if not filename.startswith(prefix): continue
    if not filename.endswith(suffix): continue
    exp = parse_filename(filename)
    exp['tag'] = 'macrobench'
    new_filename = gen_filename(exp)
    print(filename, ' => ', new_filename)
    os.rename(dir_name + '/' + filename, dir_name + '/' + new_filename)
  sys.exit(0)


def detect_core_count():
  global node_count
  global max_thread_count

  # simple core count detection
  core_id = None
  node_id = None
  max_core_id = -1
  max_node_id = -1

  for line in open('/proc/cpuinfo').readlines():
    line = line.strip()
    if line.startswith('processor'):
      core_id = int(line.partition(':')[2])
    elif line.startswith('physical id'):
      node_id = int(line.partition(':')[2])
    elif line == '':
      max_core_id = max(max_core_id, core_id)
      max_node_id = max(max_node_id, node_id)
      core_id = None
      node_id = None

  node_count = max_node_id + 1
  max_thread_count = max_core_id + 1
  max_thread_count = int(max_thread_count / 2)   # hyperthreading
  # print('total %d nodes, %d cores' % (node_count, max_thread_count))


if __name__ == '__main__':
  argv = list(sys.argv)
  if len(argv) < 3:
    print('%s dir_name [RUN | RUN patterns | PREPARE patterns]' % argv[0])
    sys.exit(1)

  detect_core_count()

  dir_name = argv[1]
  old_dir_name = 'old_' + dir_name

  argv = argv[2:]

  if not os.path.exists(dir_name):
    os.mkdir(dir_name)

  killall()

  remove_stale()
  # update_filenames()

  if argv[0].upper() == 'RUN':
    if len(argv) == 1:
      run_all(None, False)
    else:
      run_all(argv[1].split('__'), False)
  elif argv[0].upper() == 'PREPARE':
    run_all(argv[1].split('__'), True)
  else:
    assert False
