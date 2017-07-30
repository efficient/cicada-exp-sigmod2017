from __future__ import print_function
import pprint
import shutil
from common import *

# DEBUG = True
DEBUG = False

pp = pprint.PrettyPrinter()


def query(sql, locals_):
  sql = sql.format(**locals_)

  d = dict(globals())
  d.update(locals_)

  results = sqldf(sql, d)
  # print(results)
  return results


def init():
  global exp_data
  exp_data = load_data('exp_data')

  max_thread_count = detect_max_thread_count(exp_data)
  set_common_globals(max_thread_count)

  exp_data = query('''
    SELECT *,
           1. * (mica_pending_iv + mica_committed_iv + mica_aborted_iv +
           mica_unused_iv + mica_pending_niv + mica_committed_niv +
           mica_aborted_niv) / mica_net_row_count - 1. AS space_overhead
    FROM exp_data
    ''', locals())
   # 1. * aborted_count / (committed_count + aborted_count) AS abort_rate,
   # (100. - mica_committed_time_perc) / 100. AS abort_time,

  pd.set_option('display.width', shutil.get_terminal_size((80, 30)).columns)
  pd.set_option('display.max_columns', 1000)


def check_count(df):
  expected_total_seqs = 5

  if (df['COUNT(seq)'] < expected_total_seqs).any():
    print('')
    print('  Warning: incomplete data')
    # for line in str(df).split('\n'):
    #   print('    ' + line)
  elif (df['COUNT(seq)'] > expected_total_seqs).any():
    print('')
    print('  Warning: ambiguous selector')
    for line in str(df).split('\n'):
      print('    ' + line)


def perc_diff(v):
  if v >= 1:
    return '{:.1f}% higher'.format(v * 100. - 100.)
  else:
    return '{:.1f}% lower'.format(100. - v * 100.)


def dict_proj(d, keys):
  new_d = {}
  for key in keys:
    new_d[key] = d[key]
  return new_d


def filter(selectors, conds, group_by):
  selectors_s = ', '.join(['AVG(%s) as %s' % (field, field) for field in selectors])
  cond_s = ''
  for k, v in conds.items():
    if v is None:
      cond_s += ' AND %s IS NULL' % k
    else:
      cond_s += ' AND %s = %s' % (k, repr(v))
  cond_s = cond_s[5:]
  # cond_s = ' AND '.join(['%s=%s' % (k, repr(v)) for k, v in conds.items()])

  results = query('''
    SELECT {group_by}, {selectors_s}, COUNT(seq)
    FROM exp_data
    WHERE {cond_s}
    GROUP BY {group_by}
    '''.format(**locals()), locals())

  if group_by.find(',') == -1:
    results = results.set_index(group_by)

  check_count(results)

  if DEBUG:
    print(results)
  return results


def set_common_globals(max_thread_count):
  global conds_list

  conds_list = [
    ('tpcc-verycontended', {
      'bench': 'TPCC',
      'warehouse_count': 1,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-contended', {
      'bench': 'TPCC',
      'warehouse_count': 4,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-uncontended', {
      'bench': 'TPCC',
      'warehouse_count': max_thread_count,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    # ('tpcc-uncontended-1', {
    #   'bench': 'TPCC',
    #   'warehouse_count': 1,
    #   'thread_count': 1,
    #   'simple_index_update': None,
    # }),

    ('tpcc-full-verycontended', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 1,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-full-contended', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 4,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-full-uncontended', {
      'bench': 'TPCC-FULL',
      'warehouse_count': max_thread_count,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    # ('tpcc-full-uncontended-1', {
    #   'bench': 'TPCC-FULL',
    #   'warehouse_count': 1,
    #   'thread_count': 1,
    #   'simple_index_update': None,
    # }),

    ('tpcc-full-verycontended-siu', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 1,
      'thread_count': max_thread_count,
      'simple_index_update': 1,
    }),
    ('tpcc-full-contended-siu', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 4,
      'thread_count': max_thread_count,
      'simple_index_update': 1,
    }),
    ('tpcc-full-uncontended-siu', {
      'bench': 'TPCC-FULL',
      'warehouse_count': max_thread_count,
      'thread_count': max_thread_count,
      'simple_index_update': 1,
    }),
    # ('tpcc-full-uncontended-1-siu', {
    #   'bench': 'TPCC-FULL',
    #   'warehouse_count': 1,
    #   'thread_count': 1,
    #   'simple_index_update': 1,
    # }),

    ('ycsb-contended-tx', {
      'bench': 'YCSB',
      'req_per_query': 16,
      'read_ratio': 0.5,
      'zipf_theta': 0.99,
      'thread_count': max_thread_count,
    }),
    ('ycsb-uncontended-tx', {
      'bench': 'YCSB',
      'req_per_query': 16,
      'read_ratio': 0.95,
      'zipf_theta': 0.0,
      'thread_count': max_thread_count,
    }),

    ('ycsb-contended-kv', {
      'bench': 'YCSB',
      'req_per_query': 1,
      'read_ratio': 0.50,
      'zipf_theta': 0.99,
      'thread_count': max_thread_count,
    }),
    ('ycsb-uncontended-skewed-kv', {
      'bench': 'YCSB',
      'req_per_query': 1,
      'read_ratio': 0.95,
      'zipf_theta': 0.99,
      'thread_count': max_thread_count,
    }),
    ('ycsb-uncontended-uniform-kv', {
      'bench': 'YCSB',
      'req_per_query': 1,
      'read_ratio': 0.95,
      'zipf_theta': 0.0,
      'thread_count': max_thread_count,
    }),

    ## scalability
    #('ycsb-uncontended-uniform-kv-scalability', {
    #  'bench': 'YCSB',
    #  'req_per_query': 1,
    #  'read_ratio': 0.95,
    #  'zipf_theta': 0.0,
    #}),
    ('ycsb-uncontended-skewed-kv-scalability', {
      'bench': 'YCSB',
      'req_per_query': 1,
      'read_ratio': 0.95,
      'zipf_theta': 0.99,
    }),

    ## overhead
    # TPC-C low contention
    ('tpcc-full-uncontended-overhead', {
      'bench': 'TPCC-FULL',
      'simple_index_update': None,
    }),
    # TPC-C low contention (no phantom avoidance)
    ('tpcc-full-uncontended-siu-overhead', {
      'bench': 'TPCC-FULL',
      'simple_index_update': 1,
    }),
    # TPC-C-NP low contention
    ('tpcc-uncontended-overhead', {
      'bench': 'TPCC',
      'simple_index_update': None,
    }),

    # YCSB low contention
    ('ycsb-uncontended-tx-overhead', {
      'bench': 'YCSB',
      'req_per_query': 16,
      'read_ratio': 0.95,
      'zipf_theta': 0.0,
    }),

    ## contention
    # TPC-C high contention
    ('tpcc-full-verycontended-contention', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 1,
      'simple_index_update': None,
    }),
    # TPC-C high contention (no phantom avoidance)
    ('tpcc-full-verycontended-siu-contention', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 1,
      'simple_index_update': 1,
    }),
    # TPC-C-NP high contention
    ('tpcc-verycontended-contention', {
      'bench': 'TPCC',
      'warehouse_count': 1,
      'simple_index_update': None,
    }),

    # YCSB high contention
    ('ycsb-contended-tx-contention', {
      'bench': 'YCSB',
      'req_per_query': 16,
      'read_ratio': 0.5,
      'zipf_theta': 0.99,
    }),

    ## backoff
    ('tpcc-verycontended-backoff', {
      'bench': 'TPCC',
      'warehouse_count': 1,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-contended-backoff', {
      'bench': 'TPCC',
      'warehouse_count': 4,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-full-verycontended-backoff', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 1,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('tpcc-full-contended-backoff', {
      'bench': 'TPCC-FULL',
      'warehouse_count': 4,
      'thread_count': max_thread_count,
      'simple_index_update': None,
    }),
    ('ycsb-contended-skewed-kv-backoff', {
      'bench': 'YCSB',
      'req_per_query': 1,
      'read_ratio': 0.5,
      'zipf_theta': 0.99,
      'thread_count': max_thread_count,
    }),
    ('ycsb-contended-skewed-backoff', {
      'bench': 'YCSB',
      'req_per_query': 16,
      'read_ratio': 0.5,
      'zipf_theta': 0.99,
      'thread_count': max_thread_count,
    }),

  ]

def find_cond(label):
  for item in conds_list:
    if item[0] == label: return item[1]

def all_cond_labels():
  for item in conds_list:
    yield item[0]

def macrobench():
  tag = 'macrobench'
  selectors = ['tput', 'abort_rate', 'latency_avg', 'latency_99', 'space_overhead']
  # 'abort_time',

  for label in all_cond_labels():
    conds = dict(find_cond(label))
    conds['tag'] = tag

    results = filter(selectors, conds, 'alg')

    if True or label in ['tpcc-uncontended', 'tpcc-contended', 'tpcc-full-uncontended', 'tpcc-full-contended', 'ycsb-uncontended-uniform-kv']:
      print('For %s:' % label)
      print(results)

    mica_tput = results.at['MICA+INDEX', 'tput']
    mica_abort_rate = results.at['MICA+INDEX', 'abort_rate']
    mica_latency_avg = results.at['MICA+INDEX', 'latency_avg']
    mica_latency_99 = results.at['MICA+INDEX', 'latency_99']
    mica_overhead = results.at['MICA+INDEX', 'space_overhead']

    max_other_tput = query('''
      SELECT MAX(tput) FROM results WHERE alg!="MICA+INDEX" and alg!="MICA"
      ''', locals()).iat[0, 0]
    min_other_latency_avg = query('''
      SELECT MIN(latency_avg) FROM results WHERE alg!="MICA+INDEX" and alg!="MICA"
      ''', locals()).iat[0, 0]
    min_other_latency_99 = query('''
      SELECT MIN(latency_99) FROM results WHERE alg!="MICA+INDEX" and alg!="MICA"
      ''', locals()).iat[0, 0]

    print('tput-%s (%s): ' % (label, pp.pformat(conds).replace('\n', '')), end='')
    print('{:.2f} M tps, '.format(mica_tput / 1000000.), end='')
    print('{:.2f}% abort; '.format(mica_abort_rate), end='')
    print(perc_diff(mica_tput / max_other_tput))

    print('latency-%s (%s): ' % (label, pp.pformat(conds).replace('\n', '')), end='')
    print('{:.2f} avg, {:.2f} 99-th; '.format(mica_latency_avg, mica_latency_99), end='')
    print(perc_diff(mica_latency_avg / min_other_latency_avg) + ', ', end='')
    print(perc_diff(mica_latency_99 / min_other_latency_99))

    print('overhead-%s (%s): ' % (label, pp.pformat(conds).replace('\n', '')), end='')
    print('{:.2f}%'.format(mica_overhead * 100.))

def gc():
  tag = 'gc'
  selectors = ['tput', 'space_overhead']

  for label in ['tpcc-verycontended', 'tpcc-contended', 'tpcc-uncontended', 'tpcc-full-verycontended', 'tpcc-full-contended', 'tpcc-full-uncontended']:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = 'MICA+INDEX'

    results = filter(selectors, conds, 'slow_gc')

    slow_gc = query('''
          SELECT tput, space_overhead FROM results WHERE slow_gc=100000
          ''', locals())
    fast_gc = query('''
          SELECT tput, space_overhead FROM results WHERE slow_gc=10
          ''', locals())

    slow_gc_tput = slow_gc.iat[0, 0]
    fast_gc_tput = fast_gc.iat[0, 0]

    slow_gc_overhead = slow_gc.iat[0, 1]
    fast_gc_overhead = fast_gc.iat[0, 1]

    print('gc-tput-%s (%s): ' % (label, pp.pformat(conds).replace('\n', '')), end='')
    print(perc_diff(slow_gc_tput / fast_gc_tput))

    print('gc-overhead-%s (%s): ' % (label, pp.pformat(conds).replace('\n', '')), end='')
    print('slow_gc: {:.2f}% vs. fast_gc: {:.2f}%'.format(slow_gc_overhead * 100., fast_gc_overhead * 100.))

def factor():
  factor_names = ['no_tsc', 'no_tscboost', 'no_wait', 'no_newest', 'no_preval', 'no_wsort']

  tag = 'factor'
  selectors = ['tput', 'latency_avg', 'latency_99']

  for label in ['tpcc-verycontended', 'tpcc-contended', 'tpcc-full-verycontended', 'tpcc-full-contended', 'ycsb-contended-tx', 'ycsb-uncontended-skewed-kv']:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = 'MICA+INDEX'

    results = filter(selectors, conds, 'no_tsc, no_tscboost, no_wait, no_newest, no_preval, no_wsort')
    #print(results)

    full_conds = dict(conds)
    full_conds['tag'] = 'macrobench'
    results2 = filter(['tput', 'latency_avg', 'latency_99'], full_conds, 'alg')
    #print(results2)
    full_tput = results2.iat[0, 0]

    for i in range(len(factor_names)):
      factor_name = factor_names[i]
      other_factor_names = factor_names[:i] + factor_names[i + 1:]

      sep_conds = ['%s=1' % factor_name]
      sep_conds += ['%s IS NULL' % name for name in other_factor_names]
      sep_tput = query('''
        SELECT tput FROM results WHERE %s
        ''' % ' AND '.join(sep_conds), locals()).iat[0, 0]

      print('factor-%s (%s) [%s]: ' % (label, pp.pformat(conds).replace('\n', ''), factor_name), end='')
      print('{:.2f} M tps vs. '.format(sep_tput / 1000000.), end='')
      print('{:.2f} M tps (full); '.format(full_tput / 1000000.), end='')
      print(perc_diff(sep_tput / full_tput))


def scan():
  tag = 'native-scan'
  selectors = ['mica_scan_tput', 'tput']

  for label in ['ycsb-uncontended-skewed-kv']:
    #  for record_size in [10, 100, 1000]:
    for record_size in [100]:
      for no_inlining in [False, True]:
        conds = dict(find_cond(label))
        conds['tag'] = tag
        conds['alg'] = 'MICA+INDEX'
        conds['record_size'] = record_size

        results = filter(selectors, conds, 'no_inlining')
        # print(results)

        if no_inlining:
          scan_tput = query('''
            SELECT * FROM results WHERE no_inlining=1
            ''' , locals()).at[0, 'mica_scan_tput']
        else:
          scan_tput = query('''
            SELECT * FROM results WHERE no_inlining IS NULL
            ''' , locals()).at[0, 'mica_scan_tput']

        print('scan-%s (%s) [%s]: ' % (label, pp.pformat(conds).replace('\n', ''), 'no_inlining' if no_inlining else 'inlining'), end='')
        print('{:.2f} M scan tps\n'.format(scan_tput / 1000000.), end='')


def full_table_scan():
  tag = 'native-full-table-scan'
  selectors = ['mica_scan_tput', 'tput']

  for label in ['ycsb-uncontended-skewed-kv']:
    #  for record_size in [10, 100, 1000]:
    for record_size in [100]:
      for no_inlining in [False, True]:
        conds = dict(find_cond(label))
        conds['tag'] = tag
        conds['alg'] = 'MICA+INDEX'
        conds['record_size'] = record_size

        results = filter(selectors, conds, 'no_inlining')
        # print(results)

        if no_inlining:
          scan_tput = query('''
            SELECT * FROM results WHERE no_inlining=1
            ''' , locals()).at[0, 'mica_scan_tput']
        else:
          scan_tput = query('''
            SELECT * FROM results WHERE no_inlining IS NULL
            ''' , locals()).at[0, 'mica_scan_tput']

        print('full-table-scan-%s (%s) [%s]: ' % (label, pp.pformat(conds).replace('\n', ''), 'no_inlining' if no_inlining else 'inlining'), end='')
        print('{:.2f} M scan tps\n'.format(scan_tput / 1000000.), end='')

def scalability():
  tag = 'macrobench'
  selectors = ['tput']
  #label = 'ycsb-uncontended-uniform-kv-scalability'
  label = 'ycsb-uncontended-skewed-kv-scalability'

  algs = ['NO_WAIT', 'SILO-REF', 'SILO', 'TICTOC', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'HEKATON', 'ERMIA-SI_SSN-REF', 'MICA+INDEX']
  all_results = []
  for alg in algs:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = alg

    results = filter(selectors, conds, 'thread_count')
    check_count(results)
    all_results.append(results['tput'])

  d = pd.concat(all_results, axis=1)
  d.columns = algs

  print('scalability:')
  print(d)

def tpcc_overhead(label):
  tag = 'macrobench'
  selectors = ['tput']

  algs = ['NO_WAIT', 'SILO-REF', 'SILO', 'TICTOC', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'HEKATON', 'ERMIA-SI_SSN-REF', 'MICA+INDEX']
  all_results = []
  for alg in algs:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = alg

    results = filter(selectors, conds, 'thread_count, warehouse_count')
    results = results[results['thread_count'] == results['warehouse_count']]
    results.set_index('thread_count', inplace=True)
    check_count(results)
    all_results.append(results['tput'])

  d = pd.concat(all_results, axis=1)
  d.columns = algs

  print('tpcc-overhead [%s]:' % label)
  print(d)

def ycsb_overhead(label):
  tag = 'macrobench'
  selectors = ['tput']

  algs = ['NO_WAIT', 'SILO-REF', 'SILO', 'TICTOC', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'HEKATON', 'ERMIA-SI_SSN-REF', 'MICA+INDEX']
  all_results = []
  for alg in algs:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = alg

    results = filter(selectors, conds, 'thread_count')
    check_count(results)
    all_results.append(results['tput'])

  d = pd.concat(all_results, axis=1)
  d.columns = algs

  print('ycsb_overhead [%s]:' % label)
  print(d)

def tpcc_contention(label):
  tag = 'macrobench'
  selectors = ['tput']

  algs = ['NO_WAIT', 'SILO-REF', 'SILO', 'TICTOC', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'HEKATON', 'ERMIA-SI_SSN-REF', 'MICA+INDEX']
  all_results = []
  for alg in algs:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = alg

    results = filter(selectors, conds, 'thread_count')
    check_count(results)
    all_results.append(results['tput'])

  d = pd.concat(all_results, axis=1)
  d.columns = algs

  print('tpcc_contention [%s]:' % label)
  print(d)

def ycsb_contention(label):
  tag = 'macrobench'
  selectors = ['tput']

  algs = ['NO_WAIT', 'SILO-REF', 'SILO', 'TICTOC', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'HEKATON', 'ERMIA-SI_SSN-REF', 'MICA+INDEX']
  all_results = []
  for alg in algs:
    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = alg

    results = filter(selectors, conds, 'thread_count')
    check_count(results)
    all_results.append(results['tput'])

  d = pd.concat(all_results, axis=1)
  d.columns = algs

  print('ycsb_contention [%s]:' % label)
  print(d)


def backoff():
  tag = 'backoff'
  selectors = ['tput']

  all_results = []
  labels = ['tpcc-verycontended-backoff', 'tpcc-contended-backoff',
          'tpcc-full-verycontended-backoff', 'tpcc-full-contended-backoff',
          'ycsb-contended-skewed-kv-backoff', 'ycsb-contended-skewed-backoff']
  for label in labels:
    conds = dict(find_cond(label))
    conds['tag'] = 'macrobench'
    conds['alg'] = 'MICA+INDEX'
    results = filter(selectors, conds, 'thread_count')
    check_count(results)
    base_tput = results['tput'].iloc[0]

    conds = dict(find_cond(label))
    conds['tag'] = tag
    conds['alg'] = 'MICA+INDEX'
    results = filter(selectors, conds, 'fixed_backoff')
    check_count(results)
    results = results[['tput']] / base_tput
    all_results.append(results)

  d = pd.concat(all_results, axis=1)
  d.columns = labels

  print('backoff:')
  print(d)

def inline():
  tag = 'inlining'
  selectors = ['tput']

  for label in ['ycsb-uncontended-tx']:
    for record_size in [10, 20, 40, 100, 200, 400, 1000, 2000]:
      for no_inlining in [False, True]:
        conds = dict(find_cond(label))
        conds['tag'] = tag
        conds['alg'] = 'MICA+INDEX'
        conds['record_size'] = record_size

        results = filter(selectors, conds, 'no_inlining')
        check_count(results)

        if no_inlining:
          tput = query('''
            SELECT * FROM results WHERE no_inlining=1
            ''' , locals()).at[0, 'tput']
        else:
          tput = query('''
            SELECT * FROM results WHERE no_inlining IS NULL
            ''' , locals()).at[0, 'tput']
        #print(tput)

        print('inline-%s-%d (%s) [%s]: ' % (label, record_size, pp.pformat(conds).replace('\n', ''), 'no_inlining' if no_inlining else 'inlining'), end='')
        print('{:.2f} M tps\n'.format(tput / 1000000.), end='')


def query_all():
  macrobench()
  factor()
  gc()
  scan()
  full_table_scan()

  # first 4 graphs in slides
  #tpcc_overhead('tpcc-full-uncontended-overhead')
  #ycsb_overhead('ycsb-uncontended-tx-overhead')
  #tpcc_contention('tpcc-full-verycontended-contention')
  #ycsb_contention('ycsb-contended-tx-contention')

  # 4 more graphs in backup slides
  #tpcc_overhead('tpcc-full-uncontended-siu-overhead')
  #tpcc_contention('tpcc-full-verycontended-siu-contention')
  #tpcc_overhead('tpcc-uncontended-overhead')
  #tpcc_contention('tpcc-verycontended-contention')

  # loosely synchronized timestamp graph
  #scalability()

  # contention regulation graph
  #backoff()

  # inlining summary
  #inline()


if __name__ == '__main__':
  init()
  if len(sys.argv) == 2:
    sql = sys.argv[1]
    print(sql)
    print(query(sql, locals()))
  else:
    query_all()
