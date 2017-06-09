from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

# for req_per_query, zipf_theta in [(16, 0.90), (16, 0.99), (1, 0.99)]:
for req_per_query, zipf_theta in [(16, 0.99), (1, 0.99)]:
  read_ratio = 0.5
  cond = df['bench'] == 'YCSB'
  cond &= df['total_count'] == 10000000
  #  cond &= df['record_size'] == 1000
  cond &= df['record_size'] == 100
  cond &= df['thread_count'] == max_thread_count
  cond &= df['req_per_query'] == req_per_query
  cond &= df['read_ratio'] == read_ratio
  cond &= df['zipf_theta'] == zipf_theta

  filename ='output_backoff_ycsb_req_%d_read_%.2f_zipf_%.2f' % (req_per_query, read_ratio, zipf_theta)
  filename = filename.replace('.', '_')
  try:
    plot_tput_abort_rate_graph(df[cond & (df['tag'] == 'macrobench')], df[cond & (df['tag'] == 'backoff')], 'fixed_backoff',
      r'Manually chosen maximum backoff time (\textmu{}s)', filename, max_thread_count=max_thread_count)
  except:
    import traceback
    traceback.print_exc()


for bench in ['TPCC', 'TPCC-FULL']:
  # for warehouse_count in [4, max_thread_count]:
  for warehouse_count in [1, 4]:
    cond = df['bench'] == bench
    cond &= df['thread_count'] == max_thread_count
    cond &= df['warehouse_count'] == warehouse_count
    cond &= df['simple_index_update'].isnull()

    filename = 'output_backoff_%s_warehouse_%d' % (bench.lower(), warehouse_count)
    filename = filename.replace('.', '_')
    try:
      plot_tput_abort_rate_graph(df[cond & (df['tag'] == 'macrobench')], df[cond & (df['tag'] == 'backoff')], 'fixed_backoff',
        r'Manually chosen maximum backoff time (\textmu{}s)', filename, max_thread_count=max_thread_count)
    except:
      import traceback
      traceback.print_exc()
