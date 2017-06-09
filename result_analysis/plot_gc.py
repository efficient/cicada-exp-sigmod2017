from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

# for req_per_query in [1, 16]:
#   read_ratio = 0.5
#   zipf_theta = 0.99

#   cond = df['bench'] == 'YCSB'
#   cond &= df['total_count'] == 10000000
#   cond &= df['record_size'] == 1000
#   cond &= df['thread_count'] == max_thread_count
#   cond &= df['req_per_query'] == req_per_query
#   cond &= df['read_ratio'] == read_ratio
#   cond &= df['zipf_theta'] == zipf_theta

#   filename ='output_gc_ycsb_req_%d_read_%.2f_zipf_%.2f' % (req_per_query, read_ratio, zipf_theta)
#   plot_tput_gc_graph(df[cond & (df['tag'] == 'macrobench')], df[cond & (df['tag'] == 'gc')], 'slow_gc',
#     r'Epoch update interval', filename, max_thread_count=max_thread_count)


for bench in ['TPCC', 'TPCC-FULL']:
  cond = df['bench'] == bench
  cond &= df['thread_count'] == max_thread_count
  cond &= df['simple_index_update'].isnull()

  filename = 'output_gc_%s' % (bench.lower())
  filename = filename.replace('.', '_')
  try:
    plot_tput_gc_graph(df[cond & (df['tag'] == 'gc')], 'slow_gc', r'Minimum interval of quiescence', filename, max_thread_count=max_thread_count)
  except:
    import traceback
    traceback.print_exc()
