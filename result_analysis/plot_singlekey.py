from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

for tag in ['singlekey']:
  for record_size in [16]:
    req_per_query = 1
    for read_ratio in [0.0]:
      zipf_theta = 0.00

      cond = df['tag'] == tag
      cond &= df['bench'] == 'YCSB'
      cond &= df['total_count'] == 1
      cond &= df['record_size'] == record_size
      cond &= df['req_per_query'] == req_per_query
      cond &= df['read_ratio'] == read_ratio
      cond &= df['zipf_theta'] == zipf_theta

      filename ='output_%s_ycsb_record_%d_read_%.2f' % (tag, record_size, read_ratio)
      filename = filename.replace('.', '_')
      try:
        plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename, max_thread_count=max_thread_count)
      except:
        import traceback
        traceback.print_exc()
