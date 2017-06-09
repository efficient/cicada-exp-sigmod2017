from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

# for tag in ['macrobench', 'native-macrobench']:
for tag in ['macrobench']:
  # for record_size in [1000]:
  for record_size in [100]:
    for req_per_query in [1, 16]:
      for read_ratio in [0.5, 0.95]:
        # for zipf_theta in [0.00, 0.90, 0.99]:
        for zipf_theta in [0.00, 0.99]:
          cond = df['tag'] == tag
          cond &= df['bench'] == 'YCSB'
          cond &= df['total_count'] == 10000000
          cond &= df['record_size'] == record_size
          cond &= df['req_per_query'] == req_per_query
          cond &= df['read_ratio'] == read_ratio
          cond &= df['zipf_theta'] == zipf_theta

          filename ='output_%s_ycsb_record_%d_req_%d_read_%.2f_zipf_%.2f' % (tag, record_size, req_per_query, read_ratio, zipf_theta)
          filename = filename.replace('.', '_')
          try:
            plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename, max_thread_count=max_thread_count)
            # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_abort', max_thread_count=max_thread_count, plot_abort_rate=True)
          except:
            import traceback
            traceback.print_exc()

    for req_per_query in [1, 16]:
      for read_ratio in [0.5, 0.95]:
        for thread_count in [max_thread_count]:
          cond = df['tag'] == tag
          cond &= df['bench'] == 'YCSB'
          cond &= df['total_count'] == 10000000
          cond &= df['record_size'] == record_size
          cond &= df['req_per_query'] == req_per_query
          cond &= df['read_ratio'] == read_ratio
          cond &= df['thread_count'] == thread_count

          filename = 'output_%s_ycsb_record_%d_req_%d_read_%.2f_thread_%d' % (tag, record_size, req_per_query, read_ratio, thread_count)
          filename = filename.replace('.', '_')
          try:
            plot_tput_graph(df[cond], 'zipf_theta', 'Zipf skew', filename, normalize_x_axis=True, max_thread_count=max_thread_count)
            # plot_tput_graph(df[cond], 'zipf_theta', 'Zipf skew', filename + '_abort', normalize_x_axis=True, max_thread_count=max_thread_count, plot_abort_rate=True)
          except:
            import traceback
            traceback.print_exc()
