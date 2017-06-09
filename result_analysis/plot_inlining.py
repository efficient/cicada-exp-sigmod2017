from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

for tag in ['inlining']:
  for req_per_query in [16]:
    for read_ratio in [0.95]:
      for zipf_theta in [0.00]:
        for thread_count in [max_thread_count]:
            cond = df['tag'] == tag
            cond &= df['bench'] == 'YCSB'
            cond &= df['total_count'] == 10000000
            cond &= df['req_per_query'] == req_per_query
            cond &= df['read_ratio'] == read_ratio
            cond &= df['zipf_theta'] == zipf_theta
            cond &= df['thread_count'] == thread_count

            filename ='output_%s_ycsb_req_%d_read_%.2f_zipf_%.2f_thread_%d' % (tag, req_per_query, read_ratio, zipf_theta, thread_count)
            filename = filename.replace('.', '_')
            try:
              plot_tput_graph(df[cond], 'record_size', 'Record size (bytes)', filename, max_thread_count=max_thread_count)
            except:
              import traceback
              traceback.print_exc()
