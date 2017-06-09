from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

# for tag in ['factor', 'native-factor']:
for tag in ['factor']:
  # for req_per_query, read_ratio, zipf_theta in [(16, 0.50, 0.99), (1, 0.95, 0.00), (1, 0.95, 0.99)]:
  for req_per_query, read_ratio, zipf_theta in [(16, 0.50, 0.99), (1, 0.95, 0.99)]:
    cond = df['bench'] == 'YCSB'
    cond &= df['total_count'] == 10000000
    # cond &= df['record_size'] == 1000
    cond &= df['record_size'] == 100
    cond &= df['thread_count'] == max_thread_count
    cond &= df['req_per_query'] == req_per_query
    cond &= df['read_ratio'] == read_ratio
    cond &= df['zipf_theta'] == zipf_theta

    # filename ='output_%s_ycsb_req_%d_read_%.2f_zipf_%.2f_cumulative' % (tag, req_per_query, read_ratio, zipf_theta)
    # filename = filename.replace('.', '_')
    # try:
    #   plot_factor_graph(df[cond & (df['tag'] == tag)], filename, cumulative=True)
    # except:
    #   import traceback
    #   traceback.print_exc()

    filename ='output_%s_ycsb_req_%d_read_%.2f_zipf_%.2f_separate' % (tag, req_per_query, read_ratio, zipf_theta)
    filename = filename.replace('.', '_')
    try:
      plot_factor_graph(df[cond & (df['tag'] == tag)], filename, cumulative=False, max_thread_count=max_thread_count)
    except:
      import traceback
      traceback.print_exc()


  if tag == 'factor':
    for bench in ['TPCC', 'TPCC-FULL']:
      # for warehouse_count in [4, max_thread_count]:
      for warehouse_count in [4]:
        cond = df['bench'] == bench
        cond &= df['thread_count'] == max_thread_count
        cond &= df['warehouse_count'] == warehouse_count
        cond &= df['simple_index_update'].isnull()

        # filename = 'output_%s_%s_warehouse_%d_cumulative' % (tag,
        # bench.lower(), warehouse_count)
        # filename = filename.replace('.', '_')
        # try:
        #   plot_factor_graph(df[cond & (df['tag'] == tag)], filename, cumulative=True)
        # except:
        #   import traceback
        #   traceback.print_exc()

        filename = 'output_%s_%s_warehouse_%d_separate' % (tag, bench.lower(), warehouse_count)
        filename = filename.replace('.', '_')
        try:
          plot_factor_graph(df[cond & (df['tag'] == tag)], filename, cumulative=False, max_thread_count=max_thread_count)
        except:
          import traceback
          traceback.print_exc()
