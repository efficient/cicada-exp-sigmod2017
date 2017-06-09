from common import *

df = load_data('exp_data')
max_thread_count = detect_max_thread_count(df)
# print(df)

for bench, max_xlim in [('TPCC', 50), ('TPCC-FULL', 200)]:
  for warehouse_count in [1, 4, max_thread_count]:
    cond = df['bench'] == bench
    cond &= df['thread_count'] == max_thread_count
    cond &= df['warehouse_count'] == warehouse_count
    cond &= df['simple_index_update'].isnull()

    filename = 'output_latency_%s_warehouse_%d' % (bench.lower(), warehouse_count)
    filename = filename.replace('.', '_')
    try:
      plot_latency_graph(df[cond & (df['tag'] == 'macrobench')], filename, max_xlim, max_thread_count=max_thread_count)
    except:
      import traceback
      traceback.print_exc()
