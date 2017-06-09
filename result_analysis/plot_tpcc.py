from common import *

for bench, simple_index_update in [('TPCC', None), ('TPCC-FULL', None), ('TPCC-FULL', 1)]:
  df = load_data('exp_data')
  max_thread_count = detect_max_thread_count(df)
  # print(df)

  cond = df['tag'] == 'macrobench'
  cond &= df['bench'] == bench
  if simple_index_update:
    cond &= df['simple_index_update'] == 1
  else:
    cond &= df['simple_index_update'].isnull()
  df = df[cond]
  # print(df)

  if True:
    cond = df['warehouse_count'] == df['thread_count']
    filename = 'output_macrobench_%s_fixed_ratio' % bench.lower()
    if simple_index_update: filename += '_siu'
    filename = filename.replace('.', '_')
    try:
      plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_bar', bar_chart=True, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_abort', max_thread_count=max_thread_count, plot_abort_rate=True)
    except:
      import traceback
      traceback.print_exc()

  for warehouse_count in [1, 2, 4, 8, 16, max_thread_count]:
    cond = df['warehouse_count'] == warehouse_count

    filename = 'output_macrobench_%s_warehouse_%d' % (bench.lower(), warehouse_count)
    if simple_index_update: filename += '_siu'
    filename = filename.replace('.', '_')
    try:
      plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_bar', bar_chart=True, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_abort', max_thread_count=max_thread_count, plot_abort_rate=True)
    except:
      import traceback
      traceback.print_exc()

  # for thread_count in [1, 2, 4, 8, 16, max_thread_count, 32, 48, max_thread_count * 2]:
  for thread_count in [1, 2, 4, 8, 16, max_thread_count]:
    cond = df['thread_count'] == thread_count

    filename = 'output_macrobench_%s_thread_%d' % (bench.lower(), thread_count)
    if simple_index_update: filename += '_siu'
    filename = filename.replace('.', '_')
    try:
      plot_tput_graph(df[cond], 'warehouse_count', 'Warehouse count', filename, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'warehouse_count', 'Warehouse count', filename + '_bar', bar_chart=True, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'warehouse_count', 'Warehouse count', filename + '_abort', max_thread_count=max_thread_count, plot_abort_rate=True)
    except:
      import traceback
      traceback.print_exc()
