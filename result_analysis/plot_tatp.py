from common import *

for bench in ['TATP']:
  df = load_data('exp_data')
  max_thread_count = detect_max_thread_count(df)
  # print(df)

  cond = df['tag'] == 'macrobench'
  cond &= df['bench'] == bench
  df = df[cond]
  # print(df)

  for scale_factor in [1]:
    cond = df['scale_factor'] == scale_factor

    filename = 'output_macrobench_%s_scale_%d' % (bench.lower(), scale_factor)
    filename = filename.replace('.', '_')
    try:
      plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'thread_count', 'Thread count', filename + '_bar', bar_chart=True, max_thread_count=max_thread_count)
    except:
      import traceback
      traceback.print_exc()

  # for thread_count in [1, 4, 8, 16, max_thread_count, 48, max_thread_count * 2]:
  for thread_count in [1, 4, 8, 16, max_thread_count]:
    cond = df['thread_count'] == thread_count

    filename = 'output_macrobench_%s_thread_%d' % (bench.lower(), thread_count)
    filename = filename.replace('.', '_')
    try:
      plot_tput_graph(df[cond], 'scale_factor', 'Scale factor', filename, max_thread_count=max_thread_count)
      # plot_tput_graph(df[cond], 'scale_factor', 'Scale factor', filename + '_bar', bar_chart=True, max_thread_count=max_thread_count)
    except:
      import traceback
      traceback.print_exc()
