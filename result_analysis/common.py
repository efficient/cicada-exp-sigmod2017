from parse import *

import numpy as np
import matplotlib
matplotlib.use('Agg')
matplotlib.rc('text', usetex=True)

import matplotlib.pyplot as plt
# from mpl_toolkits.mplot3d import Axes3D
# import matplotlib.colors as colors
# import matplotlib.cm as cmx
# from matplotlib.ticker import LinearLocator, FormatStrFormatter

# dpi for png
dpi = 200

general_fontsize = 13
legend_fontsize = 12

matplotlib.rcParams.update({'font.size': general_fontsize})
# a workaround for a unicode character (\textmu) problem that may occur with matplotlib >= 2.0
#matplotlib.rcParams.update({'font.size': general_fontsize, 'text.latex.preamble': [r'\usepackage{lmodern}']})


def detect_max_thread_count(df):
  max_thread_count = max(df['thread_count'])
  assert max_thread_count is not None
  return max_thread_count

def set_common_globals(max_thread_count):
  global alg_names
  global styles

  alg_names = {
    'MICA': r'Cicada-',
    # 'MICA+INDEX': 'Cicada+MVIdx',
    'MICA+INDEX': r'\textbf{Cicada}',
    'MICA+FULLINDEX': 'Cicada+FullMVIdx',
    'SILO': 'Silo$\'$',
    'TICTOC': 'TicToc',
    'HEKATON': 'Hekaton',
    'NO_WAIT': '2PL',

    'SILO-REF': 'Silo',
    # 'SILO-REF-BACKOFF': 'SiloRef-BO',
    'ERMIA-SI-REF': 'ERMIA',
    'ERMIA-SSI-REF': 'ERMIA-SSI',
    #  'ERMIA-SI_SSN-REF': 'ERMIA-SSN',
    'ERMIA-SI_SSN-REF': 'ERMIA',
    # 'ERMIA-SI-REF-BACKOFF': 'ERMIA-BO',
    # 'ERMIA-SSI-REF-BACKOFF': 'ERMIA-SSI-BO',
    # 'ERMIA-SI_SSN-REF-BACKOFF': 'ERMIA-SSN-BO',
    'FOEDUS-OCC-REF': 'FOEDUS',
    'FOEDUS-MOCC-REF': 'MOCC',

    # 'MICA-NO-INLINING': r'\textbf{Cicada} (no inlining)',
    # 'MICA+INDEX-NO-INLINING': r'Cicada+MVIdx (no inlining)',
    'MICA-NO-INLINING': r'Cicada- (no inlining)',
    'MICA+INDEX-NO-INLINING': r'Cicada (no inlining)',

    # # 'MICA_auto_backoff_tput': r'\textbf{Cicada}',
    # 'MICA_auto_backoff_tput': 'Throughput (auto)',
    # 'MICA_auto_abort_rate': 'Abort rate (auto)',
    # 'MICA_manual_abort_rate': r'Abort rate (manual)',

    'MICA+INDEX_auto_backoff_tput': 'Throughput (auto)',
    'MICA+INDEX_auto_abort_rate': 'Abort rate (auto)',
    'MICA+INDEX_manual_abort_rate': r'Abort rate (manual)',

    # 'MICA_gc_1': 'TPC-C (1 warehouse)',
    # 'MICA_gc_4': 'TPC-C (4 warehouses)',
    # 'MICA_gc_%d' % max_thread_count: 'TPC-C (%d warehouses)' % max_thread_count,

    'MICA+INDEX_gc_1': '1 warehouse',
    'MICA+INDEX_gc_4': '4 warehouses',
    'MICA+INDEX_gc_%d' % max_thread_count: '%d warehouses' % max_thread_count,
  }

  # markers: o s < > d * v ^
  # hatches: / x - | o .

  styles = {
    # color, linestyle, linewidth, marker, hatch
    # 'MICA':
    #   ['r', '-', 2.5, 'o', ''],
    # 'MICA+INDEX':
    #   ['r', '--', 1, 's', ''],
    'MICA':
      ['m', '--', 1, '.', ''],
    'MICA+INDEX':
      ['r', '-', 2.5, 'o', ''],
    # 'MICA+FULLINDEX':
    #   ['r', '-', 1, 'd', ''],

    'SILO':
      ['b', '-', 1, '<', ''],
    'TICTOC':
      ['c', '-', 1, '>', ''],
    'HEKATON':
      ['g', '-', 1, 's', ''],
    'NO_WAIT':
      ['y', '-', 1, 'd', ''],

    'SILO-REF':
      ['b', '--', 1, '<', ''],
    'SILO-REF-BACKOFF':
      ['b', '--', 1, '<', ''],
    'ERMIA-SI-REF':
      ['g', '--', 1, 's', ''],
    'ERMIA-SI-REF-BACKOFF':
      ['g', '--', 1, 's', ''],
    'ERMIA-SSI-REF':
      ['g', '-.', 1, 's', ''],
    'ERMIA-SSI-REF-BACKOFF':
      ['g', '-.', 1, 's', ''],
    'ERMIA-SI_SSN-REF':
      ['g', '--', 1, 's', ''],
    'ERMIA-SI_SSN-REF-BACKOFF':
      ['g', '--', 1, 's', ''],
    'FOEDUS-OCC-REF':
      ['c', '--', 1, '^', ''],
    'FOEDUS-MOCC-REF':
      ['y', '--', 1, 'v', ''],

    # 'MICA-NO-INLINING':
    #   ['k', '-', 1.5, 'o', ''],
    # 'MICA+INDEX-NO-INLINING':
    #   ['k', '--', 1, 's', ''],
    'MICA-NO-INLINING':
      ['m', ':', 1, '.', ''],
    'MICA+INDEX-NO-INLINING':
      ['r', '--', 2.5, 'o', ''],

    # 'MICA_auto_backoff_tput':
    #   ['m', '-', 2, '', ''],
    # 'MICA_auto_abort_rate':
    #   ['c', '-', 2.5, '', ''],
    # 'MICA_manual_abort_rate':
    #   ['b', '-', 2.5, 's', ''],
    'MICA+INDEX_auto_backoff_tput':
      ['m', '-', 2.5, '', ''],
    'MICA+INDEX_auto_abort_rate':
      ['c', '-', 1.5, '', ''],
    'MICA+INDEX_manual_abort_rate':
      ['b', '-', 1.5, 's', ''],

    # 'MICA_gc_1':
    #   ['r', '-', 1, 'o', ''],
    # 'MICA_gc_4':
    #   ['b', '-', 1, 's', ''],
    # 'MICA_gc_%d' % max_thread_count:
    #   ['c', '-', 2, 'd', ''],
    'MICA+INDEX_gc_1':
      ['r', '-', 1, 'o', ''],
    'MICA+INDEX_gc_4':
      ['b', '-', 1, 's', ''],
    'MICA+INDEX_gc_%d' % max_thread_count:
      ['c', '-', 2, 'd', ''],

    # 'MICA_factor':
    #   ['y', '-', 1, '', ''],
    'MICA+INDEX_factor':
      ['y', '-', 1, '', ''],
  }

def plot_tput_graph(df, groupby, groupby_name, filename, normalize_x_axis=False, max_thread_count=None, plot_abort_rate=False):
  set_common_globals(max_thread_count)

  print(filename)

  fig = plt.figure(figsize=(6, 3.5))
  ax = fig.add_subplot(111)

  all_xs = set()
  max_y = 0

  algs = ['MICA+INDEX', 'SILO', 'TICTOC', 'NO_WAIT', 'HEKATON', 'SILO-REF', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'ERMIA-SI-REF', 'ERMIA-SSI-REF', 'ERMIA-SI_SSN-REF']

  if groupby != 'record_size':
    local_algs = algs
  else:
    local_algs = ['MICA+INDEX', 'MICA+INDEX-NO-INLINING', 'SILO', 'TICTOC', 'NO_WAIT', 'HEKATON', 'SILO-REF', 'FOEDUS-OCC-REF', 'FOEDUS-MOCC-REF', 'ERMIA-SI-REF', 'ERMIA-SSI-REF', 'ERMIA-SI_SSN-REF']

  for idx, alg in enumerate(local_algs):
    if alg == None: continue

    df_alg = df[df['alg'] == alg]
    if groupby == 'record_size':
      if alg in ('MICA', 'MICA+INDEX'):
        df_alg = df_alg[df_alg['no_inlining'].isnull()]
      elif alg in ('MICA-NO-INLINING', 'MICA+INDEX-NO-INLINING'):
        df_alg = df[df['alg'] == alg.partition('-')[0]]
        df_alg = df_alg[df_alg['no_inlining'] == 1]
    if not plot_abort_rate:
      df_alg = df_alg.groupby(groupby)['tput']
    else:
      df_alg = df_alg.groupby(groupby)['abort_rate']
    if not df_alg: continue

    xs = df_alg.mean().index
    # Several fields become float when mixed with a null value.
    if groupby in ('warehouse_count', 'record_size'):
      xs = [int(x) for x in xs]

    all_xs.update(xs)

  all_xs = list(sorted(all_xs))
  # print(all_xs)

  lines = []
  labels = []

  for idx, alg in enumerate(local_algs):
    if alg == None: continue

    df_alg = df[df['alg'] == alg]
    if groupby == 'record_size':
      if alg in ('MICA', 'MICA+INDEX'):
        df_alg = df_alg[df_alg['no_inlining'].isnull()]
      elif alg in ('MICA-NO-INLINING', 'MICA+INDEX-NO-INLINING'):
        df_alg = df[df['alg'] == alg.partition('-')[0]]
        df_alg = df_alg[df_alg['no_inlining'] == 1]
    if not plot_abort_rate:
      df_tput = df_alg.groupby(groupby)['tput']
    else:
      df_tput = df_alg.groupby(groupby)['abort_rate']
    if not df_tput: continue

    xs = df_tput.mean().index
    if groupby in ('warehouse_count', 'record_size'):
      xs = [int(x) for x in xs]
    if not plot_abort_rate:
      ys = (df_tput.mean() / 1000000.).tolist()
      yerrs = [
        ((df_tput.mean() - df_tput.min()) / 1000000.).tolist(),
        ((df_tput.max() - df_tput.mean()) / 1000000.).tolist(),
      ]
    else:
      ys = (df_tput.mean() * 100.).tolist()
      yerrs = [
        ((df_tput.mean() - df_tput.min()) * 100.).tolist(),
        ((df_tput.max() - df_tput.mean()) * 100.).tolist(),
      ]

    max_y = max([max_y] + ys)

    if normalize_x_axis:
      xs = [all_xs.index(x) for x in xs]

    color, linestyle, linewidth, marker, hatch = styles[alg]
    label = alg_names[alg]
    lines += ax.plot(xs, ys, linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=5, clip_on=False)
    ax.errorbar(xs, ys, yerr=yerrs, ecolor=color, zorder=5, fmt='none')
    labels += [label]

    # if groupby == 'record_size' and idx == 3:
    #   lines += ax.plot([0, 0], [0, 0], linewidth=0, color='none', marker='')
    #   labels += ['']
    #   lines += ax.plot([0, 0], [0, 0], linewidth=0, color='none', marker='')
    #   labels += ['']

  ax.set_xlabel(groupby_name)
  if not plot_abort_rate:
    ax.set_ylabel(r'Throughput (M tps)')
  else:
    ax.set_ylabel(r'Abort rate (\%)')

  if not plot_abort_rate:
    ax.set_ylim(0, max_y * 1.10)
    # ax.set_ylim(0, max_y * 1.25)
  else:
    ax.set_ylim(0, 100)

  if not normalize_x_axis:
    ax.set_xticks(all_xs)
    margin = max(max(all_xs) - min(all_xs), 1) * 0.10
    ax.set_xlim(min(all_xs) - margin, max(all_xs) + margin)
  else:
    ax.set_xticks(list(range(len(all_xs))))
    margin = len(all_xs) * 0.10
    ax.set_xlim(-margin, (len(all_xs) - 1) + margin)
  ax.set_xticklabels(['$' + str(x) + '$' for x in all_xs])

  legend_loc = 'best'
  legend_ncol = 2

  if groupby == 'record_size':
    max_inline_size = 256 - 40
    # ax.plot((max_inline_size, max_inline_size), (0, 10), linewidth=2, color='k', linestyle='-', zorder=7, alpha=0.5)
    ax.set_xscale('log')
    margin_factor = 1.8
    ax.set_xlim(min(all_xs) / margin_factor, max(all_xs) * margin_factor)
    ax.set_xticks(all_xs)
    ax.set_xticklabels(['$' + str(x) + '$' for x in all_xs])

    # ax.set_ylim(0, max_y * 1.10)
    ax.set_ylim(0, max_y * 1.15)

    # legend_loc = 'lower left'
    # legend_ncol = 2

  #ax.grid()
  ax.yaxis.grid(True, which='major', color='0.7', linestyle='-')
  plt.tight_layout()

  if len(lines) > 1:
    if filename.find('macrobench') != -1 and filename.find('tpcc') != -1 and filename.find('fixed_ratio') == -1:
      pass
    elif filename.find('macrobench') != -1 and filename.find('ycsb') != -1 and filename.find('req_16') != -1 and filename.find('thread_') != -1 and filename.find('read_0_50') == -1:
      pass
    elif filename.find('macrobench') != -1 and filename.find('ycsb') != -1 and filename.find('req_16') != -1 and filename.find('zipf_') != -1:
      pass
    elif filename.find('macrobench') != -1 and filename.find('ycsb') != -1 and filename.find('req_1_') != -1 and filename.find('zipf_') == -1:
      pass
    elif groupby == 'record_size':
      legend1 = ax.legend(lines[:2], labels[:2], loc='upper right', fontsize=legend_fontsize, framealpha=0.0, ncol=1)
      legend1.set_zorder(6)
      legend1.get_frame().set_linewidth(0)

      legend2 = ax.legend(lines[2:], labels[2:], loc='lower left', fontsize=legend_fontsize, framealpha=0.0, ncol=2)
      legend2.set_zorder(6)
      legend2.get_frame().set_linewidth(0)

      plt.gca().add_artist(legend1)
    elif filename.find('ycsb') != -1 and filename.find('thread') != -1:
      legend1 = ax.legend(lines[:5], labels[:5], loc='upper right', fontsize=legend_fontsize, framealpha=0.0, ncol=1)
      legend1.set_zorder(6)
      legend1.get_frame().set_linewidth(0)

      legend2 = ax.legend(lines[5:], labels[5:], loc='lower left', fontsize=legend_fontsize, framealpha=0.0, ncol=2, columnspacing=0.5)
      legend2.set_zorder(6)
      legend2.get_frame().set_linewidth(0)

      plt.gca().add_artist(legend1)
    elif filename.find('ycsb') != -1 and filename.find('req_16') != -1 and filename.find('read_0_50') != -1 and filename.find('zipf_0_99') != -1:
      pass
    else:
      # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.8, ncol=2)
      legend = ax.legend(lines, labels, loc=legend_loc, fontsize=legend_fontsize, framealpha=0.0, ncol=legend_ncol, columnspacing=1)
      if legend is not None:
          legend.set_zorder(6)
          legend.get_frame().set_linewidth(0)

  plt.savefig('%s.pdf' % filename, bbox_inches='tight')
  plt.savefig('%s.png' % filename, dpi=dpi, bbox_inches='tight')
  plt.close(fig)
  plt.clf()


def plot_tput_abort_rate_graph(df_auto, df_manual, groupby, groupby_name, filename, max_thread_count=None):
  set_common_globals(max_thread_count)
  print(filename)

  # df_manual = df_manual[df_manual['fixed_backoff'] <= 120.]
  df_manual = df_manual[df_manual['fixed_backoff'] <= 20.]

  # fig = plt.figure(figsize=(6, 4))
  fig = plt.figure(figsize=(6, 3))
  ax = fig.add_subplot(111)

  ax2 = ax.twinx()

  all_xs = set()
  max_y = 0

  lines = []
  # for idx, alg in enumerate(['MICA', 'SILO', 'TICTOC']):
  # for idx, alg in enumerate(['MICA']):
  for idx, alg in enumerate(['MICA+INDEX']):
    # automatic backoff data
    if idx == 0:
      df_auto_alg = df_auto[df_auto['alg'] == alg]
      df_auto_tput = df_auto_alg['tput']
      df_auto_abort_rate = df_auto_alg['abort_rate']
      # print(df_auto_tput)

    # manual backoff data
    df_manual_alg = df_manual[df_manual['alg'] == alg]
    df_manual_tput = df_manual_alg.groupby(groupby)['tput']
    df_manual_abort_rate = df_manual_alg.groupby(groupby)['abort_rate']
    assert list(df_manual_tput.mean().index) == list(df_manual_abort_rate.mean().index)

    ## automatic backoff tput
    if idx == 0:
      auto_tput = df_auto_tput.mean() / 1000000.

      color, linestyle, linewidth, marker, hatch = styles[alg + '_auto_backoff_tput']
      label = alg_names[alg + '_auto_backoff_tput']
      lines += ax.plot((-100., 300.), (auto_tput, auto_tput), linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=4)

      ## automatic backoff abort rate
      auto_abort_rate = df_auto_abort_rate.mean()

      color, linestyle, linewidth, marker, hatch = styles[alg + '_auto_abort_rate']
      label = alg_names[alg + '_auto_abort_rate']
      lines += ax2.plot((-100., 300.), (auto_abort_rate * 100., auto_abort_rate * 100.), linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=4)

    ## manual backoff tput
    xs = df_manual_tput.mean().index
    ys = (df_manual_tput.mean() / 1000000.).tolist()
    yerrs = [
      ((df_manual_tput.mean() - df_manual_tput.min()) / 1000000.).tolist(),
      ((df_manual_tput.max() - df_manual_tput.mean()) / 1000000.).tolist(),
    ]

    all_xs.update(xs)
    max_y = max([max_y] + ys)

    color, linestyle, linewidth, marker, hatch = styles[alg]
    # label = alg_names[alg] + ' (manual)'
    label = 'Throughput (manual)'

    lines += ax.plot(xs, ys, linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=5)  # , clip_on=False
    ax.errorbar(xs, ys, yerr=yerrs, ecolor=color, zorder=5, fmt='none')

    ## manual backoff abort rate
    ys2 = (df_manual_abort_rate.mean() * 100.).tolist()
    yerrs2 = [
      ((df_manual_abort_rate.mean() - df_manual_abort_rate.min()) * 100.).tolist(),
      ((df_manual_abort_rate.max() - df_manual_abort_rate.mean()) * 100.).tolist(),
    ]

    color, linestyle, linewidth, marker, hatch = styles[alg + '_manual_abort_rate']
    label = alg_names[alg + '_manual_abort_rate']
    lines += ax2.plot(xs, ys2, linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=5)  # , clip_on=False
    ax2.errorbar(xs, ys2, yerr=yerrs2, ecolor=color, zorder=5, fmt='none')

  all_xs = list(sorted(all_xs))
  # print(all_xs)

  ax.set_xlabel(groupby_name)
  ax.set_ylabel(r'Throughput (M tps)')

  ax.set_ylim(0, max_y * 1.1)
  # ax.set_ylim(0, max_y * 1.25)
  # ax.set_ylim(0, max_y * 1.4)

  ax2.set_ylabel(r'Abort rate (\%)')
  ax2.set_ylim(0, 100)

  # XXX: hack for contended YCSB workload
  # if 0.9 < max_y < 1.1:
  #   ax.set_ylim(0, 1.0)
  #   ax2.set_ylim(0, 100)

  margin = max(max(all_xs) - min(all_xs), 1) * 0.10
  ax.set_xlim(min(all_xs) - margin, max(all_xs) + margin)
  # ax.set_xticks(all_xs)
  # ax.set_xticklabels(['$' + str(x) + '$' for x in all_xs])

  # ax.set_xscale('log')
  # ax.set_xlim(0.05, max(all_xs))
  # ax.set_xticks([0.05, 0.25, 1, 10, 100])
  # ax.set_xticklabels(['$' + str(x) + '$' for x in [0.05, 0.25, 1, 10, 100]])

  #ax.grid()
  ax.yaxis.grid(True, which='major', color='0.7', linestyle='-')
  # plt.tight_layout()

  labels = [l.get_label() for l in lines]

  # lines = lines[1:] + lines[:1]
  # labels = labels[1:] + labels[:1]

  # XXX: hack to show the legend on TPC-C only
  if filename.find('tpcc-full') != -1:
    # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.8, ncol=2)
    legend = ax.legend(lines, labels, loc='lower left', fontsize=legend_fontsize, framealpha=0.0, ncol=2)
    if legend is not None:
        legend.set_zorder(6)
        legend.get_frame().set_linewidth(0)

  # XXX: hack to show the x ticks on some YCSB only
  if filename.find('ycsb') != -1 and filename.find('req_1_') != -1:
    plt.subplots_adjust(bottom=0.2)
  else:
    ax.set_xticklabels(['' for x in all_xs])
    ax.set_xlabel('')
    fig.set_size_inches(6, 2.6)
    # plt.tight_layout()

  # plt.savefig('%s.pdf' % filename, bbox_inches='tight')
  # plt.savefig('%s.png' % filename, dpi=dpi, bbox_inches='tight')
  plt.savefig('%s.pdf' % filename)
  plt.savefig('%s.png' % filename, dpi=dpi)
  plt.close(fig)
  plt.clf()


def plot_tput_gc_graph(df, groupby, groupby_name, filename, max_thread_count=None):
  set_common_globals(max_thread_count)
  print(filename)

  fig = plt.figure(figsize=(6, 3))
  ax = fig.add_subplot(111)

  all_xs = set()
  max_y = 0

  idx = 0
  # alg = 'MICA'
  alg = 'MICA+INDEX'
  lines = []
  # for warehouse_count in [1, 4, max_thread_count]:
  for warehouse_count in [max_thread_count, 4, 1]:
    df_alg = df[df['alg'] == alg]
    df_wc = df_alg[df_alg['warehouse_count'] == warehouse_count]

    df_tput = df_wc.groupby(groupby)['tput']

    xs = df_tput.mean().index
    ys = (df_tput.mean() / 1000000.).tolist()
    yerrs = [
      ((df_tput.mean() - df_tput.min()) / 1000000.).tolist(),
      ((df_tput.max() - df_tput.mean()) / 1000000.).tolist(),
    ]

    all_xs.update(xs)
    max_y = max([max_y] + ys)

    color, linestyle, linewidth, marker, hatch = styles[alg + '_gc_' + str(warehouse_count)]
    label = alg_names[alg + '_gc_' + str(warehouse_count)]

    lines += ax.plot(xs, ys, linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=5, clip_on=False)
    ax.errorbar(xs, ys, yerr=yerrs, ecolor=color, zorder=5, fmt='none')

  all_xs = list(sorted(all_xs))
  # print(all_xs)

  ax.set_xlabel(groupby_name)
  ax.set_ylabel(r'Throughput (M tps)')

  ax.set_ylim(0, max_y * 1.10)
  # ax.set_ylim(0, max_y * 1.15)
  # ax.set_ylim(0, max_y * 1.25)
  # ax.set_ylim(0, max_y * 1.4)

  # margin = max(max(all_xs) - min(all_xs), 1) * 0.10
  ax.set_xscale('log')
  # ax.set_xlim(min(all_xs) - margin, max(all_xs) + margin)
  # ax.set_xticks(all_xs)
  # ax.set_xticklabels(['$' + str(x) + '$' for x in all_xs])
  ax.set_xticks([1, 10, 100, 1000, 10000, 100000])
  ax.set_xticklabels([r'$1$ \textmu{}s', r'$10$ \textmu{}s', r'$100$ \textmu{}s', r'$1$ ms', r'$10$ ms', r'$100$ ms'])

  #ax.grid()
  ax.yaxis.grid(True, which='major', color='0.7', linestyle='-')
  plt.tight_layout()

  labels = [l.get_label() for l in lines]

  # lines.insert(2, ax.plot([0, 0], [0, 0], linewidth=0, color='none', marker='')[0])
  # labels.insert(2, '')

  # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.8, ncol=2)
  legend = ax.legend(lines, labels, loc='lower left', fontsize=legend_fontsize, framealpha=0.0, ncol=1, borderpad=0.0)
  if legend is not None:
      legend.set_zorder(6)
      legend.get_frame().set_linewidth(0)
  plt.savefig('%s.pdf' % filename, bbox_inches='tight')
  plt.savefig('%s.png' % filename, dpi=dpi, bbox_inches='tight')
  plt.close(fig)
  plt.clf()


def plot_latency_graph(df, filename, max_xlim, max_thread_count=None):
  set_common_globals(max_thread_count)
  print(filename)

  fig = plt.figure(figsize=(6, 4))
  ax = fig.add_subplot(111)

  # for idx, alg in enumerate(['MICA', 'SILO', 'TICTOC']):
  for idx, alg in enumerate(['MICA+INDEX', 'SILO', 'TICTOC']):
    df_alg = df[df['alg'] == alg]
    # print(df_alg)

    latency_bins = [0] * 1024 * 4

    for exp_path in df_alg['path'].tolist():
      print('  ' + exp_path)
      read_latency_dist(exp_path, latency_bins)

    cdf = [0] * (len(latency_bins) + 1)
    for i in range(1, len(cdf)):
      cdf[i] = cdf[i - 1] + latency_bins[i - 1]

    xs = list(range(len(cdf)))
    ys = np.array(cdf) / (1. * cdf[-1])

    color, linestyle, linewidth, marker, hatch = styles[alg]
    marker = ''
    label = alg_names[alg]

    ax.plot(xs, ys, linewidth=linewidth, color=color, linestyle=linestyle, label=label, marker=marker, markerfacecolor='none', markeredgecolor=color, zorder=5)

  ax.set_xlabel(r'Latency (\textmu{}s)')
  ax.set_ylabel(r'CDF')

  ax.set_ylim(0, 1)
  ax.set_yticks([0., 0.25, 0.5, 0.75, 1.])

  # ax.set_xscale('log')
  # ax.set_xlim(1, 200)
  # ax.set_xlim(0, 200)
  # ax.set_xlim(0, 100)
  # ax.set_xlim(0, 50)
  ax.set_xlim(0, max_xlim)
  # ax.set_xticks([1, 3.3, 10, 33, 100])
  # ax.set_xticklabels([r'$1$ \textmu{}s', r'$3.3$ \textmu{}s', r'$10$ \textmu{}s', r'$33$ \textmu{}s', r'$100$ \textmu{}s'])

  #ax.grid()
  ax.yaxis.grid(True, which='major', color='0.7', linestyle='-')
  plt.tight_layout()

  # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.8, ncol=2)
  legend = ax.legend(loc='lower right', fontsize=legend_fontsize, framealpha=0.0, ncol=2)
  if legend is not None:
      legend.set_zorder(6)
      legend.get_frame().set_linewidth(0)
  plt.savefig('%s.pdf' % filename, bbox_inches='tight')
  plt.savefig('%s.png' % filename, dpi=dpi, bbox_inches='tight')
  plt.close(fig)
  plt.clf()


def plot_factor_graph(df, filename, cumulative, max_thread_count=None):
  set_common_globals(max_thread_count)
  print(filename)

  # fig = plt.figure(figsize=(6, 4))
  fig = plt.figure(figsize=(6, 2.5))
  ax = fig.add_subplot(111)

  ax2 = ax.twinx()

  idx = 0
  # alg = 'MICA'
  alg = 'MICA+INDEX'
  if 1:
    df_alg = df[df['alg'] == alg]
    # print(df_alg)

    xs = []
    ys = []
    yerrs0 = []
    yerrs1 = []
    max_ys = None

    skip_tsc = True

    if not skip_tsc:
      factor_count = 7
      if cumulative:
        i_to_x = [6, 5, 4, 3, 2, 1, 0]
      else:
        i_to_x = [0, 6, 5, 4, 3, 2, 1]
    else:
      factor_count = 5
      if cumulative:
        i_to_x = [4, 3, 2, 1, 0, None, None]
      else:
        i_to_x = [None, 2, 3, 1, 0, None, None]

    for i in range(factor_count):
      df_conf = pd.DataFrame(df_alg)
      if cumulative:
        if i >= 1: df_conf = df_conf[df_conf['no_wsort'] == 1]
        else:      df_conf = df_conf[df_conf['no_wsort'].isnull()]
        if i >= 2: df_conf = df_conf[df_conf['no_preval'] == 1]
        else:      df_conf = df_conf[df_conf['no_preval'].isnull()]
        if i >= 3: df_conf = df_conf[df_conf['no_newest'] == 1]
        else:      df_conf = df_conf[df_conf['no_newest'].isnull()]
        if i >= 4: df_conf = df_conf[df_conf['no_wait'] == 1]
        else:      df_conf = df_conf[df_conf['no_wait'].isnull()]
        if i >= 5: df_conf = df_conf[df_conf['no_tscboost'] == 1]
        else:      df_conf = df_conf[df_conf['no_tscboost'].isnull()]
        if i >= 6: df_conf = df_conf[df_conf['no_tsc'] == 1]
        else:      df_conf = df_conf[df_conf['no_tsc'].isnull()]
      else:
        if i == 1: df_conf = df_conf[df_conf['no_wsort'] == 1]
        else:      df_conf = df_conf[df_conf['no_wsort'].isnull()]
        if i == 2: df_conf = df_conf[df_conf['no_preval'] == 1]
        else:      df_conf = df_conf[df_conf['no_preval'].isnull()]
        if i == 3: df_conf = df_conf[df_conf['no_newest'] == 1]
        else:      df_conf = df_conf[df_conf['no_newest'].isnull()]
        if i == 4: df_conf = df_conf[df_conf['no_wait'] == 1]
        else:      df_conf = df_conf[df_conf['no_wait'].isnull()]
        if i == 5: df_conf = df_conf[df_conf['no_tscboost'] == 1]
        else:      df_conf = df_conf[df_conf['no_tscboost'].isnull()]
        if i == 6: df_conf = df_conf[df_conf['no_tsc'] == 1]
        else:      df_conf = df_conf[df_conf['no_tsc'].isnull()]

      df_tput = df_conf['tput']
      # print(df_tput)

      y = df_tput.mean() / 1000000.
      yerr = [
        ((df_tput.mean() - df_tput.min()) / 1000000.).tolist(),
        ((df_tput.max() - df_tput.mean()) / 1000000.).tolist(),
      ]

      if not (skip_tsc and not cumulative and i == 0):
        xs.append(i_to_x[i])
        ys.append(y)
        yerrs0.append(yerr[0])
        yerrs1.append(yerr[1])

      if i == 0:
        max_ys = y

    color, linestyle, linewidth, marker, hatch = styles[alg + '_factor']

    # print(xs)
    # print(ys)
    # print(yerrs0)
    # print(yerrs1)

    # bar_width = 1. / (factor_count * 1.2)
    bar_width = 0.8
    bar_xs = np.array(xs) + idx * bar_width - (1 / 2.) * bar_width

    ax.bar(bar_xs, ys, bar_width, color=color, ecolor=color, zorder=5)
    e = ax.errorbar(bar_xs + bar_width / 2., ys, yerr=[yerrs0, yerrs1], ecolor='k', zorder=6, fmt='none')

  ax2.plot((-100., 100.), (100, 100), linewidth=2, color='k', linestyle='-', zorder=7, alpha=0.5)

  # ax.set_xlabel(r'Features')
  ax.set_ylabel(r'Throughput (M tps)')
  ax2.set_ylabel(r'Normalized throughput (\%)')

  ax.set_ylim(0, max_ys * 1.1)

  ax2.set_ylim(0, 100 * 1.1)
  ax2.set_yticks([0, 25, 50, 75, 100])

  margin = 0.6
  ax.set_xlim(-margin, (len(xs) - 1) + margin)

  ax.set_xticks(sorted(xs))

  if not skip_tsc:
    if cumulative:
      ax.set_xticklabels([
          'Base',
          '+\nTSC',
          '+\nboost',
          '+\nwait',
          '+\nnewest',
          '+\nprecheck',
          '+\nsort',
        ])
    else:
      ax.set_xticklabels([
          'Full',
          'w/o\nTSC',
          'w/o\nboost',
          'w/o\nwait',
          'w/o\nnewest',
          'w/o\nprecheck',
          'w/o\nsort',
        ])
  else:
    if cumulative:
      ax.set_xticklabels([
          'Base',
          '+wait',
          '+newest',
          '+precheck',
          '+sort',
        ])
    else:
      ax.set_xticklabels([
          'No wait',
          'No newest',
          'No sort',
          'No precheck',
        ])

  #ax.grid()
  ax.yaxis.grid(True, which='major', color='0.7', linestyle='-')
  plt.tight_layout()

  # # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.8, ncol=2)
  # legend = ax.legend(loc='best', fontsize=legend_fontsize, framealpha=0.0, ncol=2)
  # if legend is not None:
  #     legend.set_zorder(6)
  #     legend.get_frame().set_linewidth(0)
  plt.savefig('%s.pdf' % filename, bbox_inches='tight')
  plt.savefig('%s.png' % filename, dpi=dpi, bbox_inches='tight')
  plt.close(fig)
  plt.clf()
