import pandas
import sys
import os
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import pathlib

big_font_size = 18
small_font_size = 16

matplotlib.rcParams['font.family'] = "CMU Serif"

params = {'xtick.labelsize':small_font_size,
          'ytick.labelsize':small_font_size}
pylab.rcParams.update(params)

filesArray = []
for i in [75,100,112,125,150]:
    filesArray.append(str(i) + "MBs.csv")

def readAndPreprocessFile(filePath):
    df = pandas.read_csv(filePath)
    df.columns = ['ad_id', 'revenue', 'event_ts', 'process_ts', 'final_ts', 'row_count']
    df = df[df['ad_id'] != 0]
    df['elapsed'] = df['final_ts'] - df['event_ts'].min()
    df = df[df['process_ts'] > df['process_ts'].min() + 4000]
    print((df['final_ts'].max() - df['event_ts'].min()) / 1000)
    return df

def getRuntime(filePath):
    df = pandas.read_csv(filePath)
    df.columns = ['ad_id', 'revenue', 'event_ts', 'process_ts', 'final_ts', 'row_count']
    df = df[df['ad_id'] != 0]
    return (df['final_ts'].max() - df['event_ts'].min()) / 1000

def getNameFromFile(filePath):
    return os.path.splitext(os.path.basename(filePath))[0]

colorMap = ["#3193C6", "#05AD97", "#AAC56C", "#F7AB13", "#CD4E38", "#A553EC"]

dataframes = []
nameArray = ["160", "220", "250", "280", "340"]

for file in filesArray:
    df = readAndPreprocessFile(file)
    df = df.sort_values(by=['elapsed'])
    df = df.reset_index(drop=True)
    df['window'] = 5
    window = 5
    adIdSet = set()
    for (i, row) in df.iterrows():
        if (row['ad_id'] in adIdSet):
            adIdSet = set()
            adIdSet.add(row['ad_id'])
            window = window + 1
        else:
            adIdSet.add(row['ad_id'])
        df.loc[i, 'window'] = window
    dataframes.append(df)

plt.figure(figsize=((3 / 2) * 8, 7))
for i, file in enumerate(filesArray):

    df = dataframes[i]
    name = getNameFromFile(file)

    df['latency_event'] = df['final_ts'] - df['event_ts']
    dfGroupedEvent = df[['window', 'latency_event']].groupby(['window'], as_index = False).min(['latency_event'])
    plt.plot(dfGroupedEvent['window'], dfGroupedEvent['latency_event'], color = colorMap[i], linewidth=2, label= nameArray[i] + 'MB/s')

# General stuff for latency diagramm
plt.xlabel('Number of windows', fontsize=big_font_size)
plt.ylabel('Minimum latency per window (in ms)', fontsize=big_font_size)
plt.legend(loc='upper left', fontsize=small_font_size)
plt.tick_params(axis='both', which='major', labelsize=small_font_size)
plt.xticks(fontsize=small_font_size)
plt.yticks(fontsize=small_font_size)
plt.tight_layout()
plt.savefig(pathlib.Path("plot4_1.svg"), format="svg")
plt.close()

groupByCol = 'window'

plt.figure(figsize=((3 / 2) * 8, 7))
print("Printing throughput graph")
for index, file in enumerate(filesArray):

    df = dataframes[index]

    dfGrouped = df[[groupByCol, 'row_count']].groupby([groupByCol], as_index = False).sum(['row_count'])

    plt.plot(dfGrouped[groupByCol], dfGrouped['row_count'] * 2 / 5, color = colorMap[index], linewidth=2, label= nameArray[index] + 'MB/s')
    # *2 for two stream, /5 for five output-windows per ingested tuple

# General stuff for throughput diagramm
plt.xlabel('Number of windows', fontsize=big_font_size)
plt.ylabel('Throughput in M tuples per window', fontsize=big_font_size)
plt.legend(loc='upper right', fontsize=small_font_size)
plt.ticklabel_format(style='sci', axis='y', scilimits=(6, 6))
plt.tick_params(axis='both', which='major', labelsize=small_font_size)
plt.ylim((0,None))
plt.tight_layout()
plt.xticks(fontsize=small_font_size)
plt.yticks(fontsize=small_font_size)
plt.savefig(pathlib.Path("plot4_2.svg"), format="svg")
plt.close()
