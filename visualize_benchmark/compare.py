import pandas
import sys
import os
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, LinearSegmentedColormap

matplotlib.rcParams['font.family'] = "CMU Serif"

filesArray = sys.argv[1:]

def readAndPreprocessFile(filePath):
    print("Processing file " + filePath)
    df = pandas.read_csv(filePath)
    df.columns = ['ad_id', 'revenue', 'event_ts', 'process_ts', 'final_ts', 'row_count']
    df = df[df['ad_id'] != 0]
    #df = df[df['final_ts'] > df['final_ts'].min() + 5000]
    df['elapsed'] = df['final_ts'] - df['event_ts'].min()
    print(sum(df['row_count']))
    return df

def getNameFromFile(filePath):
    return os.path.splitext(os.path.basename(filePath))[0]

colorMap = cm.get_cmap('Set1')

print("Printing Latency Graph")
for i, file in enumerate(filesArray):

    df = readAndPreprocessFile(file)
    name = getNameFromFile(file)

    df['latency_event'] = df['final_ts'] - df['event_ts']
    df['latency_process'] = df['final_ts'] - df['process_ts']
    dfGroupedEvent = df[['elapsed', 'latency_event']].groupby(['elapsed'], as_index = False).min(['latency_event'])
    dfGroupedProcess = df[['elapsed', 'latency_process']].groupby(['elapsed'], as_index = False).min(['latency_process'])

    plt.plot(dfGroupedEvent['elapsed'], dfGroupedEvent['latency_event'], color = colorMap(i), linewidth=2, label= name + ': event time latency')
    plt.plot(dfGroupedProcess['elapsed'], dfGroupedProcess['latency_process'], linestyle = "--", color = colorMap(i), linewidth=2, label= name + ': processing time latency')

# General stuff for latency diagramm
plt.title('Latency over time', fontsize=16)
plt.xlabel('Time (in ms)', fontsize=14)
plt.ylabel('Latency (in ms)', fontsize=14)
plt.legend(loc='upper left', fontsize=12)
plt.tick_params(axis='both', which='major', labelsize=12)
plt.show()

groupByCol = 'elapsed_group'
print("Printing throughput graph")
for index, file in enumerate(filesArray):

    df = readAndPreprocessFile(file)
    name = getNameFromFile(file)

    df = df.sort_values(by=['elapsed'])
    df = df.reset_index(drop=True)
    #df['window'] = 0
    adIdSet = set()
    #for i in range(1,len(df)):
    #    if (df.loc[i, 'ad_id'] in adIdSet):
    #        adIdSet = set()
    #        adIdSet.add(df.loc[i, 'ad_id'])
    #        df.loc[i, 'window'] = df.loc[i-1, 'window'] + 1
    #    else:
    #        df.loc[i, 'window'] = df.loc[i-1, 'window']
    #        adIdSet.add(df.loc[i, 'ad_id'])


    #print('Number of windows found: ' + str(df['window'].max()))
    df['elapsed_group'] = (df['elapsed'] - df['elapsed'] % 1000)
    dfGrouped = df[[groupByCol, 'row_count']].groupby([groupByCol], as_index = False).sum(['row_count'])

    plt.plot(dfGrouped[groupByCol], dfGrouped['row_count'] * 2 / 5, color = colorMap(index), linewidth=2, label= name + ': throughput')
    # *2 for two stream, /5 for five output-windows per ingested tuple

# General stuff for throughput diagramm
plt.title('Throughput over time', fontsize=16)
plt.xlabel('# of windows', fontsize=14)
plt.ylabel('Throughput (in Tuples/sec)', fontsize=14)
plt.legend(loc='upper right', fontsize=12)
plt.ticklabel_format(style='sci', axis='y', scilimits=(6, 6))
plt.tick_params(axis='both', which='major', labelsize=12)
plt.show()
