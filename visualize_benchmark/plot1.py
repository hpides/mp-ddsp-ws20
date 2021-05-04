import pandas
import sys
import os
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.container as pycontainer
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import numpy as np
import scipy.stats
import pathlib

big_font_size = 18
small_font_size = 16

matplotlib.rcParams['font.family'] = "CMU Serif"

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, h

def getRuntime(filePath):
    df = pandas.read_csv(filePath)
    df.columns = ['ad_id', 'revenue', 'event_ts', 'process_ts', 'final_ts', 'row_count']
    df = df[df['ad_id'] != 0]
    return (df['final_ts'].max() - df['event_ts'].min()) / 1000

def getNameFromFile(filePath):
    return os.path.splitext(os.path.basename(filePath))[0]

colorMap = cm.get_cmap('Set1')

filesArray = []

for tuples in ["flink_n1_", "operator_engine_n1_", "distributed_hardcoded_streaming_engine_n1_", "distributed_string_engine_n1_"]:
    filesArray.append([])
    for i in [1,2,3,4,5]:
        filesArray[-1].append(tuples + str(i) + ".csv")

runtimes = []
means = []
errors = []
for i, files in enumerate(filesArray):
	runtimes.append([]);
	for j, file in enumerate(files):
		runtimes[i].append(getRuntime(file))
	mean, error = mean_confidence_interval(runtimes[i])
	means.append(mean)
	errors.append(error)

x_labels = "Apache Flink", "Iterator-style engine", "Hard-coded query", "Query-compiled engine"

df = pandas.DataFrame({"y_values": means})

plt.figure(figsize=((3 / 2) * 8, 7))

df["y_values"].plot(kind='bar', yerr=errors, align='center', capsize=10, zorder=0, color = ["#3193C6", "#05AD97", "#AAC56C", "#F7AB13", "#CD4E38", "#A553EC"])

for i, rows in df.iterrows():
    plt.annotate(round(rows["y_values"]), xy=(i - 0.15, rows["y_values"] + 2.75), rotation=0, zorder=5, fontsize=small_font_size, ha="center", )

plt.xticks([0,1,2,3], x_labels)
plt.xticks(rotation=0, ha='center')
plt.xlabel('Type of SPE', fontsize=big_font_size)
plt.ylabel('Runtime (in s, 95% CI)', fontsize=big_font_size)
plt.xticks(fontsize=small_font_size)
plt.yticks(fontsize=small_font_size)

plt.tight_layout()
plt.savefig(pathlib.Path("plot1.svg"), format="svg")
plt.close()
