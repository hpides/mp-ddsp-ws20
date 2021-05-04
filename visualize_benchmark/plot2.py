import pandas
import sys
import os
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
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

colorMap = ["#3193C6", "#05AD97", "#AAC56C", "#F7AB13", "#CD4E38", "#A553EC"]

filesArray = [["distributed_string_engine_n1_0.csv", "distributed_string_engine_n1_1.csv", "distributed_string_engine_n1_2.csv", "distributed_string_engine_n1_3.csv", "distributed_string_engine_n1_4.csv", "distributed_string_engine_n1_5.csv"],
["distributed_string_engine_n2_1.csv", "distributed_string_engine_n2_2.csv", "distributed_string_engine_n2_3.csv", "distributed_string_engine_n2_4.csv", "distributed_string_engine_n2_5.csv"],
["distributed_string_engine_n4_1.csv", "distributed_string_engine_n4_2.csv", "distributed_string_engine_n4_3.csv", "distributed_string_engine_n4_4.csv", "distributed_string_engine_n4_5.csv"],
["distributed_string_engine_n8_1.csv", "distributed_string_engine_n8_2.csv", "distributed_string_engine_n8_3.csv", "distributed_string_engine_n8_4.csv", "distributed_string_engine_n8_5.csv"]]
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

plt.figure(figsize=((3 / 2) * 8, 7))
plt.errorbar([1,2,4,8], means, yerr=errors, color = colorMap[0], linewidth=2, capsize=10, label = "Observed runtime (95% CI)")
plt.plot([1,2,4,8], [means[0], means[0]/2, means[0]/4, means[0]/8], color = colorMap[3], label = "Expected linear scaling")
plt.xticks([1,2,4,8],[1,2,4,8])
plt.xlabel('Number of nodes', fontsize=big_font_size)
plt.ylabel('Runtime (in s)', fontsize=big_font_size)
plt.legend(loc='upper right', fontsize=small_font_size)
plt.xticks(fontsize=small_font_size)
plt.yticks(fontsize=small_font_size)
plt.tight_layout()
plt.savefig(pathlib.Path("plot2.svg"), format="svg")
plt.close()
