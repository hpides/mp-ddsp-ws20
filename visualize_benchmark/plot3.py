import pandas
import sys
import os
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import numpy as np
import scipy.stats
import pathlib

big_font_size = 18
small_font_size = 16

matplotlib.rcParams['font.family'] = "CMU Serif"

params = {'xtick.labelsize':small_font_size,
          'ytick.labelsize':small_font_size}
pylab.rcParams.update(params)

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, h

def getAverageThroughput(filePath):
    df = pandas.read_csv(filePath)
    df.columns = ['ad_id', 'revenue', 'event_ts', 'process_ts', 'final_ts', 'row_count']
    df = df[df['ad_id'] != 0]
    #df = df[df['final_ts'] > df['final_ts'].min() + 5000]
    return (df['row_count'].sum() * 1000 * 2 / 5) / (df['final_ts'].max() - df['event_ts'].min())

def getNameFromFile(filePath):
    return os.path.splitext(os.path.basename(filePath))[0]

colorMap = ["#3193C6", "#05AD97", "#AAC56C", "#F7AB13", "#CD4E38", "#A553EC"]

filesArray = []

runtimes = []
means = []
errors = []

for tuples in ["180M", "360M", "720M", "1440M"]:
    filesArray.append([])
    for i in [1,2,3,4,5]:
        filesArray[-1].append("scale_out_" + tuples + "_n8_" + str(i) + ".csv")

for i, files in enumerate(filesArray):
	runtimes.append([]);
	for j, file in enumerate(files):
		runtimes[i].append(getAverageThroughput(file))
	mean, error = mean_confidence_interval(runtimes[i])
	means.append(mean)
	errors.append(error)

plt.figure(figsize=((3 / 2) * 8, 7))
plt.errorbar([1,2,4,8], means, yerr=errors, color = colorMap[0], linewidth=2, capsize=10)
plt.xticks([1,2,4,8],["180M", "360M", "720M", "1440M"])
plt.xlabel('Input size (in number of tuples)', fontsize=big_font_size)
plt.ylabel('Average throughput (in M tuples/s, 95% CI)', fontsize=big_font_size)
plt.ticklabel_format(style='sci', axis='y', scilimits=(6, 6))
plt.xticks(fontsize=small_font_size)
plt.yticks(fontsize=small_font_size)
plt.ylim((0,100000000))
plt.tight_layout()
plt.savefig(pathlib.Path("plot3.svg"), format="svg")
plt.close()
