# Visualize benchmark runs

This directory contains the python scripts we used for visualizing the benchmark results. 

## [compare.py](compare.py) 

This is used for displaying the developement of latency and throughput over time. As a command line options it takes a list of files, that are to be displayed. These files may be the output of one or mutltiple engines and runs (They all produce the same output format).

In the lower part of the script there are two ways to calculate the throughput. One simply groups everything by intervals of one second (since there should be a window every second), the other explicitly searches for different windows in the data and displays them. This is helpful, if some windows are delayed or need longer for processing, e.g. one window finishes at a runtime of 1,3 seconds, the next at 2,7 seconds and the next at 3.1 seconds. The simple logic would probably group two of these together and therefore produce a spike in the output graph.

## plot{1-5}.py

This are the python scripts producing the plots for our report and the final presentation.
