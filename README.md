basho_bench_dist
================

Merge the results of several instances of Basho Bench. The Basho Bench (mdediana/basho\_bench) used is slightly modified, since it generates a single histogram for the whole run (i.e., no graphs in the end, only single figures for each metric) and exports it as a binary file. Later, basho\_bench\_dist may be used to merge all histograms, and generate consolidates summary.csv, errors.csv and \*\_latencies.csv. 
