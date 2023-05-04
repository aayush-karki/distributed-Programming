#!/usr/bin/env python3
import sys

# find the average tumor radius for malignant patient
# how many patient are diagonosed as benign but their 
#	tumor radius is more that avg

count_dict = {}
totalrad_dict = {}
count = 0
benign = []

for line in sys.stdin:
    line = line.strip()
    key, outcome, radius = line.split("\t")
    radius = float(radius)

    try:
        count_dict[outcome] += key
        totalrad_dict[outcome] += radius

    except:
        count_dict[outcome] = key
        totalrad_dict[outcome] = radius

    if outcome == "B":
        benign.append(radius)

avg = {}
for k in totalrad_dict.keys():
    avg[k] = totalrad_dict[k] / count_dict[k]
print("%s\t%s", avg['M'])


for k in benign:
    if k > avg:
        count +=1
print(count)