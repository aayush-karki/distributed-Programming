#!/usr/bin/env python3
import sys

avgTemp = {}
frequency = {}

for line in sys.stdin:
        line = line.strip()
        key,month, temp = line.split('\t')
        # converting the string temp to int
        try:
                temp = int(temp)
        except ValueError:
                continue
        # comparing the value with existing value if it exists
        try:
                if temp < avgTemp[key]:
                        avgTemp[key] = avgTemp[key] + temp
                        frequency[key] = frequency[key] + 1
                else:
                        continue
        except:
                avgTemp[key] = temp
                frequency[key] = 1

for key in avgTemp.keys():
        avgTemp[key] = avgTemp[key] / frequency[key]

for key in avgTemp.keys():
        print('%s\t%s' %(key, avgTemp[key]))