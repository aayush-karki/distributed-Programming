#!/usr/bin/env python3
import sys

avgRadius = {}
frequency = {}

for line in sys.stdin:
        line = line.strip()
        key, radius = line.split("\t")
        try:
                radius = float(radius)
        except ValueError:
                continue

        try:
                avgRadius[key] = avgRadius[key] + radius
                frequency[key] = frequency[key] + 1
        except:
                avgRadius[key] = radius
                frequency[key] = 1

for key in avgRadius.keys():
        avgRadius[key] = avgRadius[key] / frequency[key]
        print("%s\t%s" %(key, avgRadius[key]) )

