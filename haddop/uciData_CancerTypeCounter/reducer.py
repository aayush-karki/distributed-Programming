#!/usr/bin/env python3
import sys

count = {}

for line in sys.stdin:
        line = line.strip()
        key, value = line.split("\t")
        try:
                value = int(value)
        except ValueError:
                continue

        try:
                count[key] = count[key] + 1
        except:
                count[key] = 1

for key in count.keys():
        print("%s\t%s" %(key, count[key]) )

