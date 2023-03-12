#!/usr/bin/env python3
import sys

minTemp = {}
minMonth = {}
coldestPlace = None
coldestTemp = 0

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
                if temp < minTemp[key]:
                        minTemp[key] = temp
                        minMonth[key] = month
                else:
                        continue
        except:
                minTemp[key] = temp
                minMonth[key] = month

for key in minTemp.keys():
        if(coldestPlace == None or minTemp < coldestTemp):
                coldestPlace = key
                coldestTemp = minTemp[key]

print('%s\t%s\t%s' %(coldestPlace, minMonth[coldestPlace], minTemp[coldestPlace]))