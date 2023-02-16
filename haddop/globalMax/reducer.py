#!/usr/bin/env python3
import sys

maxTemp = {}
maxMonth = None
maxVal = 0

for line in sys.stdin:
	key,month,temp = line.strip().split('\t')
	
	try:
		temp = float(temp)
	except:
		continue

	try:
		if  temp > maxTemp[month]:
			maxTemp[month] = temp
		else:
			continue
	except:
		maxTemp[month] = temp

for key in maxTemp.keys():
	if maxTemp[key] > maxVal:
		maxMonth = key
		maxVal  = maxTemp[key] 


print("GLobal maxMonth: %s\nGlobal maxValue: %s" %(maxMonth, maxVal))
