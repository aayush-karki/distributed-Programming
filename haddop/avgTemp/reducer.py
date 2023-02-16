#!/usr/bin/env python3
import sys

avgTemp = {}
frequency = {} 


for line in sys.stdin:
	line = line.strip()
	key, val = line.split('\t')
	# converting the string temp to int
	try:
		val = int(val)
	except ValueError:
		continue
	# if the key exist in the dictinary then add the value
	try:
		avgTemp[key] += val
		frequency[key] += 1
	except:
		avgTemp[key] = val
		frequency[key] = 1

for key in avgTemp.keys():
	avgTemp[key] /= frequency[key]
	print('%s\t%s' %(key, avgTemp[key]))
