#!/usr/bin/env python3
import sys

maxTemp = {}

for line in sys.stdin:
	line = line.strip()
	key, val = line.split('\t')
	# converting the string temp to int
	try:
		val = int(val)
	except ValueError:
		continue
	# comparing the value with existing value if it exists
	try:
		if val > maxTemp[key]:
			maxTemp[key] = val
		else:
			continue
	except:
		maxTemp[key] = val;

for key in maxTemp.keys():
	print('%s\t%s' %(key, maxTemp[key]))
