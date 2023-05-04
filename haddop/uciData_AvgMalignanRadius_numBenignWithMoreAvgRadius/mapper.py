#!/usr/bin/env python3

# find the average tumor radius for malignant patient
# how many patient are diagonosed as benign but their 
#	tumor radius is more that avg

import sys

for line in sys.stdin:
	line = line.strip()
	line = line.split(',')
	print("%s\t%s" %(line[1], line[2]))
