#!/usr/bin/env python3
import sys

key = 1
for line in sys.stdin:
	line = line.strip()
	month, temp = line.split('\t')
	print("%s\t%s\t%s" %(key,month,temp))

