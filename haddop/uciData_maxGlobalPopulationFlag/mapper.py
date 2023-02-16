#!/usr/bin/env python3
import sys

key = 1
for line in sys.stdin:
	line = line.strip()
	list  = line.split(',')
	print("%s\t%s\t%s" %(key,list[0],list[4]))

