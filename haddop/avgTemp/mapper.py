#!/usr/bin/env python3
import sys

for line in sys.stdin:
	val = line.strip()
	month, temp = val.split('\t')
	print("%s\t%s" %(month, temp))
