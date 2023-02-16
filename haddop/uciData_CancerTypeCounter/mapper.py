#!/usr/bin/env python3

import sys

for line in sys.stdin:
	line = line.strip()
	line = line.split(',')
	print("%s\t%s" %(line[1], 1))
