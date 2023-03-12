#!/usr/bin/env python3
import sys

for line in sys.stdin:
        line = line.strip()
        place,month,temp = line.split(',')
        print("%s\t%s\t%s" %(place,month,temp))
