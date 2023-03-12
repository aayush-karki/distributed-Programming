#!/usr/bin/env python3
import sys

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(",")
    # increase counters
    for word in words:
        # if the word is whitespace then ignore
        if(word.isspace()):
                continue

        # write the results to STDOUT (standard output);
        print(word, 1)
