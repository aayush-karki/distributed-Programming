#!/usr/bin/env python3
import sys
word_count = {}
curr_word = None

'''
wordcount reducer.py
'''

for line in sys.stdin:
        try:
                line = line.strip()
                word, count = line.split()
                count = int(count)
        except:
                continue


        if word == curr_word:
                word_count[word] = word_count[word] + count
        else:
                curr_word = word
                word_count[word] = count

for word in word_count.keys():
        print(word, word_count[word])
