#!/usr/bin/env python3
import sys
letter_count = {}
curr_letter = None
highest_count = 0
highest_count_letter = None

for line in sys.stdin:
        try:
                line = line.strip()
                letter, count = line.split()
                count = int(count)
        except:
                continue


        if letter == curr_letter:
                letter_count[letter] = letter_count[letter] + count
        else:
                curr_letter = letter
                letter_count[letter] = count

for letter in letter_count.keys():
        if(letter_count[letter] > highest_count):
                highest_count = letter_count[letter]
                highest_count_letter = letter

print("Highest letter occurence is %s with %s number of occurence" %(highest_count_letter, highest_count))