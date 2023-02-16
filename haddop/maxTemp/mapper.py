
#!/usr/bin/env python3
import sys

maxPopulation = {}
maxCountry = None
maxVal = 0

for line in sys.stdin:
        key,country,population = line.strip().split('\t')

        try:
                population = float(population)
        except:
                continue

        try:
                if  population > maxPopulation[population]:
                        maxPopulation[country] = population
                else:
                        continue
        except:
                maxPopulation[country] = population

for key in maxPopulation.keys():
        if maxPopulation[key] > maxVal:
                maxCountry = key
                maxVal  = maxPopulation[key] 


print("GLobal maxPopulationCountry: %s\nGlobal maxPopulationValue: %s" %(maxCountry, maxVal))