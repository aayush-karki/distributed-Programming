A = LOAD 'dummydata.txt' USING PigStorage(',') As (name:chararray, age:int, gpa:float)
STORE A INTO 'output' USING PigStorage(',')
DUMP A