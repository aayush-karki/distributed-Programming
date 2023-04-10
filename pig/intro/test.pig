A = LOAD 'dummydata.txt' USING PigStorage(',') As (name:chararray, age:int, gpa:float);
--STORE A INTO 'output' USING PigStorage(',');
DUMP A

R = FILTER A BY (age >= 20) AND (age > 3.5);
--STORE A INTO 'output' USING PigStorage(',');
DUMP R

D = FOREACH A GENERATE age,gpa;
--STORE D INTO 'output' USING PigStorage(',');
DUMP D

E = GROUP A BY age;
--STORE E INTO 'output' USING PigStorage(',');
DUMP E

F = FOREACH E GENERATE group, A.name;
STORE F INTO 'output' USING PigStorage(',');
DUMP F
