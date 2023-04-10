loadData = LOAD 'input_avgTemp.txt' USING PigStorage(',') as (month:chararray, temp:float);
groupLoadData = GROUP loadData BY month;
groupMonth = FOREACH groupLoadData GENERATE group, AVG(loadData.temp);
STORE groupMonth INTO 'output' USING PigStorage(',');
DUMP groupMonth