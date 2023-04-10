lineData = LOAD 'wordcount.txt' As (line:chararray);
dump lineData

tokenizedWordData = FOREACH  lineData GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;
dump tokenizedWordData

wordGroup = GROUP tokenizedWordData BY word;
dump wordGroup

wordCount = FOREACH wordGroup GENERATE group, COUNT(tokenizedWordData);
STORE wordCount INTO 'output' USING PigStorage();
dump wordCount