CA = LOAD '/home/hduser/Downloads/pda/pig/global.csv' using PigStorage(',') AS (eventid:INT,year:INT,month:INT,day:INT,extended:INT,country:INT,country_txt:chararray,region_txt:chararray,city:chararray,latitude:chararray,
longitude:chararray,vicinity:INT,doubter:INT,multiple:INT,success:INT,sucide:INT,attacktype:INT,attachtype:chararray,target:INT,targettype:chararray,
gname:chararray,individual:INT,nperpe:INT,npercap:INT,claimed:INT,weaptype:INT,weaptxt:chararray,wound:INT,property:INT,propvalue:INT,kid:INT,cntkil:INT);

task1 = FOREACH (GROUP CA by country_txt){
A = filter CA by region_txt == 'South Asia';
B = A.propvalue; 
sumr = SUM(B);
GENERATE FLATTEN(sumr), group AS country_txt;
};

STORE task1 INTO 'task1PIG' USING PigStorage (',');

