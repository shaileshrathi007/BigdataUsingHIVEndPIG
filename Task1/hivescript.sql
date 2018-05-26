SET mapreduce.framework.name = local;
SET mapred.local.dir=/home/hduser/tmp/mapred/local;

DROP TABLE data;  
 
CREATE TABLE data(eventid INT,year INT,month INT,day INT,extended_data INT,country INT,country_name STRING,
region STRING,city STRING,latitude STRING, longitude STRING, vicinity INT,doubt_blast INT, multiple_attack INT, success_rate INT,no_of_terrist_sucide INT, attacktype INT, desc_attacktype string, target_id INT, desc_target string, gang_name string, individual INT, cnt_terriorist INT,
ready_claimed int, weapentype INT, desc_weapon string, cnt_wound INT, type_property INT, total_property INT, kid INT, cnt_kill INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


LOAD data local inpath '/home/hduser/Downloads/pda/hive/globala.txt' into table data;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/Downloads/pda/hive/Task1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select  region,case when region = 'Australasia' then sum(total_property)
		    when region = 'Central America' then sum(total_property)
		    when region = 'Central Asia' then sum(total_property)
		    when region = 'East Asia' then sum(total_property)
		    when region = 'Eastern Europe' then sum(total_property)
	 	    when region = 'Middle East' then sum(total_property)
                    when region = 'North America' then sum(total_property)
		    when region = 'South America' then sum(total_property)
                    when region = 'South Asia' then sum(total_property)
                    when region = 'Southeast Asia' then sum(total_property)
		    when region = 'Sub-Saharan Africa' then sum(total_property)
		    when region = 'Western Europe' then sum(total_property)			
	    else '0' END
from data
group by region;


INSERT overwrite local directory '/home/hduser/Downloads/pda/hive/Task2'
row format delimited fields terminated by ','
SELECT year,country_name,city,desc_attacktype,gang_name,count(cnt_terriorist),count(cnt_wound),sum(total_property) from data
where country_name = 'India' 
group by year,country_name,desc_attacktype,gang_name,city;





