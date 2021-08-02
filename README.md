


### SETUP:

Data was saved in a subfolder of the local home directory as 6 seperate txt files. The entire folder was moved to HDFS with the following command:
hdfs dfs -cp p1/ /user/devin

The data provided fell into two categories: Branch data, and Consumer data. Each category had 3 associated .txt files.

A table was prepared to recieve the branch data with the following command in Hive:

CREATE TABLE branch (product STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;

That table was populated with these commands, which collected all 3 branch .txt files into one table:

LOAD DATA INPATH '/user/devin/p1/Bev_BranchA.txt' INTO TABLE branch;

LOAD DATA INPATH '/user/devin/p1/Bev_BranchB.txt' INTO TABLE branch;

LOAD DATA INPATH '/user/devin/p1/Bev_BranchC.txt' INTO TABLE branch;

Another table was prepared to recieve the consumer data with the following command:
CREATE TABLE consumers (product STRING, consumers INT) row format delimited fields terminated by ',' stored as textfile;

That table was populated with these commands:
LOAD DATA INPATH '/user/devin/p1/Bev_ConscountA.txt' INTO TABLE consumers;
LOAD DATA INPATH '/user/devin/p1/Bev_ConscountB.txt' INTO TABLE consumers;
LOAD DATA INPATH '/user/devin/p1/Bev_ConscountC.txt' INTO TABLE consumers;

Once the data was loaded into the tables, it was easy to query. Note that the decision to collect the data into two tables, rather than a separate table for each .txt file, was made to make querying data simpler. The ability to do this is one of the advantages of hive.

###Problem Scenario 1
####What is the total number of consumers for Branch1?

SELECT SUM(c.consumers) FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch1";

RETURNS 1115974

####What is the number of consumers for the Branch2?

SELECT SUM(c.consumers) FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch2";

RETURNS 5099141


###Problem Scenario 2
####What is the most consumed beverage on Branch1

SELECT c.product, SUM(c.consumers) total FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch1"
GROUP BY c.product
ORDER BY total DESC
LIMIT 1;

RETURNS  Special_cappuccino  | 108163

####What is the least consumed beverage on Branch2

SELECT c.product, SUM(c.consumers) total FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch2"
GROUP BY c.product
ORDER BY total ASC
LIMIT 1;

RETURNS Cold_MOCHA  | 47524

####What is the Average consumed beverage of  Branch2

WITH t1 AS (SELECT sum(c.consumers) tot FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch2"
GROUP BY c.product)
SELECT AVG(t1.tot) FROM t1;

RETURNS 99983.1568627451


###Problem Scenario 3
####What are the beverages available on Branch10, Branch8, and Branch1?

SELECT DISTINCT(product) FROM branch
WHERE branch = "Branch10" OR branch = "Branch8" OR branch = "Branch1";
| Cold_Coffee         |
| Cold_LATTE          |
| Cold_Lite           |
| Cold_cappuccino     |
| Double_Coffee       |
| Double_Espresso     |
| Double_LATTE        |
| Double_MOCHA        |
| Double_cappuccino   |
| ICY_Coffee          |
| ICY_Espresso        |
| ICY_Lite            |
| ICY_MOCHA           |
| ICY_cappuccino      |
| LARGE_Coffee        |
| LARGE_Espresso      |
| LARGE_MOCHA         |
| LARGE_cappuccino    |
| MED_Coffee          |
| MED_Espresso        |
| MED_LATTE           |
| MED_MOCHA           |
| MED_cappuccino      |
| Mild_Coffee         |
| Mild_Espresso       |
| Mild_LATTE          |
| Mild_Lite           |
| Mild_cappuccino     |
| SMALL_Espresso      |
| SMALL_LATTE         |
| SMALL_Lite          |
| SMALL_MOCHA         |
| Special_Coffee      |
| Special_Espresso    |
| Special_LATTE       |
| Special_Lite        |
| Special_MOCHA       |
| Special_cappuccino  |
| Triple_Coffee       |
| Triple_Espresso     |
| Triple_LATTE        |
| Triple_Lite         |
| Triple_MOCHA        |
| Triple_cappuccino   |


####what are the comman beverages available in Branch4,Branch7?

WITH b4 AS (SELECT product FROM branch
WHERE branch = "Branch4"),
b7 AS (SELECT product FROM branch
WHERE branch = "Branch7")
SELECT DISTINCT(b4.product) product FROM b4
INNER JOIN b7
ON b4.product = b7.product;

| Cold_Coffee         |
| Cold_Espresso       |
| Cold_LATTE          |
| Cold_Lite           |
| Cold_MOCHA          |
| Cold_cappuccino     |
| Double_Coffee       |
| Double_Espresso     |
| Double_Lite         |
| Double_MOCHA        |
| Double_cappuccino   |
| ICY_Coffee          |
| ICY_LATTE           |
| ICY_Lite            |
| ICY_MOCHA           |
| ICY_cappuccino      |
| LARGE_Coffee        |
| LARGE_Espresso      |
| LARGE_LATTE         |
| LARGE_Lite          |
| LARGE_MOCHA         |
| LARGE_cappuccino    |
| MED_Coffee          |
| MED_Espresso        |
| MED_Lite            |
| MED_MOCHA           |
| MED_cappuccino      |
| Mild_Coffee         |
| Mild_Espresso       |
| Mild_LATTE          |
| Mild_Lite           |
| Mild_MOCHA          |
| Mild_cappuccino     |
| SMALL_Coffee        |
| SMALL_Espresso      |
| SMALL_LATTE         |
| SMALL_Lite          |
| SMALL_MOCHA         |
| SMALL_cappuccino    |
| Special_Coffee      |
| Special_Espresso    |
| Special_LATTE       |
| Special_Lite        |
| Special_MOCHA       |
| Special_cappuccino  |
| Triple_Coffee       |
| Triple_Espresso     |
| Triple_LATTE        |
| Triple_Lite         |
| Triple_MOCHA        |
| Triple_cappuccino   |
+---------------------+

###Problem Scenario 4
####create a partition,index,View for the scenario3.

#####VIEW =================

CREATE VIEW view1 AS
SELECT DISTINCT(b4.product) product FROM (SELECT product FROM branch
WHERE branch = "Branch4") AS b4
INNER JOIN
(SELECT product FROM branch
WHERE branch = "Branch7") AS b7
ON b4.product = b7.product;

#####PARTITION ==============

CREATE TABLE branch (product STRING) PARTITIONED BY (branch STRING) row format delimited fields terminated by ',' stored as textfile;
LOAD DATA INPATH '/user/devin/p1/Bev_BranchA.txt' INTO TABLE branch;
LOAD DATA INPATH '/user/devin/p1/Bev_BranchB.txt' INTO TABLE branch;
LOAD DATA INPATH '/user/devin/p1/Bev_BranchC.txt' INTO TABLE branch;


###Problem Scenario 5
####Alter the table properties to add "note","comment"
ALTER TABLE branch SET TBLPROPERTIES("note" = "comment");
SHOW TBLPROPERTIES branch;
+------------------------+-------------+
|       prpt_name        | prpt_value  |
+------------------------+-------------+
| bucketing_version      | 2           |
| last_modified_by       | devin       |
| last_modified_time     | 1627419197  |
| note                   | comment     |
| numFiles               | 3           |
| numRows                | 0           |
| rawDataSize            | 0           |
| totalSize              | 12995       |
| transient_lastDdlTime  | 1627419197  |
+------------------------+-------------+

###Problem Scenario 6
####Remove the row 5 from the output of any scenario
SCENARIO 1 - MOST PURCHASED BRANCH 1 BEVERAGE - 5th MOST PURCHASED.

WITH t1 AS (SELECT ROW_NUMBER() OVER (ORDER BY b.product) as row_number, b.product, SUM(c.consumers)  FROM branch b
LEFT JOIN consumers c
ON b.product = c.product
WHERE b.branch = "Branch1"
GROUP BY b.product)
SELECT * FROM t1 WHERE t1.row_number = 5;
+----------------+---------------+---------+
| t1.row_number  |  t1.product   | t1._c2  |
+----------------+---------------+---------+
| 5              | ICY_Espresso  | 50820   |
+----------------+---------------+---------+



