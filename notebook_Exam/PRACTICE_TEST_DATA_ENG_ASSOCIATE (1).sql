-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUESTION 20

-- COMMAND ----------

CREATE OR REPLACE TABLE raw_table(card_id STRING,itemss ARRAY<STRING>)

-- COMMAND ----------

describe extended raw_table

-- COMMAND ----------

INSERT INTO TABLE raw_table VALUES
(1,ARRAY("ABE","ANE","MOL"))

-- COMMAND ----------

SELECT * FROM raw_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### A junior data engineer has ingested a JSON file into a table raw_table with the following schema:
-- MAGIC ###### cart_id STRING,
-- MAGIC ###### items ARRAY<item_id:STRING>
-- MAGIC ###### The junior data engineer would like to unnest the items column in raw_table to result in a
-- MAGIC ###### new table with the following schema:
-- MAGIC ###### cart_id STRING,
-- MAGIC ###### item_id STRING
-- MAGIC ###### Which of the following commands should the junior data engineer run to complete this task?
-- MAGIC ######  A. SELECT cart_id, filter(items) AS item_id FROM raw_table;
-- MAGIC ######  B. SELECT cart_id, flatten(items) AS item_id FROM raw_table;
-- MAGIC ######  C. SELECT cart_id, reduce(items) AS item_id FROM raw_table;
-- MAGIC ######  D. SELECT cart_id, explode(items) AS item_id FROM raw_table;   X
-- MAGIC ######  E. SELECT cart_id, slice(items) AS item_id FROM raw_table;

-- COMMAND ----------

SELECT card_id, explode(itemss) as item_id FROM raw_table

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### QUESTION 21

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### A data engineer has ingested a JSON file into a table raw_table with the following schema: 
-- MAGIC ###### transaction_id STRING,
-- MAGIC ###### payload ARRAY<customer_id:STRING, date:TIMESTAMP, store_id:STRING>
-- MAGIC ###### The data engineer wants to efficiently extract the date of each transaction into a table with
-- MAGIC ###### the following schema:
-- MAGIC ###### transaction_id STRING,
-- MAGIC ###### date TIMESTAMP
-- MAGIC ###### Which of the following commands should the data engineer run to complete this task?
-- MAGIC ######  A. SELECT transaction_id, explode(payload) FROM raw_table;
-- MAGIC ######  B. SELECT transaction_id, payload.date FROM raw_table;   X
-- MAGIC ######  C. SELECT transaction_id, date FROM raw_table;
-- MAGIC ######  D. SELECT transaction_id, payload[date] FROM raw_table;
-- MAGIC ######  E. SELECT transaction_id, date from payload FROM raw_table;

-- COMMAND ----------

CREATE OR REPLACE TABLE raw_table1(transaction_id STRING, payload STRUCT<customer_id:STRING, date:DATE, store_id:STRING>);
INSERT INTO TABLE raw_table1 VALUES (1,("abe",'2022-08-03',"df"));
SELECT * FROM raw_table1;

-- COMMAND ----------

SELECT transaction_id, payload.date FROM raw_table1;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, DateType
-- MAGIC from pyspark.sql.functions import col, to_date
-- MAGIC values=sc.parallelize([(3,'2012-02-02'),(5,'2018-08-08')])
-- MAGIC rdd = values.map(lambda t: Row(A=t[0],date=t[1]))
-- MAGIC 
-- MAGIC # Importing date as String in Schema
-- MAGIC schema = StructType([StructField('A', IntegerType(), True), StructField('date', StringType(), True)])
-- MAGIC df = sqlContext.createDataFrame(rdd, schema)
-- MAGIC 
-- MAGIC # Finally converting the string into date using to_date() function.
-- MAGIC df = df.withColumn('date',to_date(col('date'), 'yyyy-MM-dd'))
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumn("week_later", col("date")+7)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC combiner dex tables par exemple achats en novembre et decembre de sorte qu'il n'y ait pas de doublons 

-- COMMAND ----------

CREATE OR REPLACE TABLE achatnov(i_prod INT, quantite INT);
INSERT INTO TABLE achatnov VALUES
(1,34),(2,55),(4,45);
SELECT * FROM achatnov

-- COMMAND ----------

CREATE OR REPLACE TABLE achatdec(i_prod INT, quantite INT);
INSERT INTO TABLE achatdec VALUES
(1,34),(2,55),(5,40);
SELECT * FROM achatdec

-- COMMAND ----------

(SELECT * FROM achatnov) UNION (SELECT * FROM achatdec)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("select * from achatnov")

-- COMMAND ----------

DELETE FROM achatnov WHERE i_prod = 1

-- COMMAND ----------

DESCRIBE EXTENDED achatnov 

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE vente(id INT, date DATE);
insert into table vente values 
(1,'2022-01-01'),
(2,'2023-01-01');
select * from vente

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### **DLT EXPECTATIONS**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### **CONSTRAINT ..... EXPECT**
-- MAGIC - Records violate expectations are added to target table.
-- MAGIC - Flagged in invalid in event log.
-- MAGIC - Pipeline continiues.

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE orders_valid
(CONSTRAINT valid_timestamp EXPECT(timestamp > '2022-01-01'))
SELECT * FROM LIVE.orders_vw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect("valid timestamp","timestamp>'2012-01-01'")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### **CONSTRAINT ..... EXPECT ON VIOLATION DROP ROW**
-- MAGIC - Records violate expectation are dropped from target table.
-- MAGIC - Flagged invalid in event log
-- MAGIC - Pipline Continues

-- COMMAND ----------

CREATE OR RAPLCE LIVE TABLE orders_valid
(CONSTRAINT valid_timestemp EXPECT(timestamp > '20206-01-01') ON VIOLATION DROP ROW)
SELECT * FROM LIVE.orders_vw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect_or_drop("valid timestamp > '2012-01-01'")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### **CONSTRAINT ..... EXPECT ON VIOLATION FAIL**
-- MAGIC - Records violate expectation cause the Job fail

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE orders_valid
(CONSTRAINT valid_timestamp EXPECT(timestamp > '2020-01-01')ON VIOLATION FAIL)
SELECT * FROM LIVE.orders_vw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect_or_fail("valid timestamp","timestamp > '2012-01-01'")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### **UDF FONCTION**

-- COMMAND ----------

CREATE FUNCTION udf_convert(temp DOUBLE, measure STRING)
RETURNS DOUBLE
RETURN CASE WHEN measure == 'F' THEN (temp * 9/5) + 32 
        ELSE (temp - 33) * 5/9
      END

-- COMMAND ----------

CREATE OR REPLACE TABLE temperature(tem DOUBLE, measure STRING);
INSERT INTO TABLE temperature VALUES
(45.768,'F'),
(347,'F');
SELECT * FROM temperature

-- COMMAND ----------

SELECT udf_convert(tem,measure) as temp
FROM temperature

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - You were asked to identify number of times a temperature sensor exceed threshold temperature(100.00) by each device, each row contains 5 readings collected very 5 minutes
-- MAGIC - ===> On vous a demandé d'identifier le nombre de fois qu'un capteur de température dépasse le seuil de température (100.00) pour chaque dispositif, chaque ligne contient 5 lectures collectées pendant 5 minutes.

-- COMMAND ----------

CREATE OR REPLACE TABLE sensors(deviceId INT, deviceTemp Array<FLOAT>,dateTimeCalled TIMESTAMP);
INSERT INTO TABLE sensors VALUES
(1,ARRAY(99.00,99.00,99.00,100.10,100.9),'2021-10-10 10:10:00'),
(1,ARRAY(99.00,99.00,100.00,100.15,102),'2021-10-10 10:15:00'),
(1,ARRAY(99.00,99.00,100.00,100.20,101),'2021-10-10 10:10:20');
SELECT * FROM sensors

-- COMMAND ----------

SELECT deviceId,sum(size(filter(deviceTemp,i -> i > 100.00))) AS count
FROM sensors
GROUP BY deviceId

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC You are cuurently looking a table that contains data from an e-commerce platform, eachrow contains a list of items(item number) that were present in the cart, when the customer makes a change to the cart the entire information is saved as a separate list and appended to an existing list for the duration of the customer session,to identify all the items customer bought you have to make a unique list of items, you were asked to create a unique item's list that added to the cart by the user.

-- COMMAND ----------

CREATE OR REPLACE TABLE carts(cartId INT, items ARRAY<ARRAY<INT>>);
INSERT INTO TABLE carts VALUES
(1,ARRAY(ARRAY(1,100,200,300),ARRAY(1,250,300))),
(2,ARRAY(ARRAY(10,150,200,300),ARRAY(1,250,300),ARRAY(350)));
SELECT * FROM carts

-- COMMAND ----------

SELECT cartId,ARRAY_DISTINCT(FLATTEN(items)) FROM carts

-- COMMAND ----------

CREATE OR REPLACE TABLE cartss(cartId INT, items ARRAY<INT>);
INSERT INTO TABLE cartss VALUES
(1,ARRAY(1,100,200,300)),
(1,ARRAY(1,250,300));
SELECT * FROM cartss

-- COMMAND ----------

SELECT cartId,COLLECT_SET(items) 
FROM cartss
GROUP BY cartId

-- COMMAND ----------

SELECT cartId,ARRAY_UNION(COLLECT_SET(items))
FROM cartss
GROUP BY cartId

-- COMMAND ----------

SELECT array_union(array(1, 2, 2, 3), array(1, 3, 5));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You are working on IOT data where each device ha 5 reading in a array collected in Celsius, you were asked to convert each indivdual reading from Celsius to Fahrenheit, fill in the blank ith an appropriat function that be used in the scenario

-- COMMAND ----------

CREATE OR REPLACE TABLE device(deviceId INT, deviceTempC ARRAY<FLOAT>);
INSERT INTO TABLE device VALUES(1,ARRAY(25.00,26.00,25.00,26.00,27.00));
SELECT * FROM device

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Query a delta table in Pyspark DataFrame

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table(f"select * from device")
-- MAGIC        

-- COMMAND ----------

SELECT deviceId, TRANSFORM(deviceTempC, i -> (i * 9/5)+32) as deviceTempF
FROM device

-- COMMAND ----------

-- MAGIC %md
-- MAGIC which of the following array functions takes input column return unique list of values in an array ==> **COLLECT_SET**

-- COMMAND ----------

CREATE OR REPLACE TABLE toUnique(array_values ARRAY<STRING>);
INSERT INTO TABLE toUnique VALUES
(ARRAY("A","B")),
(ARRAY("A","B")),
(ARRAY("B","C")),
(ARRAY("B","C"));
SELECT COLLECT_SET(array_values) FROM toUnique

-- COMMAND ----------

create or replace table sales as 
select 1 as batchId ,
	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
  current_timestamp() as insertDate
union all 
select 2 as batchId ,
  from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
                'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
                current_timestamp() as insertDa

-- COMMAND ----------

CREATE OR REPLACE TABLE sales as 
SELECT 1 AS batchId ,
	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') AS performance,
  CURRENT_TIMESTAMP() AS insertDate

-- COMMAND ----------

SELECT * FROM sales

-- COMMAND ----------

CREATE OR REPLACE TABLE sales as 
SELECT 1 AS batchId ,
	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') AS performance,
  CURRENT_TIMESTAMP() AS insertDate
UNION ALL 
SELECT 2 AS batchId ,
  from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
                'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') AS performance,
                CURRENT_TIMESTAMP() AS insertDa

-- COMMAND ----------

SELECT * FROM sales

-- COMMAND ----------

SELECT performance AS total_sales From sales

-- COMMAND ----------

SELECT performance.sales AS total_sales FROM sales

-- COMMAND ----------

SELECT COLLECT_LIST(performance.sales) AS total_sales FROM sales

-- COMMAND ----------

SELECT FLATTEN(COLLECT_LIST(performance.sales)) as total_sales FROM sales

-- COMMAND ----------

SELECT aggregate(flatten(collect_list(performance.sales)), 0, (x, y) -> x + y) 
AS  total_sales FROM sales 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Convert timestamp in date format**

-- COMMAND ----------

CREATE OR REPLACE TABLE orders(
      orderId INT,
      orderTime timestamp,
      units INT);
INSERT INTO TABLE orders VALUES
(1,'2022-01-01T09:10:24',100),
(2,'2022-01-01T10:30:30',10);
SELECT * FROM orders;

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TABLE orders(
      orderId INT,
      orderTime TIMESTAMP,
      orderdate DATE GENERATED ALWAYS AS(CAST(ordertime as DATE)),
      units INT);
INSERT INTO TABLE orders VALUES
(1,'2022-01-01T09:10:24',100),
(2,'2022-01-01T10:30:30',10);
SELECT * FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - the technique structured streaming uses to create an end-to-end fault tolerance is **Checkpointing and idempotent sinks**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Bronze**
-- MAGIC   - Raw copy of ingested data.
-- MAGIC   - Replaces traditional data lake.
-- MAGIC   - provides efficient storage and quering of full, unprocessed history of data.
-- MAGIC   - No schema is applied at this layer.
-- MAGIC 
-- MAGIC **Silver**
-- MAGIC   - Reduces data storage complexity, latency, and redundency.
-- MAGIC   - Optimiqes ETL troughput and analytic query performance.
-- MAGIC   - Preserves grain of original data(without aggregation)
-- MAGIC   - Eliminates duplicate records.
-- MAGIC   - Production schema enforced.
-- MAGIC   - Data quality cheks, quarantine corrupt data.
-- MAGIC   
-- MAGIC **Gold**
-- MAGIC   

-- COMMAND ----------

CREATE USER FUNCTION udf_convert(temp DOUBLE, measure STRING)
RETURNS DOUBLE
RETURN CASE WHEN measure == 'F' THEN(temp*9/5) + 12
            ELSE (temp - 33) * 5/9
          END

-- COMMAND ----------

CREATE OR REPLACE TABLE tempera

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Sales team is looking to get a report on a measure number of units sold by date, below is the schema. Fill in the blank with the appropriate array function.
-- MAGIC - ===> L'équipe des ventes cherche à obtenir un rapport sur une mesure du nombre d'unités vendues par date, ci-dessous est le schéma. Remplissez le vide avec la fonction de tableau appropriée.

-- COMMAND ----------

CREATE OR REPLACE TABLE orders(orderDate  DATE, orderIds  ARRAY<INT>);
INSERT INTO TABLE orders VALUES
('2021-10-10',ARRAY(100,101,102,103,104)),
('2021-11-10',ARRAY(105,106,107,108,109));
SELECT * FROM orders;

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDetail(orderId  INT, unitsSold  INT, salesAmt DOUBLE);
INSERT INTO TABLE orderDetail VALUES
(100,10,100),
(100,15,200);
SELECT * FROM orderDetail;

-- COMMAND ----------

SELECT orderDate, SUM(unitsSold)
      FROM orderDetail od
JOIN (select orderDate, EXPLODE(orderIds) as orderId FROM orders) o
    ON o.orderId = od.orderId
GROUP BY orderDate


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC 
-- MAGIC # Create a dataframe with a column of arrays
-- MAGIC df = spark.createDataFrame([(1, [1, 2, 3]), (2, [4, 5, 6])], ["id", "array_col"])
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use the explode function to transform the array column into multiple rows
-- MAGIC exploded_df = df.select("id", explode("array_col").alias("element"))
-- MAGIC 
-- MAGIC exploded_df.show()

-- COMMAND ----------

CREATE OR REPLACE TABLE sample(deviceId INT,deviceTemp ARRAY<DOUBLE>, dateTimeCollected TIMESTAMP);
INSERT INTO TABLE sample VALUES
(1,ARRAY(99.00,99.00,99.00,100.10,100.9),'2021-10-10 10:10:00'),
(1,ARRAY(99.00,99.00,100.00,100.15,102),'2021-10-10 10:15:00'),
(1,ARRAY(99.00,99.00,100.00,100.20,101),'2021-10-10 10:20:00');

SELECT * FROM sample


-- COMMAND ----------

SELECT deviceId, FILTER(deviceTemp, i -> i > 100.00)  as temp_sup_100 FROM sample 

-- COMMAND ----------

SELECT deviceId, SIZE(FILTER(deviceTemp, i -> i > 100.00))  as temp_sup_100 FROM sample 

-- COMMAND ----------

SELECT deviceId, SUM(SIZE(FILTER(deviceTemp, i -> i > 100.00))) as count

FROM sample

GROUP BY deviceId

-- COMMAND ----------

CREATE OR REPLACE TABLE tempt(deviceId INT,deviceTempC ARRAY<DOUBLE>);
INSERT INTO TABLE tempt VALUES(1,ARRAY(25.00,26.00,25.00,26.00,27.00));
SELECT * FROM tempt;

-- COMMAND ----------



-- COMMAND ----------

SELECT deviceId, TRANSFORM(deviceTempC,i-> (i * 9/5) + 32) as deviceTempF
FROM tempt

-- COMMAND ----------

SELECT deviceId, COLLECT_SET(deviceTempC) FROM tempt
GROUP BY deviceId

-- COMMAND ----------

CREATE OR REPALCE TABLE sample_data()
