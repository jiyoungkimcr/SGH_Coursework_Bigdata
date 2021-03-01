-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ####Flights Exercises - 2020-11-29
-- MAGIC #####_Code by: Jiyoung Kim_
-- MAGIC 
-- MAGIC Import flights.csv, airlines.csv,airports.csv and do the following exercises using Spark SQL:
-- MAGIC 
-- MAGIC 1. Find out which airports when treated as Origin have the smallest and the largest Departure delays.
-- MAGIC 2. Create a list of U.S. States with the number of airports that each of them has in descending order. Show this on a map of the U.S.
-- MAGIC 3. Create a list containing: DATE, NUM_OF_FLIGHTS, MAX_ARRIVAL_DELAY, AVG_ARRIVAL_DELAY. Please provide the date (from FlightDate) in the following format i.e. 2018-01-10. The maximum and Average delays should be provided in minutes but rounded to 2 digits after the decimal point.
-- MAGIC 5. Show on a bar chart the number of flights per Carrier. Use the full name of the airline (from airlines.csv).
-- MAGIC 6. Show on a pie chart the number of flights per Carrier but this time only show the individual results for the top 10 Carriers (measured by number of flights) and the rest show as one OTHER element. This will probably require the use of Temporary Views (Create or Replace Temporary View new_view as Select * from XXX). If it is done correctly OTHER should account for 16% of all flights.

-- COMMAND ----------

--Prepare Datasets(flights, airlines, airports)
%python
dbutils.fs.mv("dbfs:/FileStore/tables/flights-1.zip", "file:/tmp/flights.zip")

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls -al /tmp/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /tmp
-- MAGIC unzip flights.zip
-- MAGIC ls -al

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/flights.csv", "dbfs:/FileStore/tables/flights-1.csv")

-- COMMAND ----------

/* Import 'flights' dataset into SQL Table*/

-- (mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered)
CREATE TEMPORARY VIEW flights
USING CSV
OPTIONS (path "/FileStore/tables/flights-1.csv", header "true", mode "FAILFAST")



-- COMMAND ----------

SELECT * FROM flights

-- COMMAND ----------

/* Import 'airlines' dataset into SQL Table*/

CREATE TEMPORARY VIEW airlines
USING CSV
OPTIONS (path "/FileStore/tables/airlines.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

SELECT * FROM airlines

-- COMMAND ----------

/* Import 'airports' dataset into SQL Table*/

CREATE TEMPORARY VIEW airports
USING CSV
OPTIONS (path "/FileStore/tables/airports.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

SELECT * FROM airports

-- COMMAND ----------

describe flights

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Question 1.
-- MAGIC #####Find out which airports when treated as Origin have the smallest and the largest Departure delays. 

-- COMMAND ----------

CREATE TABLE flights_1 AS
SELECT Origin, cast(DepDelay as numeric)
FROM flights

-- COMMAND ----------

describe flights_1

-- COMMAND ----------

SELECT Origin, DepDelay
FROM flights_1 
WHERE DepDelay = (SELECT MAX(DepDelay) FROM flights_1) OR DepDelay = (SELECT MIN(DepDelay) FROM flights_1)

-- COMMAND ----------

-- MAGIC %md So, the answer for Question 1 is: 
-- MAGIC #####_Yakutat Airport(YAK) has the smallest DepDelay and Eagle County Regional Airport(EGE) has the largest DepDelay_

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Question 2.
-- MAGIC #####Create a list of U.S. States with the number of airports that each of them has in descending order. Show this on a map of the U.S. 

-- COMMAND ----------

CREATE TABLE airports_new AS
SELECT distinct fl.OriginStateName, arp.*
FROM flights AS fl RIGHT JOIN airports AS arp
ON fl.OriginState = arp.STATE

-- COMMAND ----------

SELECT *
FROM airports_new

-- COMMAND ----------

select State, OriginStateName, count(distinct AIRPORT) Airports_Num
from airports_new
group by State, OriginStateName
order by Airports_Num desc

-- COMMAND ----------

select State, count(distinct AIRPORT) Airports_Num
from airports_new
group by State
order by Airports_Num desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Question 3.
-- MAGIC #####Create a list containing: DATE, NUM_OF_FLIGHTS, MAX_ARRIVAL_DELAY, AVG_ARRIVAL_DELAY. Please provide the date (from FlightDate) in the following format i.e. 2018-01-10. The maximum and Average delays should be provided in minutes but rounded to 2 digits after the decimal point.

-- COMMAND ----------

select FlightDate as DATE, count(FlightDate) as NUM_OF_FLIGHTS, round(MAX(ArrDelay),2) as MAX_ARR_Delay, round(AVG(ArrDelay),2) as AVG_ARR_Delay
from flights
group by FlightDate


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Question 4.
-- MAGIC #####Show on a bar chart the number of flights per Carrier. Use the full name of the airline (from airlines.csv).

-- COMMAND ----------

CREATE TABLE flights_new AS
select fl.Carrier, air.airline
from flights as fl LEFT JOIN airlines AS air
ON fl.Carrier = air.IATA_CODE

-- COMMAND ----------

select *
from flights_new

-- COMMAND ----------

CREATE TABLE FlightsNum_Carrier AS
select Carrier, airline, count(Carrier) as NUM_OF_FLIGHTS
from flights_new
group by Carrier, airline
order by NUM_OF_FLIGHTS desc

-- COMMAND ----------

SELECT *
FROM FlightsNum_Carrier

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Question 5.
-- MAGIC #####Show on a pie chart the number of flights per Carrier but this time only show the individual results for the top 10 Carriers (measured by number of flights) and the rest show as one OTHER element. 
-- MAGIC _(This will probably require the use of Temporary Views (Create or Replace Temporary View new_view as Select * from XXX). If it is done correctly OTHER should account for 16% of all flights.)_

-- COMMAND ----------

--first of all, creating table with TOP 10 values

CREATE TABLE CarrierRank AS
SELECT Carrier, airline, count(Carrier) as NUM_OF_FLIGHTS 
FROM flights_new
GROUP BY Carrier, airline
ORDER BY NUM_OF_FLIGHTS DESC LIMIT 10

-- COMMAND ----------

select *
from CarrierRank  

-- COMMAND ----------

--And then, Insert the row of "OTHER" to already made CarrierRank(top10) table

CREATE TABLE CarrierRank_final AS
SELECT *
FROM CarrierRank
UNION ALL
--and the remainder:
SELECT 'OTHER', 'OTHER',
    --- The remainder is the grand total of all NUM_OF_FLIGHTS values,
    --- minus the sum of the top 10.
    (SELECT SUM(NUM_OF_FLIGHTS) FROM FlightsNum_Carrier)-
    (SELECT SUM(NUM_OF_FLIGHTS) FROM CarrierRank)


-- COMMAND ----------

select *
from CarrierRank_final
