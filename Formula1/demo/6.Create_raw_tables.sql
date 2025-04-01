-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create external table from circuits.csv file

-- COMMAND ----------

drop table if exists f1_raw.circuits;

create table if not exists f1_raw.circuits(
  circuitID int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat float,
  lng float,
  alt int,
  url string
)
using csv
options (path "/mnt/purvadl/raw/circuits.csv/", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create external table from races.csv file
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId int,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "/mnt/purvadl/raw/races.csv/", header true);

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create External Tables from JSON file
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Constructors Table
-- MAGIC 1. Simple line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING, 
  name STRING,
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS (path "/mnt/purvadl/raw/constructors.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Drivers Table
-- MAGIC 1. Simple line JSON
-- MAGIC 2. Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  nationality STRING,
  url STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE)
  USING JSON
  OPTIONS (path "/mnt/purvadl/raw/drivers.json");


-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Results Table
-- MAGIC 1. Simple line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points DECIMAL(10),
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING JSON
OPTIONS (path "/mnt/purvadl/raw/results.json");

-- COMMAND ----------

select * from f1_raw.results;


-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
  driverId INT,
  raceId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS (path "/mnt/purvadl/raw/pit_stops.json", multiLine True);

-- COMMAND ----------

select * from f1_raw.pitstops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create External Table for multiple files 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Lap Time Tables
-- MAGIC 1. Multiple CSV files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/purvadl/raw/lap_times")


-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/purvadl/raw/qualifying", multiLine True);

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying

-- COMMAND ----------


