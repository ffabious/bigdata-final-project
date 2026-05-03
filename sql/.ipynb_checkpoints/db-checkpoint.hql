-- Drop database if exists for clean reruns
DROP DATABASE IF EXISTS team18_projectdb CASCADE;

-- Create database with location
CREATE DATABASE team18_projectdb 
LOCATION 'project/hive/warehouse';

USE team18_projectdb;

-- Set optimization parameters
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;

-- Create external table for events (unpartitioned, from Sqoop import)
CREATE EXTERNAL TABLE events_raw 
STORED AS AVRO 
LOCATION 'project/warehouse/events' 
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/events.avsc');

-- Verify table creation
SELECT COUNT(*) as total_events FROM events_raw;
SELECT * FROM events_raw LIMIT 10;