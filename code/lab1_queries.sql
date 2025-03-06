CREATE DATABASE IF NOT EXISTS DEV;

CREATE SCHEMA IF NOT EXISTS DEV.RAW;
CREATE SCHEMA IF NOT EXISTS DEV.ADHOC;
CREATE SCHEMA IF NOT EXISTS DEV.ANALYTICS;

SELECT * FROM DEV.RAW.STOCK_DATA;

show tables in dev.raw;
show views;
show schemas;
select * from dev.analytics.stock_data;