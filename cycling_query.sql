--2019
CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2019/*']
);  

CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2019` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;

--2020
CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2020/*']
);  

CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2020` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;

--2021
CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2021/*']
);  

CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2021` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;

--2022
CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2022/*']
);  

CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2022` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;

--2023
CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2023/*']
);  

--2024
CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2023` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;

CREATE OR REPLACE EXTERNAL TABLE `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cycling_data_boreal-quarter-455022-q5/combined/2024/*']
);  

CREATE OR REPLACE TABLE `boreal-quarter-455022-q5.cycling_data_uk.cycling_2024` AS
SELECT * FROM `boreal-quarter-455022-q5.cycling_data_uk.cyclingext`;




