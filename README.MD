# TPC-DI Benchmak in Spark SQL

TPC Benchmark™ DI (TPC-DI) is a performance test of tools that move and integrate data between various systems. Data Integration (DI) tools are available from a number of vendors, but until now there has been no standard way to compare them. Such tools have also been referred to as Extract, Transform and Load (ETL) tools at times. 

Several tools are used for the purpose of implementing TPC-DI benchmark on Apache Spark. The goal of this project was to benchmark Apache Spark and evaluate this data warehouse based on the performance using TPC-DI guideline and data.

## Code Structure

The repository contains the implementation of the TPCD-DI in Spark. The file TPC-DI.py contains the execution code for the five steps of TPC-DI. In addtion, there is a visualization notebook to create graphs of the results. Th execution results are inside of folder result and images.

## Implementation

This implementation was developed in the section of the TPC-DI notebook.

### Step 1: Initialization 

This stage involves: 

- Data is cleaned from previous runs. 

- Data warehouse schema is created. 

- Helper functions to process data are created.


### Step 2: Historical Load 

- The Data Warehouse is populated with data. 

- Transformations are applied as described in clause 4.5 of the TPC-DI manual (batch1 folder on the generated data). 

- Validations are being performed based on clause 7.4. 

- Upon completion of the validation stage, a phase completion record is written into DIMessages table. 

- Measuring the time of this step. 


### Step 3: Incremental Update 1 

- The Data Warehouse is populated with data. 

- Transformations are applied as described in clause 4.5 of the TPC-DI manual (batch2 folder on the generated data). 

- Validations are being performed based on clause 7.4 

- Upon completion of the validation stage, a phase completion record is written into DIMessages table. 

- Measuring the time of this step. 

 
 ### Step 4: Incremental Update 2 

- The Data Warehouse is populated with data. 

- Transformations are applied as described in clause 4.6 of the TPC-DI manual (batch3 folder on the generated data). 

- Validations are being performed based on clause 7.4 

- Upon completion of the validation stage, a phase completion record is written into DIMessages table. 

- Measuring the time of this step.

