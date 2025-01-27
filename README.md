# Spark and Git -> Merge and clean Spark Dataframes

A simple script to read data from .csv files, load them to Spark Dataframes, and merge and clean the data removing duplicates by keeping the latest entry point given an 'ID'. 

### What is included on this template?

- 'data' folder containing the .csv files
- 'environment' folder containing requirements.txt to install dependencies
- 'utils' folder containing:
    * load_data.py -> function to load data in a Spark dataframe from .csv file
    * df_manipulation.py -> function to merge and clean data from two dataframes
- 'clean_db.py' main script to run


---
## Install requirements

```bash
pip install -r environment/requirements.txt
```

## Usage

```bash
$ python clean_db.py
```

## Sample output from existing data
+---+-------------------+------+--------+

| ID|        Date       |Amount|Currency|

+---+-------------------+------+--------+

|101|2023-01-10 00:00:00| 150.0|     USD|

|102|2023-01-07 00:00:00| 190.0|     EUR|

|103|2023-01-15 00:00:00| 500.0|     USD|

+---+-------------------+------+--------+
