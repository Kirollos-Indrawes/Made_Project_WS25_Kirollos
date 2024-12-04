#!/bin/bash

echo "Running the pipeline..."
python3 ./pipeline.py

# Test 1: Check if Datasets are there
echo "Test 1: Checking if datasets are downloaded..."
if [ -f "./data/emissions.csv" ] && [ -f "./data/CO2 Emissions_Canada.csv" ]; then
    echo "Test 1: Dataset download - PASSED"
else
    echo "Test 1: Dataset download - FAILED"
    exit 1
fi

# Test 2: Check Data Transformations part ETL 
echo "Test 2: Checking data transformations..."
python3 -c "
import pandas as pd
cars = pd.read_csv('./data/CO2 Emissions_Canada.csv')
# Check if specified columns are dropped
assert 'Fuel Consumption Hwy (L/100 km)' not in cars.columns, 'Column not dropped'
assert 'Fuel Consumption City (L/100 km)' not in cars.columns, 'Column not dropped'
assert 'Fuel Consumption Comb (mpg)' not in cars.columns, 'Column not dropped'
print('Test 2: Data cleaning - PASSED')
"

# Test 3: Check if the Database Output is sucessfully loaded
echo "Test 3: Checking if database is created..."
if [ -f './data/pipeline.db' ]; then
    echo "Test 3: Database output - PASSED"
else
    echo "Test 3: Database output - FAILED"
    exit 1
fi

# Test 4: Check if SQLite Database Tables loaded
echo "Test 4: Checking if tables exist in the SQLite database..."
python3 -c "
import sqlite3
conn = sqlite3.connect('./data/pipeline.db')
cursor = conn.cursor()
cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\";')
tables = cursor.fetchall()
# Check the 3 tables in my Database
assert 'co2_usa' in [table[0] for table in tables], 'co2_usa table not found'
assert 'cars' in [table[0] for table in tables], 'cars table not found'
assert 'co2_canada' in [table[0] for table in tables], 'co2_canada table not found'
print('Test 4: Database validation - PASSED')
"

echo "All tests finished"