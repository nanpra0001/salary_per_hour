import sqlite3
import pandas as pd

employees_data = 'employees.csv'
timesheets_data = 'timesheets.csv'

# Connect to the SQLite database
db = "salary_per_hour.db"
conn = sqlite3.connect(db)
cursor = conn.cursor()

# Query lists
create_table_query = """
CREATE TABLE IF NOT EXISTS salary_per_hour_results (
    year INTEGER,
    month INTEGER,
    branch_id TEXT,
    salary_per_hour NUMERIC(10, 2)
);
"""
cursor.execute(create_table_query)

create_employee_table = """
CREATE TABLE IF NOT EXISTS employees (
    employee_id VARCHAR(255),
    branch_id VARCHAR(255),
    salary INTEGER,
    join_date DATE,
    resign_date DATE
)
"""
cursor.execute(create_employee_table)

create_timesheet_table = """
CREATE TABLE IF NOT EXISTS timesheets (
    timesheet_id VARCHAR(255),
    employee_id VARCHAR(255),
    date DATE,
    checkin TIME,
    checkout TIME
)
"""
cursor.execute(create_timesheet_table)

# Temporary table for preprocessing
create_temp_employees = """
CREATE TEMPORARY TABLE IF NOT EXISTS temp_employees (
	employee_id VARCHAR(255),
    branch_id VARCHAR(255),
    salary INTEGER,
	join_date DATE,
	resign_date DATE
)
"""
cursor.execute(create_temp_employees)

# Temporary table for preprocessing
create_temp_timesheets = """
CREATE TEMPORARY TABLE IF NOT EXISTS temp_timesheets (
	timesheet_id VARCHAR(255),
    employee_id VARCHAR(255),
    date DATE,
    checkin TIME,
	checkout TIME
)
"""
cursor.execute(create_temp_timesheets)

conn.commit()

# To filter duplicate employee_id
insert_preprocessed_employees = """
INSERT INTO employees
SELECT employee_id, branch_id, salary, join_date, resign_date
FROM (
    SELECT 
        employee_id, branch_id, salary, join_date, resign_date,
        ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY join_date) AS rn
    FROM temp_employees
    WHERE
        employee_id IS NOT NULL AND
        branch_id IS NOT NULL AND
        salary IS NOT NULL AND
        join_date IS NOT NULL
) AS ranked
WHERE rn = 1
"""

# To filter empty checkout data
insert_preprocessed_timesheets = """
INSERT INTO timesheets
SELECT timesheet_id, employee_id, date, checkin, checkout
FROM (
    SELECT 
        timesheet_id, employee_id, date, checkin, checkout,
        ROW_NUMBER() OVER(PARTITION BY timesheet_id ORDER BY date) AS rn
    FROM temp_timesheets
    WHERE
        timesheet_id IS NOT NULL AND
        employee_id IS NOT NULL AND
        date IS NOT NULL AND
        checkin IS NOT NULL AND
        checkout IS NOT NULL
) AS ranked
WHERE rn = 1
"""

transform_and_load_query = """
INSERT INTO salary_per_hour_results (year, month, branch_id, salary_per_hour)
SELECT
    strftime('%Y', ts.date) AS year,
    strftime('%m', ts.date) AS month,
    e.branch_id,
    ROUND(SUM(e.salary) / SUM((strftime('%s', ts.checkout) - strftime('%s', ts.checkin)) / 3600), 2) AS salary_per_hour
FROM employees e
JOIN timesheets ts ON e.employee_id = ts.employee_id
WHERE
    ts.date IS NOT NULL
    AND ts.checkin IS NOT NULL
    AND ts.checkout IS NOT NULL
    AND ts.date >= e.join_date
    AND (ts.date <= e.resign_date OR e.resign_date IS NULL)
GROUP BY year, month, branch_id
"""

def read_data():
    """
    A function to get the data from CSV files

    Output
    ======
    employees_df : pandas.DataFrame
        Employee data
    
    timesheets_df : pandas.DataFrame
        timesheet data
    """

    employees_df = pd.read_csv(employees_data)
    timesheets_df = pd.read_csv(timesheets_data)
    
    # renaming wrong column name
    employees_df = employees_df.rename(columns={'employe_id': 'employee_id'})

    return employees_df, timesheets_df

def preprocess_data(employees_df, timesheets_df):
    """
    A function to preprocess the data

    Input
    =====
    employees_df : pandas.DataFrame
        Employee data
    
    timesheets_df : pandas.DataFrame
        timesheet data
    """
    
    # Only append the new data
    employees_df.to_sql("temp_employees", conn, if_exists='append', index=False)
    timesheets_df.to_sql("temp_timesheets", conn, if_exists='append', index=False)

    cursor.execute(insert_preprocessed_employees)
    cursor.execute(insert_preprocessed_timesheets)
    conn.commit()

def transform_load():
    """
    A function to transform and load the data into new table
    """

    cursor.execute(transform_and_load_query)
    conn.commit()


employees_dataframe, timesheets_dataframe = read_data()
preprocess_data(employees_dataframe, timesheets_dataframe)
transform_load()

conn.close()
