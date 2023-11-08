-- Executed with PostgreSQL

-- create schema
CREATE SCHEMA salary_per_hour;

-- create table
CREATE TABLE salary_per_hour.employees (
	employee_id VARCHAR(255) PRIMARY KEY,
    branch_id VARCHAR(255),
    salary INTEGER,
	join_date DATE,
	resign_date DATE
);

CREATE TABLE salary_per_hour.timesheets (
	timesheet_id VARCHAR(255) PRIMARY KEY,
    employee_id VARCHAR(255),
    date DATE,
    checkin TIME,
	checkout TIME
);

-- Create temporary table for further preprocessing
CREATE TEMPORARY TABLE temp_employees (
	employee_id VARCHAR(255),
    branch_id VARCHAR(255),
    salary INTEGER,
	join_date DATE,
	resign_date DATE
);

-- load employees CSV
COPY temp_employees FROM 'employees.csv' DELIMITER ',' CSV HEADER;

-- preprocess employees data (ensuring no duplicates on employee_id)
INSERT INTO salary_per_hour.employees
SELECT DISTINCT ON (employee_id) 
	employee_id, branch_id, salary, join_date, resign_date 
FROM temp_employees
WHERE
	employee_id IS NOT NULL AND
	branch_id IS NOT NULL AND
	salary IS NOT NULL AND
	join_date IS NOT NULL
ORDER BY employee_id, join_date;

DROP TABLE temp_employees;

-- Create temporary table for further preprocessing
CREATE TEMPORARY TABLE temp_timesheets (
	timesheet_id VARCHAR(255),
    employee_id VARCHAR(255),
    date DATE,
    checkin TIME,
	checkout TIME
);

-- load timesheets CSV
COPY temp_timesheets FROM 'timesheets.csv' DELIMITER ',' CSV HEADER;

-- preprocess employees data (ensuring no duplicates on timesheet_id)
INSERT INTO salary_per_hour.timesheets
SELECT DISTINCT ON (timesheet_id)
	timesheet_id, employee_id, date, checkin, checkout
FROM temp_timesheets
WHERE
	timesheet_id IS NOT NULL AND
	employee_id IS NOT NULL AND
	date IS NOT NULL AND
	checkin IS NOT NULL AND
	checkout IS NOT NULL;

DROP TABLE temp_timesheets;

-- Create a result table
CREATE TABLE IF NOT EXISTS salary_per_hour.salary_per_hour_results (
    year INTEGER,
    month INTEGER,
    branch_id VARCHAR(255),
    salary_per_hour NUMERIC(10, 2)
);

-- Truncate the destination table to remove old data
TRUNCATE TABLE salary_per_hour.salary_per_hour_results;

-- Transform and load the result
INSERT INTO salary_per_hour.salary_per_hour_results (year, month, branch_id, salary_per_hour)
SELECT
    EXTRACT(YEAR FROM ts.date) AS year,
    EXTRACT(MONTH FROM ts.date) AS month,
    e.branch_id,
    ROUND(SUM(e.salary) / SUM(EXTRACT(EPOCH FROM (ts.checkout - ts.checkin)) / 3600), 2) AS salary_per_hour
FROM
    salary_per_hour.employees e
JOIN
    salary_per_hour.timesheets ts ON e.employee_id = ts.employee_id
WHERE
    ts.date IS NOT NULL
    AND ts.checkin IS NOT NULL
    AND ts.checkout IS NOT NULL
    AND ts.date >= e.join_date
    AND (ts.date <= e.resign_date OR e.resign_date IS NULL)
GROUP BY
    year, month, branch_id;

-- SELECT year, month, branch_id, salary_per_hour FROM salary_per_hour.salary_per_hour_results
-- ORDER BY year, month, branch_id;

