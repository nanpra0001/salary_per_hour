# salary_per_hour
Data Engineer Coding Challenge

## Description

The solution for the Salary per Hour challenge.

## Contents

You will find two files consisting of Python code & SQL script (`code.py` and `script.sql`). Both of them solve the same problem. The SQL script uses `PostgreSQL` as the backend, while the Python code uses `SQLite3` as the backend.

## Challenges encountered

There are some cases, such as:
- on `employee.csv`, the column name is `employe_id` instead of `employee_id`
  - Solved it by renaming the column
- duplicate `employee_id` on `employee.csv`
  - Solved it by implementing preprocessing that only use one of them
- empty `checkout` data on `timesheets.csv`
  - Solved it by removing the empty data
