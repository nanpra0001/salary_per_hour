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
  - Solved it by implementing preprocessing that only uses one of them
- empty `checkout` data on `timesheets.csv`
  - Solved it by removing the empty data

## My approach to the problem

1. Read the CSV file
2. load the contents to a temporary table to be preprocessed
3. preprocess insert the data from the temporary table to the main tables (`employees` & `timesheets`)
   - for the `employees` table: ensure that the `employee_id` is unique
   - for the `timesheets` table: ensure that the `timesheet_id` is unique AND no NULL values on `checkout`
4. Do the transformation process
5. Load the result to the table

In the Python code, you may notice that I'm using (mostly) SQL approaches instead of the classic Pandas approach. Why? All the data are in tabular format and SQL can handle those data pretty well, there's no need to use more sophisticated tool (for this problem only)
