-- Basic Databricks SQL Scripts for Reference

-- 1. Create a Database
CREATE DATABASE IF NOT EXISTS my_database;

-- 2. Use a Database
USE my_database;

-- 3. Create a Table
CREATE TABLE IF NOT EXISTS employees (
  id INT,
  name STRING,
  position STRING,
  salary DOUBLE
);

-- 4. Insert Data into Table
INSERT INTO employees (id, name, position, salary) VALUES
(1, 'Alice', 'Data Engineer', 90000),
(2, 'Bob', 'Data Analyst', 75000);

-- 5. Select Data
SELECT * FROM employees;

-- 6. Filter Data
SELECT * FROM employees WHERE salary > 80000;

-- 7. Update Data
UPDATE employees SET salary = 95000 WHERE id = 1;

-- 8. Delete Data
DELETE FROM employees WHERE id = 2;

-- 9. Drop Table
DROP TABLE IF EXISTS employees;

-- 10. Create a View
CREATE OR REPLACE VIEW high_earners AS
SELECT * FROM employees WHERE salary > 85000;

-- 11. Use a CTE (Common Table Expression)
WITH avg_salary AS (
  SELECT AVG(salary) AS avg_sal FROM employees
)
SELECT * FROM employees WHERE salary > (SELECT avg_sal FROM avg_salary);

-- 12. Aggregate Functions
SELECT COUNT(*) AS total_employees FROM employees;
SELECT SUM(salary) AS total_salary FROM employees;
SELECT MIN(salary) AS min_salary FROM employees;
SELECT MAX(salary) AS max_salary FROM employees;
SELECT AVG(salary) AS avg_salary FROM employees;
SELECT STDDEV(salary) AS std_dev FROM employees;
SELECT VAR_SAMP(salary) AS variance FROM employees;
SELECT COLLECT_LIST(position) AS all_positions FROM employees;
SELECT COLLECT_SET(position) AS unique_positions FROM employees;

-- 13. Use UNION
SELECT name FROM employees WHERE position = 'Data Engineer'
UNION
SELECT name FROM employees WHERE position = 'Data Analyst';

-- 14. Table Management
CREATE TABLE new_employees AS
SELECT * FROM employees;

DESCRIBE TABLE employees;
SHOW TABLES IN my_database;

-- 15. Data Types and Casting
SELECT CAST(salary AS STRING) AS salary_str FROM employees;
SELECT id, name, salary::STRING AS salary_str FROM employees;

-- 16. Filtering and Sorting
SELECT * FROM employees WHERE salary > 70000 AND position = 'Data Engineer';
SELECT * FROM employees ORDER BY salary DESC;

-- 17. Grouping
SELECT position, AVG(salary) AS avg_salary
FROM employees
GROUP BY position;

SELECT position, COUNT(*) AS count
FROM employees
GROUP BY position
HAVING count > 1;

-- 18. Joins
SELECT a.name, b.department
FROM employees a
JOIN departments b ON a.id = b.employee_id;

SELECT a.name, b.department
FROM employees a
LEFT JOIN departments b ON a.id = b.employee_id;

-- 19. Window Functions
SELECT name, salary,
       ROW_NUMBER() OVER (PARTITION BY position ORDER BY salary DESC) AS row_num
FROM employees;

SELECT name, salary,
       SUM(salary) OVER (ORDER BY id) AS running_total
FROM employees;

-- 20. Date Functions
SELECT CURRENT_DATE();
SELECT DATE_ADD(CURRENT_DATE(), 7) AS next_week;
SELECT DATEDIFF('2025-12-01', '2025-06-01') AS days_between;

-- 21. CASE Statements
SELECT name, salary,
  CASE 
    WHEN salary >= 90000 THEN 'High'
    WHEN salary >= 75000 THEN 'Medium'
    ELSE 'Low'
  END AS salary_level
FROM employees;

-- 22. Delta Lake-Specific Commands
CREATE TABLE delta_employees (
  id INT,
  name STRING,
  position STRING,
  salary DOUBLE
) USING DELTA;

OPTIMIZE delta_employees;
DESCRIBE HISTORY delta_employees;

-- 23. File and External Table Operations
CREATE TABLE csv_employees
USING CSV
OPTIONS (
  path 'dbfs:/mnt/data/employees.csv',
  header 'true',
  inferSchema 'true'
);

CREATE TABLE parquet_employees
USING PARQUET
OPTIONS (
  path 'dbfs:/mnt/data/employees.parquet'
);

-- 24. Permissions and Access Control
GRANT SELECT ON TABLE employees TO `data_analyst_role`;
SHOW GRANT ON TABLE employees;

