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

-- 12. Use UNION
SELECT name FROM employees WHERE position = 'Data Engineer'
UNION
SELECT name FROM employees WHERE position = 'Data Analyst';
