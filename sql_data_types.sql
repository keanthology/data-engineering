-- ==========================================
-- Basic SQL Data Types Demonstration
-- Author: [Your Name]
-- Description: Creates a table with various common SQL data types.
-- ==========================================

-- Drop the table if it already exists (optional, depends on your SQL engine)
DROP TABLE IF EXISTS employee_data;

-- Create a demo table with various data types
CREATE TABLE employee_data (
    employee_id     INT,               -- INT: Whole number, commonly used for unique IDs
    full_name       VARCHAR(100),      -- VARCHAR: Variable-length text, ideal for names, comments, etc.
    age             SMALLINT,          -- SMALLINT: Small integer, saves space if values are within -32k to 32k
    salary          DECIMAL(10, 2),    -- DECIMAL(p, s): Exact numeric value with 2 decimal places for currency
    is_active       BOOLEAN,           -- BOOLEAN: True/False flag (can be 1/0 or TRUE/FALSE)
    hire_date       DATE,              -- DATE: Stores only date (YYYY-MM-DD), no time
    last_login      TIMESTAMP          -- TIMESTAMP: Stores date and time (YYYY-MM-DD HH:MM:SS)
);

-- Insert sample data
INSERT INTO employee_data VALUES 
(1, 'Alice Johnson', 30, 75000.50, TRUE,  '2020-05-10', '2024-06-01 08:30:00'),
(2, 'Bob Smith',     45, 92000.00, FALSE, '2018-09-23', '2024-05-29 14:50:00'),
(3, 'Charlie Reyes', 28, 61000.75, TRUE,  '2022-01-17', '2024-06-05 09:00:00');

-- View all records in the table
SELECT * FROM employee_data;

-- View table structure and data types (varies by SQL engine)

-- PostgreSQL or MySQL
-- \d employee_data

-- SQL Server or Databricks
DESCRIBE employee_data;

-- ==========================================
-- Summary of Data Types Used:
-- ------------------------------------------
-- INT         : Whole numbers
-- SMALLINT    : Smaller range of integers (uses less memory)
-- VARCHAR(n)  : Text up to n characters
-- DECIMAL(p,s): Exact numeric with precision (p) and scale (s)
-- BOOLEAN     : Logical TRUE/FALSE values
-- DATE        : Date only
-- TIMESTAMP   : Date + time
-- ==========================================
