-- ==========================================================
-- Lab 3: Advanced DML Operations (Full Solution)
-- ==========================================================

-- ======================
-- Part A: Setup
-- ======================
DROP DATABASE IF EXISTS advanced_lab;
CREATE DATABASE advanced_lab;
\c advanced_lab;

-- Create tables
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50) DEFAULT 'General',
    salary INT DEFAULT 40000,
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'Active'
);

CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(50),
    budget INT,
    manager_id INT
);

CREATE TABLE projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(50),
    dept_id INT,
    start_date DATE,
    end_date DATE,
    budget INT
);

-- ======================
-- Part B: INSERT
-- ======================

-- 2. INSERT with column specification
INSERT INTO employees (first_name, last_name, department)
VALUES ('John', 'Doe', 'IT');

-- 3. INSERT with DEFAULT values
INSERT INTO employees (first_name, last_name)
VALUES ('Alice', 'Smith');

-- 4. INSERT multiple rows
INSERT INTO departments (dept_name, budget, manager_id)
VALUES 
  ('IT', 200000, 1),
  ('HR', 100000, 2),
  ('Sales', 150000, 3);

-- 5. INSERT with expressions
INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES ('Bob', 'Taylor', 'Finance', 50000 * 1.1, CURRENT_DATE);

-- 6. INSERT from SELECT
CREATE TEMP TABLE temp_employees AS
SELECT * FROM employees WHERE department = 'IT';

-- ======================
-- Part C: UPDATE
-- ======================

-- 7. UPDATE with arithmetic
UPDATE employees SET salary = salary * 1.1;

-- 8. UPDATE with conditions
UPDATE employees
SET status = 'Senior'
WHERE salary > 60000 AND hire_date < '2020-01-01';

-- 9. UPDATE with CASE
UPDATE employees
SET department = CASE
    WHEN salary > 80000 THEN 'Management'
    WHEN salary BETWEEN 50000 AND 80000 THEN 'Senior'
    ELSE 'Junior'
END;

-- 10. UPDATE with DEFAULT
UPDATE employees
SET department = DEFAULT
WHERE status = 'Inactive';

-- 11. UPDATE with subquery
UPDATE departments d
SET budget = (
    SELECT AVG(salary) * 1.2
    FROM employees e
    WHERE e.department = d.dept_name
);

-- 12. UPDATE multiple columns
UPDATE employees
SET salary = salary * 1.15,
    status = 'Promoted'
WHERE department = 'Sales';

-- ======================
-- Part D: DELETE
-- ======================

-- 13. DELETE simple
DELETE FROM employees WHERE status = 'Terminated';

-- 14. DELETE with complex WHERE
DELETE FROM employees
WHERE salary < 40000 AND hire_date > '2023-01-01' AND department IS NULL;

-- 15. DELETE with subquery
DELETE FROM departments
WHERE dept_name NOT IN (
    SELECT DISTINCT department
    FROM employees
    WHERE department IS NOT NULL
);

-- 16. DELETE with RETURNING
DELETE FROM projects
WHERE end_date < '2023-01-01'
RETURNING *;

-- ======================
-- Part E: NULL Operations
-- ======================

-- 17. INSERT with NULLs
INSERT INTO employees (first_name, last_name, salary, department)
VALUES ('NullGuy', 'Test', NULL, NULL);

-- 18. UPDATE NULL handling
UPDATE employees
SET department = 'Unassigned'
WHERE department IS NULL;

-- 19. DELETE with NULL conditions
DELETE FROM employees
WHERE salary IS NULL OR department IS NULL;

-- ======================
-- Part F: RETURNING
-- ======================

-- 20. INSERT with RETURNING
INSERT INTO employees (first_name, last_name, department)
VALUES ('Eve', 'Johnson', 'IT')
RETURNING emp_id, first_name || ' ' || last_name AS full_name;

-- 21. UPDATE with RETURNING
UPDATE employees
SET salary = salary + 5000
WHERE department = 'IT'
RETURNING emp_id, salary - 5000 AS old_salary, salary AS new_salary;

-- 22. DELETE with RETURNING all columns
DELETE FROM employees
WHERE hire_date < '2020-01-01'
RETURNING *;

-- ======================
-- Part G: Advanced DML Patterns
-- ======================

-- 23. Conditional INSERT (NOT EXISTS)
INSERT INTO employees (first_name, last_name, department)
SELECT 'Sam', 'Brown', 'HR'
WHERE NOT EXISTS (
    SELECT 1 FROM employees
    WHERE first_name = 'Sam' AND last_name = 'Brown'
);

-- 24. UPDATE with JOIN-like logic
UPDATE employees e
SET salary = salary * CASE
    WHEN (SELECT budget FROM departments d WHERE d.dept_name = e.department) > 100000
        THEN 1.10
    ELSE 1.05
END;

-- 25. Bulk operations
INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES
 ('A1','Test','Sales',45000,CURRENT_DATE),
 ('A2','Test','Sales',46000,CURRENT_DATE),
 ('A3','Test','Sales',47000,CURRENT_DATE),
 ('A4','Test','Sales',48000,CURRENT_DATE),
 ('A5','Test','Sales',49000,CURRENT_DATE);

UPDATE employees
SET salary = salary * 1.10
WHERE last_name = 'Test';

-- 26. Data migration
CREATE TABLE employee_archive (LIKE employees INCLUDING ALL);

INSERT INTO employee_archive
SELECT * FROM employees WHERE status = 'Inactive';

DELETE FROM employees WHERE status = 'Inactive';

-- 27. Complex business logic
UPDATE projects
SET end_date = end_date + INTERVAL '30 days'
WHERE budget > 50000
AND dept_id IN (
    SELECT d.dept_id
    FROM departments d
    JOIN employees e ON e.department = d.dept_name
    GROUP BY d.dept_id
    HAVING COUNT(e.emp_id) > 3
);

-- ==========================================================
-- END OF LAB 3
-- ==========================================================
