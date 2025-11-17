-- Part 1: Setup

CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50),
    location VARCHAR(50)
);

CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100),
    dept_id INT,
    salary DECIMAL(10,2),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

CREATE TABLE projects (
    proj_id INT PRIMARY KEY,
    proj_name VARCHAR(100),
    budget DECIMAL(12,2),
    dept_id INT,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

INSERT INTO departments VALUES 
(101, 'IT', 'Building A'),
(102, 'HR', 'Building B'),
(103, 'Operations', 'Building C');

INSERT INTO employees VALUES
(1, 'John Smith', 101, 50000),
(2, 'Jane Doe', 101, 55000),
(3, 'Mike Johnson', 102, 48000),
(4, 'Sarah Williams', 102, 52000),
(5, 'Tom Brown', 103, 60000);

INSERT INTO projects VALUES
(201, 'Website Redesign', 75000, 101),
(202, 'Database Migration', 120000, 101),
(203, 'HR System Upgrade', 50000, 102);

-- Part 2: Basic Indexes

CREATE INDEX emp_salary_idx ON employees(salary);

SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'employees';

CREATE INDEX emp_dept_idx ON employees(dept_id);

SELECT * FROM employees WHERE dept_id = 101;

SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Part 3: Multicolumn Indexes

CREATE INDEX emp_dept_salary_idx ON employees(dept_id, salary);

SELECT emp_name, salary 
FROM employees 
WHERE dept_id = 101 AND salary > 52000;

CREATE INDEX emp_salary_dept_idx ON employees(salary, dept_id);

SELECT * FROM employees WHERE dept_id = 102 AND salary > 50000;
SELECT * FROM employees WHERE salary > 50000 AND dept_id = 102;

-- Part 4: Unique Indexes

ALTER TABLE employees ADD COLUMN email VARCHAR(100);

UPDATE employees SET email = 'john.smith@company.com' WHERE emp_id = 1;
UPDATE employees SET email = 'jane.doe@company.com' WHERE emp_id = 2;
UPDATE employees SET email = 'mike.johnson@company.com' WHERE emp_id = 3;
UPDATE employees SET email = 'sarah.williams@company.com' WHERE emp_id = 4;
UPDATE employees SET email = 'tom.brown@company.com' WHERE emp_id = 5;

CREATE UNIQUE INDEX emp_email_unique_idx ON employees(email);

INSERT INTO employees (emp_id, emp_name, dept_id, salary, email)
VALUES (6, 'New Employee', 101, 55000, 'john.smith@company.com');

ALTER TABLE employees ADD COLUMN phone VARCHAR(20) UNIQUE;

SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'employees' AND indexname LIKE '%phone%';

-- Part 5: Indexes and Sorting

CREATE INDEX emp_salary_desc_idx ON employees(salary DESC);

SELECT emp_name, salary 
FROM employees 
ORDER BY salary DESC;

CREATE INDEX proj_budget_nulls_first_idx ON projects(budget NULLS FIRST);

SELECT proj_name, budget 
FROM projects 
ORDER BY budget NULLS FIRST;

-- Part 6: Expression / Function-Based Indexes

CREATE INDEX emp_name_lower_idx ON employees(LOWER(emp_name));

SELECT * FROM employees WHERE LOWER(emp_name) = 'john smith';

ALTER TABLE employees ADD COLUMN hire_date DATE;

UPDATE employees SET hire_date = '2020-01-15' WHERE emp_id = 1;
UPDATE employees SET hire_date = '2019-06-20' WHERE emp_id = 2;
UPDATE employees SET hire_date = '2021-03-10' WHERE emp_id = 3;
UPDATE employees SET hire_date = '2020-11-05' WHERE emp_id = 4;
UPDATE employees SET hire_date = '2018-08-25' WHERE emp_id = 5;

CREATE INDEX emp_hire_year_idx ON employees ((EXTRACT(YEAR FROM hire_date)));

SELECT emp_name, hire_date 
FROM employees 
WHERE EXTRACT(YEAR FROM hire_date) = 2020;

-- Part 7: Managing Indexes

ALTER INDEX emp_salary_idx RENAME TO employees_salary_index;

SELECT indexname FROM pg_indexes WHERE tablename = 'employees';

DROP INDEX emp_salary_dept_idx;

REINDEX INDEX employees_salary_index;

-- Part 8: Practical Scenarios

SELECT e.emp_name, e.salary, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 50000
ORDER BY e.salary DESC;

CREATE INDEX emp_salary_filter_idx ON employees(salary) WHERE salary > 50000;

CREATE INDEX proj_high_budget_idx ON projects(budget) 
WHERE budget > 80000;

SELECT proj_name, budget 
FROM projects 
WHERE budget > 80000;

EXPLAIN SELECT * FROM employees WHERE salary > 52000;

-- Part 9: Index Types Comparison

CREATE INDEX dept_name_hash_idx ON departments USING HASH (dept_name);

SELECT * FROM departments WHERE dept_name = 'IT';

CREATE INDEX proj_name_btree_idx ON projects(proj_name);
CREATE INDEX proj_name_hash_idx ON projects USING HASH (proj_name);

SELECT * FROM projects WHERE proj_name = 'Website Redesign';

SELECT * FROM projects WHERE proj_name > 'Database';

-- Part 10: Cleanup and Best Practices

SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

DROP INDEX IF EXISTS proj_name_hash_idx;

CREATE VIEW index_documentation AS
SELECT 
    tablename,
    indexname,
    indexdef,
    'Improves salary-based queries' as purpose
FROM pg_indexes
WHERE schemaname = 'public' 
  AND indexname LIKE '%salary%';

SELECT * FROM index_documentation;
