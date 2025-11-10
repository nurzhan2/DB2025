  DROP VIEW IF EXISTS employee_details;
CREATE VIEW employee_details AS
SELECT e.emp_id, e.emp_name, e.salary, d.dept_id, d.dept_name, d.location
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

DROP VIEW IF EXISTS dept_statistics;
CREATE VIEW dept_statistics AS
SELECT d.dept_id, d.dept_name,
COALESCE(COUNT(e.emp_id),0) AS employee_count,
COALESCE(ROUND(AVG(e.salary)::numeric,2),0) AS avg_salary,
COALESCE(MAX(e.salary),0) AS max_salary,
COALESCE(MIN(e.salary),0) AS min_salary
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name;

DROP VIEW IF EXISTS project_overview;
CREATE VIEW project_overview AS
SELECT p.project_id, p.project_name, p.budget, d.dept_id, d.dept_name, d.location,
COALESCE(t.team_size,0) AS team_size
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
LEFT JOIN (SELECT dept_id, COUNT(*) AS team_size FROM employees WHERE dept_id IS NOT NULL GROUP BY dept_id) t ON d.dept_id = t.dept_id;

DROP VIEW IF EXISTS high_earners;
CREATE VIEW high_earners AS
SELECT emp_id, emp_name, salary, dept_id
FROM employees
WHERE salary > 55000;

CREATE OR REPLACE VIEW employee_details AS
SELECT e.emp_id, e.emp_name, e.salary,
CASE WHEN e.salary > 60000 THEN 'High' WHEN e.salary > 50000 THEN 'Medium' ELSE 'Standard' END AS salary_grade,
d.dept_id, d.dept_name, d.location
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

DROP VIEW IF EXISTS top_performers;
ALTER VIEW high_earners RENAME TO top_performers;

CREATE TEMPORARY VIEW temp_view AS
SELECT emp_id, emp_name, salary
FROM employees
WHERE salary < 50000;
DROP VIEW IF EXISTS temp_view;

DROP VIEW IF EXISTS employee_salaries;
CREATE VIEW employee_salaries AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees;

DROP VIEW IF EXISTS it_employees;
CREATE VIEW it_employees AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees
WHERE dept_id = 101
WITH LOCAL CHECK OPTION;

DROP MATERIALIZED VIEW IF EXISTS dept_summary_mv;
CREATE MATERIALIZED VIEW dept_summary_mv AS
SELECT d.dept_id, d.dept_name,
COALESCE(COUNT(e.emp_id),0) AS total_employees,
COALESCE(SUM(e.salary),0) AS total_salaries,
COALESCE(COUNT(p.project_id),0) AS total_projects,
COALESCE(SUM(p.budget),0) AS total_project_budget
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
LEFT JOIN projects p ON d.dept_id = p.dept_id
GROUP BY d.dept_id, d.dept_name
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dept_summary_mv_dept_id ON dept_summary_mv(dept_id);

DROP MATERIALIZED VIEW IF EXISTS project_stats_mv;
CREATE MATERIALIZED VIEW project_stats_mv AS
SELECT p.project_id, p.project_name, p.budget, d.dept_id, d.dept_name,
COALESCE(pc.assigned_count,0) AS assigned_employees_count
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
LEFT JOIN (SELECT dept_id, COUNT(*) AS assigned_count FROM employees WHERE dept_id IS NOT NULL GROUP BY dept_id) pc ON d.dept_id = pc.dept_id
WITH NO DATA;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='analyst') THEN CREATE ROLE analyst; END IF; END$$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='data_viewer') THEN CREATE ROLE data_viewer LOGIN PASSWORD 'viewer123'; END IF; END$$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='report_user') THEN CREATE ROLE report_user LOGIN PASSWORD 'report456'; END IF; END$$;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='db_creator') THEN CREATE ROLE db_creator LOGIN PASSWORD 'creator789' CREATEDB; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='user_manager') THEN CREATE ROLE user_manager LOGIN PASSWORD 'manager101' CREATEROLE; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='admin_user') THEN CREATE ROLE admin_user LOGIN PASSWORD 'admin999' SUPERUSER; END IF;
END$$;

GRANT SELECT ON employees, departments, projects TO analyst;
GRANT ALL PRIVILEGES ON TABLE employee_details TO data_viewer;
GRANT SELECT, INSERT ON employees TO report_user;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='hr_team') THEN CREATE ROLE hr_team; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='finance_team') THEN CREATE ROLE finance_team; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='it_team') THEN CREATE ROLE it_team; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='hr_user1') THEN CREATE ROLE hr_user1 LOGIN PASSWORD 'hr001'; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='hr_user2') THEN CREATE ROLE hr_user2 LOGIN PASSWORD 'hr002'; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='finance_user1') THEN CREATE ROLE finance_user1 LOGIN PASSWORD 'fin001'; END IF;
END$$;

GRANT hr_team TO hr_user1;
GRANT hr_team TO hr_user2;
GRANT finance_team TO finance_user1;

GRANT SELECT, UPDATE ON employees TO hr_team;
GRANT SELECT ON dept_statistics TO finance_team;

REVOKE UPDATE ON employees FROM hr_team;
REVOKE hr_team FROM hr_user2;
REVOKE ALL PRIVILEGES ON employee_details FROM data_viewer;

ALTER ROLE analyst WITH LOGIN PASSWORD 'analyst123';
ALTER ROLE user_manager WITH SUPERUSER;
ALTER ROLE analyst WITH PASSWORD NULL;
ALTER ROLE data_viewer WITH CONNECTION LIMIT 5;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='read_only') THEN CREATE ROLE read_only; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='junior_analyst') THEN CREATE ROLE junior_analyst LOGIN PASSWORD 'junior123'; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='senior_analyst') THEN CREATE ROLE senior_analyst LOGIN PASSWORD 'senior123'; END IF;
END$$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO read_only;
GRANT read_only TO junior_analyst;
GRANT read_only TO senior_analyst;
GRANT INSERT, UPDATE ON employees TO senior_analyst;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='project_manager') THEN CREATE ROLE project_manager LOGIN PASSWORD 'pm123'; END IF;
END$$;

ALTER VIEW dept_statistics OWNER TO project_manager;
ALTER TABLE projects OWNER TO project_manager;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='temp_owner') THEN CREATE ROLE temp_owner LOGIN PASSWORD 'temp123'; END IF;
END$$;

DROP TABLE IF EXISTS temp_table;
CREATE TABLE temp_table (id INT);
ALTER TABLE temp_table OWNER TO temp_owner;
REASSIGN OWNED BY temp_owner TO postgres;
DROP OWNED BY temp_owner;
DROP ROLE IF EXISTS temp_owner;

DROP VIEW IF EXISTS hr_employee_view;
CREATE VIEW hr_employee_view AS
SELECT emp_id, emp_name, dept_id, salary
FROM employees
WHERE dept_id = 102;
GRANT SELECT ON hr_employee_view TO hr_team;

DROP VIEW IF EXISTS finance_employee_view;
CREATE VIEW finance_employee_view AS
SELECT emp_id, emp_name, salary
FROM employees;
GRANT SELECT ON finance_employee_view TO finance_team;

DROP VIEW IF EXISTS dept_dashboard;
CREATE VIEW dept_dashboard AS
SELECT d.dept_id, d.dept_name, d.location,
COALESCE(COUNT(e.emp_id),0) AS employee_count,
COALESCE(ROUND(COALESCE(AVG(e.salary),0)::numeric,2),0) AS avg_salary,
COALESCE(COUNT(DISTINCT p.project_id),0) AS active_projects,
COALESCE(SUM(p.budget),0) AS total_project_budget,
CASE WHEN COUNT(e.emp_id)=0 THEN 0 ELSE ROUND((COALESCE(SUM(p.budget),0)/COUNT(e.emp_id))::numeric,2) END AS budget_per_employee
FROM departments d
LEFT JOIN employees e ON d.dept_id=e.dept_id
LEFT JOIN projects p ON d.dept_id=p.dept_id
GROUP BY d.dept_id,d.dept_name,d.location;

DO $$ BEGIN
BEGIN
ALTER TABLE projects ADD COLUMN created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
EXCEPTION WHEN duplicate_column THEN NULL;
END;
END$$;

DROP VIEW IF EXISTS high_budget_projects;
CREATE VIEW high_budget_projects AS
SELECT p.project_id, p.project_name, p.budget, d.dept_name, p.created_date,
CASE WHEN p.budget>150000 THEN 'Critical Review Required'
WHEN p.budget>100000 THEN 'Management Approval Needed'
ELSE 'Standard Process' END AS approval_status
FROM projects p
LEFT JOIN departments d ON p.dept_id=d.dept_id
WHERE p.budget>75000;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='viewer_role') THEN CREATE ROLE viewer_role; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='entry_role') THEN CREATE ROLE entry_role; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='analyst_role') THEN CREATE ROLE analyst_role; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='manager_role') THEN CREATE ROLE manager_role; END IF;
END$$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO viewer_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO viewer_role;
GRANT viewer_role TO entry_role;
GRANT entry_role TO analyst_role;
GRANT analyst_role TO manager_role;
GRANT INSERT ON employees, projects TO entry_role;
GRANT UPDATE ON employees, projects TO analyst_role;
GRANT DELETE ON employees, projects TO manager_role;

DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='alice') THEN CREATE ROLE alice LOGIN PASSWORD 'alice123'; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='bob') THEN CREATE ROLE bob LOGIN PASSWORD 'bob123'; END IF;
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='charlie') THEN CREATE ROLE charlie LOGIN PASSWORD 'charlie123'; END IF;
END$$;

GRANT viewer_role TO alice;
GRANT analyst_role TO bob;
GRANT manager_role TO charlie;
