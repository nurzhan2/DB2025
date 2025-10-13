-- Part 1: CHECK Constraints

-- Task 1.1: Basic CHECK Constraint
CREATE TABLE employees (
    employee_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER CHECK (age BETWEEN 18 AND 65),
    salary NUMERIC CHECK (salary > 0)
);

INSERT INTO employees VALUES (1, 'John', 'Doe', 25, 50000);
INSERT INTO employees VALUES (2, 'Jane', 'Smith', 30, 60000);

-- Task 1.2: Named CHECK Constraint
CREATE TABLE products_catalog (
    product_id INTEGER,
    product_name TEXT,
    regular_price NUMERIC,
    discount_price NUMERIC,
    CONSTRAINT valid_discount CHECK (
        regular_price > 0 AND 
        discount_price > 0 AND 
        discount_price < regular_price
    )
);

INSERT INTO products_catalog VALUES (1, 'Laptop', 1000, 800);
INSERT INTO products_catalog VALUES (2, 'Mouse', 50, 40);

-- Task 1.3: Multiple Column CHECK
CREATE TABLE bookings (
    booking_id INTEGER,
    check_in_date DATE,
    check_out_date DATE,
    num_guests INTEGER CHECK (num_guests BETWEEN 1 AND 10),
    CHECK (check_out_date > check_in_date)
);

INSERT INTO bookings VALUES (1, '2024-01-01', '2024-01-05', 2);
INSERT INTO bookings VALUES (2, '2024-02-01', '2024-02-03', 4);

-- Part 2: NOT NULL Constraints

-- Task 2.1: NOT NULL Implementation
CREATE TABLE customers (
    customer_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    registration_date DATE NOT NULL
);

INSERT INTO customers VALUES (1, 'john@email.com', '123-456-7890', '2024-01-01');
INSERT INTO customers VALUES (2, 'jane@email.com', NULL, '2024-01-02');

-- Task 2.2: Combining Constraints
CREATE TABLE inventory (
    item_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price NUMERIC NOT NULL CHECK (unit_price > 0),
    last_updated TIMESTAMP NOT NULL
);

INSERT INTO inventory VALUES (1, 'Widget A', 100, 19.99, '2024-01-01 10:00:00');
INSERT INTO inventory VALUES (2, 'Widget B', 50, 29.99, '2024-01-01 11:00:00');

-- Part 3: UNIQUE Constraints

-- Task 3.1: Single Column UNIQUE
CREATE TABLE users (
    user_id INTEGER,
    username TEXT UNIQUE,
    email TEXT UNIQUE,
    created_at TIMESTAMP
);

INSERT INTO users VALUES (1, 'alice123', 'alice@email.com', '2024-01-01 09:00:00');
INSERT INTO users VALUES (2, 'bob456', 'bob@email.com', '2024-01-01 10:00:00');

-- Task 3.2: Multi-Column UNIQUE
CREATE TABLE course_enrollments (
    enrollment_id INTEGER,
    student_id INTEGER,
    course_code TEXT,
    semester TEXT,
    UNIQUE (student_id, course_code, semester)
);

-- Task 3.3: Named UNIQUE Constraints
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id INTEGER,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP,
    CONSTRAINT unique_username UNIQUE (username),
    CONSTRAINT unique_email UNIQUE (email)
);

INSERT INTO users VALUES (1, 'alice123', 'alice@email.com', '2024-01-01 09:00:00');
INSERT INTO users VALUES (2, 'bob456', 'bob@email.com', '2024-01-01 10:00:00');

-- Part 4: PRIMARY KEY Constraints

-- Task 4.1: Single Column Primary Key
CREATE TABLE departments (
    dept_id INTEGER PRIMARY KEY,
    dept_name TEXT NOT NULL,
    location TEXT
);

INSERT INTO departments VALUES (1, 'HR', 'New York');
INSERT INTO departments VALUES (2, 'IT', 'San Francisco');
INSERT INTO departments VALUES (3, 'Finance', 'Chicago');

-- Task 4.2: Composite Primary Key
CREATE TABLE student_courses (
    student_id INTEGER,
    course_id INTEGER,
    enrollment_date DATE,
    grade TEXT,
    PRIMARY KEY (student_id, course_id)
);

-- Part 5: FOREIGN KEY Constraints

-- Task 5.1: Basic Foreign Key
CREATE TABLE employees_dept (
    emp_id INTEGER PRIMARY KEY,
    emp_name TEXT NOT NULL,
    dept_id INTEGER REFERENCES departments(dept_id),
    hire_date DATE
);

INSERT INTO employees_dept VALUES (101, 'John Smith', 1, '2023-01-15');
INSERT INTO employees_dept VALUES (102, 'Maria Garcia', 2, '2023-02-20');

-- Task 5.2: Multiple Foreign Keys
CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY,
    author_name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE publishers (
    publisher_id INTEGER PRIMARY KEY,
    publisher_name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER REFERENCES authors(author_id),
    publisher_id INTEGER REFERENCES publishers(publisher_id),
    publication_year INTEGER,
    isbn TEXT UNIQUE
);

INSERT INTO authors VALUES (1, 'J.K. Rowling', 'UK');
INSERT INTO authors VALUES (2, 'George Orwell', 'UK');
INSERT INTO authors VALUES (3, 'Agatha Christie', 'UK');

INSERT INTO publishers VALUES (1, 'Penguin Books', 'London');
INSERT INTO publishers VALUES (2, 'HarperCollins', 'New York');
INSERT INTO publishers VALUES (3, 'Simon & Schuster', 'New York');

INSERT INTO books VALUES (1, 'Harry Potter', 1, 1, 1997, '978-0439708180');
INSERT INTO books VALUES (2, '1984', 2, 2, 1949, '978-0451524935');
INSERT INTO books VALUES (3, 'Murder on the Orient Express', 3, 3, 1934, '978-0062693662');

-- Task 5.3: ON DELETE Options
CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE products_fk (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INTEGER REFERENCES categories ON DELETE RESTRICT
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL
);

CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders ON DELETE CASCADE,
    product_id INTEGER REFERENCES products_fk,
    quantity INTEGER CHECK (quantity > 0)
);

INSERT INTO categories VALUES (1, 'Electronics');
INSERT INTO categories VALUES (2, 'Books');

INSERT INTO products_fk VALUES (1, 'Laptop', 1);
INSERT INTO products_fk VALUES (2, 'Novel', 2);

INSERT INTO orders VALUES (1, '2024-01-15');
INSERT INTO orders VALUES (2, '2024-01-16');

INSERT INTO order_items VALUES (1, 1, 1, 2);
INSERT INTO order_items VALUES (2, 1, 2, 1);
INSERT INTO order_items VALUES (3, 2, 1, 1);

-- Part 6: Practical Application

-- Task 6.1: E-commerce Database Design
CREATE TABLE ecommerce_customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    registration_date DATE NOT NULL
);

CREATE TABLE ecommerce_products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price NUMERIC NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
);

CREATE TABLE ecommerce_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES ecommerce_customers,
    order_date DATE NOT NULL,
    total_amount NUMERIC CHECK (total_amount >= 0),
    status TEXT CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
);

CREATE TABLE ecommerce_order_details (
    order_detail_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES ecommerce_orders ON DELETE CASCADE,
    product_id INTEGER REFERENCES ecommerce_products,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC NOT NULL CHECK (unit_price >= 0)
);

INSERT INTO ecommerce_customers VALUES 
(1, 'Alice Johnson', 'alice@email.com', '555-0101', '2024-01-01'),
(2, 'Bob Smith', 'bob@email.com', '555-0102', '2024-01-02'),
(3, 'Carol Davis', 'carol@email.com', '555-0103', '2024-01-03'),
(4, 'David Wilson', 'david@email.com', '555-0104', '2024-01-04'),
(5, 'Eva Brown', 'eva@email.com', '555-0105', '2024-01-05');

INSERT INTO ecommerce_products VALUES 
(1, 'Smartphone', 'Latest smartphone model', 699.99, 50),
(2, 'Laptop', 'High-performance laptop', 1299.99, 25),
(3, 'Headphones', 'Wireless noise-canceling', 199.99, 100),
(4, 'Tablet', '10-inch tablet', 399.99, 30),
(5, 'Smartwatch', 'Fitness tracking watch', 249.99, 75);

INSERT INTO ecommerce_orders VALUES 
(1, 1, '2024-01-10', 899.98, 'delivered'),
(2, 2, '2024-01-11', 1299.99, 'processing'),
(3, 3, '2024-01-12', 449.98, 'shipped'),
(4, 1, '2024-01-13', 199.99, 'pending'),
(5, 4, '2024-01-14', 1749.97, 'processing');

INSERT INTO ecommerce_order_details VALUES 
(1, 1, 1, 1, 699.99),
(2, 1, 3, 1, 199.99),
(3, 2, 2, 1, 1299.99),
(4, 3, 4, 1, 399.99),
(5, 3, 3, 1, 199.99),
(6, 4, 3, 1, 199.99),
(7, 5, 2, 1, 1299.99),
(8, 5, 4, 1, 399.99),
(9, 5, 5, 1, 249.99);
