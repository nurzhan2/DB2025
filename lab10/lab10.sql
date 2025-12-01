-- Setup
DROP TABLE IF EXISTS accounts, products CASCADE;
CREATE TABLE accounts (
 id SERIAL PRIMARY KEY,
 name VARCHAR(100) NOT NULL,
 balance DECIMAL(10,2) DEFAULT 0.00
);
CREATE TABLE products (
 id SERIAL PRIMARY KEY,
 shop VARCHAR(100) NOT NULL,
 product VARCHAR(100) NOT NULL,
 price DECIMAL(10,2) NOT NULL
);
INSERT INTO accounts (name, balance) VALUES
 ('Alice',1000.00),('Bob',500.00),('Wally',750.00);
INSERT INTO products (shop,product,price) VALUES
 ('Joe''s Shop','Coke',2.50),('Joe''s Shop','Pepsi',3.00);

-----------------------------------------------------------------
-- Task 1: COMMIT
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE name='Alice';
UPDATE accounts SET balance = balance + 100 WHERE name='Bob';
COMMIT;
SELECT * FROM accounts;

-----------------------------------------------------------------
-- Task 2: ROLLBACK
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE name='Alice';
SELECT * FROM accounts WHERE name='Alice';
ROLLBACK;
SELECT * FROM accounts WHERE name='Alice';

-----------------------------------------------------------------
-- Task 3: SAVEPOINT
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE name='Alice';
SAVEPOINT sp;
UPDATE accounts SET balance = balance + 100 WHERE name='Bob';
ROLLBACK TO sp;
UPDATE accounts SET balance = balance + 100 WHERE name='Wally';
COMMIT;
SELECT * FROM accounts;

-----------------------------------------------------------------
-- Task 4 Scenario A: READ COMMITTED
-- Terminal 1:
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM products WHERE shop='Joe''s Shop';

-- Terminal 2:
BEGIN;
DELETE FROM products WHERE shop='Joe''s Shop';
INSERT INTO products (shop,product,price)
VALUES ('Joe''s Shop','Fanta',3.50);
COMMIT;

-- Terminal 1:
SELECT * FROM products WHERE shop='Joe''s Shop';
COMMIT;

-----------------------------------------------------------------
-- Task 4 Scenario B: SERIALIZABLE
-- Terminal 1:
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM products WHERE shop='Joe''s Shop';

-- Terminal 2:
BEGIN;
DELETE FROM products WHERE shop='Joe''s Shop';
INSERT INTO products (shop,product,price)
VALUES ('Joe''s Shop','Fanta',3.50);
COMMIT;

-- Terminal 1:
SELECT * FROM products WHERE shop='Joe''s Shop';
COMMIT;

-----------------------------------------------------------------
-- Task 5: Phantom Read
-- Terminal 1:
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT MAX(price),MIN(price) FROM products WHERE shop='Joe''s Shop';

-- Terminal 2:
BEGIN;
INSERT INTO products (shop,product,price)
VALUES ('Joe''s Shop','Sprite',4.00);
COMMIT;

-- Terminal 1:
SELECT MAX(price),MIN(price) FROM products WHERE shop='Joe''s Shop';
COMMIT;

-----------------------------------------------------------------
-- Task 6: Dirty Read
-- Terminal 1:
BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM products WHERE shop='Joe''s Shop';

-- Terminal 2:
BEGIN;
UPDATE products SET price = 99.99 WHERE product='Fanta';
-- wait...
ROLLBACK;

-- Terminal 1:
SELECT * FROM products WHERE shop='Joe''s Shop';
COMMIT;

-----------------------------------------------------------------
-- Independent Exercise 1
BEGIN;
UPDATE accounts SET balance = balance - 200
 WHERE name='Bob' AND balance >= 200;
UPDATE accounts SET balance = balance + 200
 WHERE name='Wally';
COMMIT;
SELECT * FROM accounts;

-----------------------------------------------------------------
-- Independent Exercise 2
BEGIN;
INSERT INTO products (shop,product,price)
VALUES ('Joe''s Shop','Water',1.20);
SAVEPOINT sp1;
UPDATE products SET price=2.00 WHERE product='Water';
SAVEPOINT sp2;
DELETE FROM products WHERE product='Water';
ROLLBACK TO sp1;
COMMIT;
SELECT * FROM products;

-----------------------------------------------------------------
-- Independent Exercise 3 (simplified two-session withdraw)
-- Use SERIALIZABLE for safety
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE accounts SET balance = balance - 200
 WHERE name='Alice' AND balance >= 200;
COMMIT;
SELECT * FROM accounts;

-----------------------------------------------------------------
-- Independent Exercise 4 (MAX < MIN issue demonstration)
-- Incorrect isolation
BEGIN;
SELECT MAX(price),MIN(price) FROM products WHERE shop='Joe''s Shop';
-- Meanwhile another session modifies…
ROLLBACK;

-- Correct with isolation
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT MAX(price),MIN(price) FROM products WHERE shop='Joe''s Shop';
COMMIT;
