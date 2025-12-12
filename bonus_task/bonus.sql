
-- README / Design Notes


-- Schema: customers, accounts, transactions, exchange_rates, audit_log.
-- Key decisions:
-- * Use SELECT ... FOR UPDATE when modifying account balances to prevent race conditions.
-- * process_transfer implemented as PROCEDURE to allow transaction control and OUT parameter.
-- * process_salary_batch:
--    - uses advisory lock (pg_try_advisory_lock(hashtext(...))) to avoid concurrent batch processing
--    - nested BEGIN..EXCEPTION blocks are used to simulate savepoint behaviour for per-row failures
--    - collects individual credits then updates balances atomically at the end (single debit, multiple credits)
--    - salary transactions bypass daily_limit checks (business rule)
-- * Index strategy:
--    - Composite B-tree on transactions(from_account_id, created_at DESC) for recent lookups
--    - Partial index on accounts(customer_id, balance) WHERE is_active = true to speed active-account queries
--    - Expression index on LOWER(email) for case-insensitive lookups
--    - GIN indexes on audit_log JSONB columns for JSON searches
--    - Covering index (INCLUDE) on accounts(customer_id) to enable index-only scans
-- Testing notes:
-- * Run EXPLAIN ANALYZE before and after index creation, paste outputs into this file.
-- * Demonstrate concurrency by running the locking demo (see below).



--tables 
--  customers
 CREATE TABLE customers (
  customer_id serial PRIMARY KEY,
  iin      char(12) UNIQUE NOT NULL,
  full_name   text NOT NULL,
  phone   text,
  email   text,
  status  text NOT NULL DEFAULT 'active', -- active/blocked/frozen
  created_at timestamptz NOT NULL DEFAULT now(),
  daily_limit_kzt numeric(18,2) NOT NULL DEFAULT 100000.00
);



-- accounts
 CREATE TABLE accounts (
  account_id serial PRIMARY KEY,
  customer_id int REFERENCES customers(customer_id) ON DELETE CASCADE,
  account_number text UNIQUE NOT NULL, -- assume IBAN-like
  currency  text NOT NULL, -- KZT,USD,EUR,RUB
  balance   numeric(18,2) NOT NULL DEFAULT 0,
  is_active boolean NOT NULL DEFAULT true,
  opened_at timestamptz NOT NULL DEFAULT now(),
  closed_at timestamptz
);



-- transactions
CREATE TABLE transactions (
  transaction_id serial PRIMARY KEY,
  from_account_id int REFERENCES accounts(account_id),
   to_account_id   int REFERENCES accounts(account_id),
  amount numeric(18,2) NOT NULL,
  currency text NOT NULL,
  exchange_rate numeric(18,8),
  amount_kzt numeric(18,2),
  type text NOT NULL, -- transfer/deposit/withdrawal
  status text NOT NULL DEFAULT 'pending', -- pending/completed/failed/reversed
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  description text
);



-- exchange_rates
 CREATE TABLE exchange_rates (
   rate_id serial PRIMARY KEY,
   from_currency text NOT NULL,
   to_currency   text NOT NULL,
   rate numeric(18,8) NOT NULL,
   valid_from timestamptz NOT NULL DEFAULT now(),
   valid_to   timestamptz
);

--  audit_log
 CREATE TABLE audit_log (
  log_id serial PRIMARY KEY,
  table_name text NOT NULL,
  record_id text,
  action text NOT NULL, -- INSERT/UPDATE/DELETE
  old_values jsonb,
  new_values jsonb,
  changed_by text,
  changed_at timestamptz NOT NULL DEFAULT now(),
  ip_address text
);





--data 
INSERT INTO customers(iin, full_name, phone, email, status, daily_limit_kzt) VALUES
('123456789011','Patrick Star','+7-700-1111111','patrick@bikinibottom.com','active',150000),
('123456789012','SpongeBob SquarePants','+7-700-2222222','sponge@bikinibottom.com','active',200000),
('123456789013','Gennady GGG Golovkin','+7-700-3333333','ggg@boxing.kz','active',500000),
('123456789014','Mike Tyson','+7-700-4444444','tyson@boxing.com','active',300000),
('123456789015','Brad Pitt','+7-700-5555555','brad@hollywood.com','active',400000),
('123456789016','Postman Pechkin','+7-700-6666666','pechkin@prostokvashino.ru','blocked',50000),
('123456789017','Homer Simpson','+7-700-7777777','homer@springfield.tv','active',120000),
('123456789018','Rick Sanchez','+7-700-8888888','rick@multiverse.io','frozen',10000),
('123456789019','Morty Smith','+7-700-9999999','morty@earth-d42.io','active',90000),
('123456789020','Tony Stark','+7-701-0000000','ironman@starkindustries.com','active',900000);





 INSERT INTO accounts(customer_id, account_number, currency, balance, is_active)
VALUES
(1,'KZ00PAT0001','KZT',70000,true),
(2,'KZ00SPG0001','KZT',500000,true),
(3,'US00GGG0001','USD',3500,true),
(4,'US00TYS0001','USD',10000,true),
(5,'EU00BRD0001','EUR',2200,true),
(6,'KZ00PCH0001','KZT',15000,true),
(7,'KZ00HMR0001','KZT',45000,true),
(8,'KZ00RCK0001','KZT',5000,true),
(9,'KZ00MRT0001','KZT',8000,true),
(10,'KZ00STK0001','KZT',3000000,true);






 INSERT INTO exchange_rates(from_currency,to_currency,rate,valid_from,valid_to)
VALUES
('USD','KZT',475.40, now()-interval '5 hours', NULL),
 ('EUR','KZT',515.20, now()-interval '1 day', NULL),
('RUB','KZT',5.90 , now()-interval '2 days', NULL),
('KZT','KZT',1.0 , now(), NULL),
('USD','EUR',0.91 , now(), NULL);




INSERT INTO transactions(from_account_id,to_account_id,amount,currency,type,status,amount_kzt,exchange_rate,description)
VALUES
 (1,2,2000,'KZT','transfer','completed',2000,1,'Patrick→Sponge rent'),
(2,1,5000,'KZT','transfer','completed',5000,1,'Sponge repays debt'),
(3,4,100,'USD','transfer','completed',47540,475.40,'GGG→Tyson sparring fee'),
(4,3,150,'USD','transfer','completed',71310,475.40,'Tyson reverse spar payment'),
(5,1,50,'EUR','transfer','completed',25760,515.20,'Brad sends euros'),
(7,8,3000,'KZT','transfer','completed',3000,1,'Homer buys portal fluid from Rick'),
(9,7,200,'KZT','transfer','completed',200,1,'Morty buys donuts'),
(10,3,100000,'KZT','transfer','completed',100000,1,'Stark invests in GGG'),
(6,1,500,'KZT','transfer','failed',500,1,'Pechkin failed transfer'),
(8,9,700,'KZT','transfer','completed',700,1,'Rick→Morty gadget payment');





--task 1

DROP PROCEDURE IF EXISTS process_transfer(VARCHAR, VARCHAR, NUMERIC, VARCHAR, TEXT, OUT TEXT);

CREATE OR REPLACE PROCEDURE process_transfer(
    IN p_from_account_number VARCHAR,
    IN p_to_account_number   VARCHAR,
    IN p_amount              NUMERIC,
    IN p_currency            VARCHAR,
    IN p_description         TEXT,
    OUT p_result             TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_account_id      INTEGER;
    v_to_account_id        INTEGER;
    v_from_currency        VARCHAR(3);
    v_to_currency          VARCHAR(3);
    v_from_balance         NUMERIC;
    v_to_balance           NUMERIC;
    v_from_is_active       BOOLEAN;
    v_to_is_active         BOOLEAN;
    v_from_customer_id     INTEGER;
    v_from_customer_status VARCHAR(20);

    v_rate_to_kzt          NUMERIC;
    v_amount_kzt           NUMERIC;
    v_rate_p_to_from       NUMERIC;
    v_rate_p_to_to         NUMERIC;
    v_amount_debit         NUMERIC;
    v_amount_credit        NUMERIC;
    v_daily_limit_kzt      NUMERIC;
    v_used_today_kzt       NUMERIC;

    v_transaction_id       BIGINT;
BEGIN
    -- Basic checks
    IF p_amount IS NULL OR p_amount <= 0 THEN
        p_result := 'ERROR:ARG001: amount must be > 0';
        RETURN;
    END IF;

    -- Lock and read source account
    SELECT account_id, currency, balance, is_active, customer_id
    INTO v_from_account_id, v_from_currency, v_from_balance, v_from_is_active, v_from_customer_id
    FROM accounts
    WHERE account_number = p_from_account_number
    FOR UPDATE;

    IF NOT FOUND THEN
        p_result := 'ERROR:ACC001: source account not found';
        RETURN;
    END IF;

    IF NOT v_from_is_active THEN
        p_result := 'ERROR:ACC002: source account is not active';
        RETURN;
    END IF;

    -- Lock and read destination account
    SELECT account_id, currency, balance, is_active
    INTO v_to_account_id, v_to_currency, v_to_balance, v_to_is_active
    FROM accounts
    WHERE account_number = p_to_account_number
    FOR UPDATE;

    IF NOT FOUND THEN
        p_result := 'ERROR:ACC003: destination account not found';
        RETURN;
    END IF;

    IF NOT v_to_is_active THEN
        p_result := 'ERROR:ACC004: destination account is not active';
        RETURN;
    END IF;

    -- Check sender's customer status and daily limit config
    SELECT status, daily_limit_kzt
    INTO v_from_customer_status, v_daily_limit_kzt
    FROM customers
    WHERE customer_id = v_from_customer_id;

    IF NOT FOUND THEN
        p_result := 'ERROR:CUST002: source customer not found';
        RETURN;
    END IF;

    IF v_from_customer_status <> 'active' THEN
        p_result := 'ERROR:CUST001: source customer status is not active';
        RETURN;
    END IF;

    -- Get p_currency -> KZT rate to compute daily limit usage
    IF p_currency = 'KZT' THEN
        v_rate_to_kzt := 1.0;
    ELSE
        SELECT rate INTO v_rate_to_kzt
        FROM exchange_rates
        WHERE from_currency = p_currency
          AND to_currency = 'KZT'
          AND (valid_to IS NULL OR CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to)
        ORDER BY valid_from DESC
        LIMIT 1;

        IF NOT FOUND THEN
            p_result := 'ERROR:RATE001: rate ' || p_currency || '->KZT not found';
            RETURN;
        END IF;
    END IF;

    v_amount_kzt := (p_amount * v_rate_to_kzt)::NUMERIC;

    -- Sum completed transfers today (in KZT)
    SELECT COALESCE(SUM(amount_kzt),0) INTO v_used_today_kzt
    FROM transactions
    WHERE from_account_id = v_from_account_id
      AND DATE(created_at) = CURRENT_DATE
      AND status = 'completed';

    IF (v_used_today_kzt + v_amount_kzt) > COALESCE(v_daily_limit_kzt, 0) THEN
        p_result := 'ERROR:LIMIT001: daily limit exceeded (used=' || v_used_today_kzt || ', transfer_kzt=' || v_amount_kzt || ', limit=' || COALESCE(v_daily_limit_kzt,0) || ')';
        RETURN;
    END IF;

    -- Compute debit (in source account currency)
    IF p_currency = v_from_currency THEN
        v_amount_debit := p_amount;
    ELSE
        SELECT rate INTO v_rate_p_to_from
        FROM exchange_rates
        WHERE from_currency = p_currency
          AND to_currency = v_from_currency
          AND (valid_to IS NULL OR CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to)
        ORDER BY valid_from DESC
        LIMIT 1;

        IF NOT FOUND THEN
            p_result := 'ERROR:RATE002: rate ' || p_currency || '->' || v_from_currency || ' not found';
            RETURN;
        END IF;

        v_amount_debit := (p_amount * v_rate_p_to_from)::NUMERIC;
    END IF;

    -- Compute credit (in destination account currency)
    IF p_currency = v_to_currency THEN
        v_amount_credit := p_amount;
    ELSE
        SELECT rate INTO v_rate_p_to_to
        FROM exchange_rates
        WHERE from_currency = p_currency
          AND to_currency = v_to_currency
          AND (valid_to IS NULL OR CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to)
        ORDER BY valid_from DESC
        LIMIT 1;

        IF NOT FOUND THEN
            p_result := 'ERROR:RATE003: rate ' || p_currency || '->' || v_to_currency || ' not found';
            RETURN;
        END IF;

        v_amount_credit := (p_amount * v_rate_p_to_to)::NUMERIC;
    END IF;

    -- Balance check
    IF v_from_balance < v_amount_debit THEN
        p_result := 'ERROR:BAL001: insufficient balance (available=' || v_from_balance || ' ' || v_from_currency || ', required=' || v_amount_debit || ')';
        RETURN;
    END IF;

    -- All validations passed — do the transfer atomically
    BEGIN
        INSERT INTO transactions (
            from_account_id, to_account_id, amount, currency, exchange_rate, amount_kzt, type, status, description, created_at, completed_at
        ) VALUES (
            v_from_account_id, v_to_account_id, p_amount, p_currency, v_rate_to_kzt, v_amount_kzt, 'transfer', 'completed', p_description, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        ) RETURNING transaction_id INTO v_transaction_id;

        UPDATE accounts SET balance = balance - v_amount_debit WHERE account_id = v_from_account_id;
        UPDATE accounts SET balance = balance + v_amount_credit WHERE account_id = v_to_account_id;

        -- audit (best-effort)
        BEGIN
            INSERT INTO audit_log (table_name, record_id, action, old_values, new_values)
            VALUES (
                'transactions', v_transaction_id, 'INSERT', NULL,
                jsonb_build_object(
                    'transaction_id', v_transaction_id,
                    'from', p_from_account_number,
                    'to', p_to_account_number,
                    'amount', p_amount,
                    'currency', p_currency,
                    'amount_kzt', v_amount_kzt,
                    'status', 'completed'
                )
            );
        EXCEPTION WHEN OTHERS THEN
            -- do not fail transfer if audit insert fails
            NULL;
        END;

        p_result := 'SUCCESS: tx=' || v_transaction_id || '; debited=' || v_amount_debit || ' ' || v_from_currency || '; credited=' || v_amount_credit || ' ' || v_to_currency || '; kzt=' || v_amount_kzt;
        RETURN;
    EXCEPTION WHEN OTHERS THEN
        -- try to write audit of failure, but swallow any further errors
        BEGIN
            INSERT INTO audit_log (table_name, record_id, action, new_values)
            VALUES ('transactions', NULL, 'INSERT', jsonb_build_object('error', SQLERRM, 'from', p_from_account_number, 'to', p_to_account_number, 'amount', p_amount));
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        p_result := 'FAILED: ' || SQLERRM;
        RETURN;
    END;

END;
$$;


--mini test

--CALL process_transfer('KZ11AAAA0000000001','KZ11AAAA0000000010',10000,'KZT','test payment', NULL);
--output: ERROR:ACC001: source account not found

--CALL process_transfer('KZ00PAT0001','KZ00SPG0001',10000,'KZT','test payment', NULL);
--output: SUCCESS: tx=41; debited=10000 KZT; credited=10000 KZT; kzt=10000.0


--task 2

--view 1
CREATE OR REPLACE VIEW customer_balance_summary AS
WITH acct_rates AS (
    -- pick current rate from account currency -> KZT (if account currency is already KZT -> 1)
    SELECT
        a.account_id,
        a.customer_id,
        a.account_number,
        a.currency,
        a.balance,
        COALESCE(er.rate, 1.0) AS rate_to_kzt,
        (a.balance * COALESCE(er.rate, 1.0))::NUMERIC(18,2) AS balance_kzt
    FROM accounts a
    LEFT JOIN LATERAL (
        SELECT rate
        FROM exchange_rates er
        WHERE er.from_currency = a.currency
          AND er.to_currency = 'KZT'
          AND (er.valid_to IS NULL OR CURRENT_TIMESTAMP BETWEEN er.valid_from AND er.valid_to)
        ORDER BY er.valid_from DESC
        LIMIT 1
    ) er ON true
    WHERE 1=1
),
cust_agg AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.status,
        c.daily_limit_kzt,
        ar.account_id,
        ar.account_number,
        ar.currency,
        ar.balance,
        ar.rate_to_kzt,
        ar.balance_kzt
    FROM customers c
    LEFT JOIN acct_rates ar ON c.customer_id = ar.customer_id
),
used_today AS (
    SELECT
        a.customer_id,
        COALESCE(SUM(t.amount_kzt),0)::NUMERIC(18,2) AS used_today_kzt
    FROM accounts a
    LEFT JOIN transactions t
      ON a.account_id = t.from_account_id
      AND DATE(t.created_at) = CURRENT_DATE
      AND t.status = 'completed'
    GROUP BY a.customer_id
),
customer_totals AS (
    SELECT
        ca.customer_id,
        ca.full_name,
        ca.email,
        ca.status,
        ca.daily_limit_kzt,
        SUM(COALESCE(ca.balance_kzt,0)) AS total_balance_kzt,
        COALESCE(ud.used_today_kzt,0) AS used_today_kzt
    FROM cust_agg ca
    LEFT JOIN used_today ud ON ca.customer_id = ud.customer_id
    GROUP BY ca.customer_id, ca.full_name, ca.email, ca.status, ca.daily_limit_kzt, ud.used_today_kzt
)
SELECT
    ca.customer_id,
    ca.full_name,
    ca.email,
    ca.status,
    ca.account_id,
    ca.account_number,
    ca.currency,
    ca.balance,
    ca.rate_to_kzt,
    ca.balance_kzt,
    ct.total_balance_kzt,
    ct.daily_limit_kzt,
    ct.used_today_kzt,
    -- percent utilization (rounded)
    ROUND( (ct.used_today_kzt / NULLIF(ct.daily_limit_kzt,0)) * 100::numeric, 2) AS daily_limit_utilization_pct,
    RANK() OVER (ORDER BY ct.total_balance_kzt DESC) AS balance_rank
FROM cust_agg ca
JOIN customer_totals ct ON ca.customer_id = ct.customer_id
ORDER BY ct.total_balance_kzt DESC, ca.customer_id, ca.account_id;

--mini test
--SELECT * FROM customer_balance_summary WHERE customer_id = 1;
--output
--1,Patrick Star,patrick@bikinibottom.com,active,1,KZ00PAT0001,KZT,60000.00,1,60000.00,60000,150000.00,12000,8,6

--view 2

CREATE OR REPLACE VIEW daily_transaction_report AS
WITH completed AS (
    SELECT
        DATE(t.created_at) AS txn_date,
        t.type,
        t.amount_kzt
    FROM transactions t
    WHERE t.status = 'completed'
),
daily_agg AS (
    SELECT
        txn_date,
        type,
        COUNT(*)::INT AS transaction_count,
        COALESCE(SUM(amount_kzt),0)::NUMERIC(18,2) AS total_volume_kzt,
        COALESCE(AVG(amount_kzt),0)::NUMERIC(18,2) AS avg_amount_kzt
    FROM completed
    GROUP BY txn_date, type
),
running AS (
    SELECT
        da.*,
        SUM(da.total_volume_kzt) OVER (PARTITION BY da.type ORDER BY da.txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_kzt,
        LAG(da.total_volume_kzt) OVER (PARTITION BY da.type ORDER BY da.txn_date) AS prev_day_volume
    FROM daily_agg da
)
SELECT
    txn_date AS transaction_date,
    type,
    transaction_count,
    total_volume_kzt,
    avg_amount_kzt,
    running_total_kzt,
    -- day-over-day growth percent
    CASE
        WHEN prev_day_volume IS NULL OR prev_day_volume = 0 THEN NULL
        ELSE ROUND(((total_volume_kzt - prev_day_volume) / prev_day_volume) * 100.0, 2)
    END AS day_over_day_growth_pct
FROM running
ORDER BY transaction_date DESC, type;

--view 3
CREATE OR REPLACE VIEW suspicious_activity_view
WITH (security_barrier = true) AS
WITH
large_transactions AS (
    SELECT
        t.transaction_id,
        t.from_account_id,
        fa.account_number AS from_account_number,
        fa.customer_id,
        c.full_name,
        t.amount_kzt,
        t.created_at,
        'Large transaction (>=5,000,000 KZT)' AS suspicion_reason
    FROM transactions t
    JOIN accounts fa ON t.from_account_id = fa.account_id
    JOIN customers c ON fa.customer_id = c.customer_id
    WHERE t.status = 'completed'
      AND COALESCE(t.amount_kzt,0) >= 5000000
),
frequent_transactions AS (
    SELECT
        NULL::bigint AS transaction_id,
        t.from_account_id,
        a.account_number AS from_account_number,
        a.customer_id,
        c.full_name,
        DATE_TRUNC('hour', t.created_at) AS hour_window,
        COUNT(*) AS txn_count,
        'High frequency (>10 txn/hour)' AS suspicion_reason
    FROM transactions t
    JOIN accounts a ON t.from_account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t.status = 'completed'
    GROUP BY t.from_account_id, a.account_number, a.customer_id, c.full_name, DATE_TRUNC('hour', t.created_at)
    HAVING COUNT(*) > 10
),
rapid_sequential AS (
    SELECT
        t.transaction_id,
        t.from_account_id,
        a.account_number AS from_account_number,
        a.customer_id,
        c.full_name,
        t.amount_kzt,
        t.created_at,
        LAG(t.created_at) OVER (PARTITION BY t.from_account_id ORDER BY t.created_at) AS prev_txn_time,
        'Rapid sequential transfers (<60s)' AS suspicion_reason
    FROM transactions t
    JOIN accounts a ON t.from_account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t.status = 'completed'
)
SELECT
    transaction_id,
    from_account_id,
    from_account_number,
    customer_id,
    full_name,
    suspicion_reason,
    amount_kzt,
    created_at
FROM large_transactions

UNION ALL

SELECT
    transaction_id,
    from_account_id,
    from_account_number,
    customer_id,
    full_name,
    suspicion_reason,
    NULL::numeric AS amount_kzt,
    hour_window AS created_at
FROM frequent_transactions

UNION ALL

SELECT
    transaction_id,
    from_account_id,
    from_account_number,
    customer_id,
    full_name,
    suspicion_reason,
    amount_kzt,
    created_at
FROM rapid_sequential
WHERE prev_txn_time IS NOT NULL
  AND EXTRACT(EPOCH FROM (created_at - prev_txn_time)) < 60
;

--mini test
/*
SELECT *
FROM suspicious_activity_view
ORDER BY created_at DESC
LIMIT 10;
-- no output

SELECT transaction_id, full_name, amount_kzt, suspicion_reason
FROM suspicious_activity_view
WHERE suspicion_reason LIKE '%Large%'
ORDER BY amount_kzt DESC
LIMIT 5;
-- no output

SELECT transaction_id, from_account_number, created_at, suspicion_reason
FROM suspicious_activity_view
WHERE suspicion_reason LIKE '%60s%'
ORDER BY created_at DESC
LIMIT 5;
-- no output

SELECT *
FROM suspicious_activity_view
WHERE suspicion_reason LIKE '%10%'
LIMIT 5;

--no output
*/

--task3

--before indexes test
-- run BEFORE creating indexes
/*
--Query: recent transactions for a sender (typical pattern)
EXPLAIN ANALYZE
SELECT transaction_id, amount, currency, status, created_at
FROM transactions
WHERE from_account_id = 5
  AND created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY created_at DESC
LIMIT 10;


/*
Limit  (cost=1.41..1.41 rows=1 width=84) (actual time=0.060..0.062 rows=2.00 loops=1)
  Buffers: shared hit=1
  ->  Sort  (cost=1.41..1.41 rows=1 width=84) (actual time=0.059..0.060 rows=2.00 loops=1)
        Sort Key: created_at DESC
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=1
        ->  Seq Scan on transactions  (cost=0.00..1.40 rows=1 width=84) (actual time=0.040..0.045 rows=2.00 loops=1)
              Filter: ((from_account_id = 5) AND (created_at >= (CURRENT_DATE - '7 days'::interval)))
              Rows Removed by Filter: 19
              Buffers: shared hit=1
Planning:
  Buffers: shared hit=4
Planning Time: 0.295 ms
Execution Time: 0.108 ms





*/
--Query: lookup account by account_number (exact match)

EXPLAIN ANALYZE
SELECT account_id, customer_id, balance, currency, is_active
FROM accounts
WHERE account_number = 'KZ11AAAA0000000001';

/*


Seq Scan on accounts  (cost=0.00..1.25 rows=1 width=45) (actual time=0.243..0.244 rows=0.00 loops=1)
  Filter: (account_number = 'KZ11AAAA0000000001'::text)
  Rows Removed by Filter: 10
  Buffers: shared hit=1
Planning Time: 0.183 ms
Execution Time: 0.275 ms




*/


--Query: active accounts for customer (uses partial index)
EXPLAIN ANALYZE
SELECT account_id, balance, currency
FROM accounts
WHERE customer_id = 1 AND is_active = true;
/*


Seq Scan on accounts  (cost=0.00..1.25 rows=1 width=40) (actual time=0.016..0.017 rows=1.00 loops=1)
  Filter: (is_active AND (customer_id = 1))
  Rows Removed by Filter: 9
  Buffers: shared hit=1
Planning Time: 0.116 ms
Execution Time: 0.031 ms




*/


--Query: case-insensitive email lookup
EXPLAIN ANALYZE
SELECT customer_id, full_name, email
FROM customers
WHERE LOWER(email) = 'patrick@example.com';

/*

Seq Scan on customers  (cost=0.00..1.30 rows=1 width=68) (actual time=1.804..1.805 rows=0.00 loops=1)
  Filter: (lower(email) = 'patrick@example.com'::text)
  Rows Removed by Filter: 10
  Buffers: shared hit=1
Planning Time: 0.155 ms
Execution Time: 1.832 ms



*/
--Query: audit_log JSONB search
EXPLAIN ANALYZE
SELECT log_id, table_name, new_values
FROM audit_log
WHERE new_values @> '{"transaction_id": 5}'
LIMIT 10;

/*


Limit  (cost=0.00..1.02 rows=1 width=72) (actual time=0.080..0.080 rows=0.00 loops=1)
  Buffers: shared hit=1 dirtied=1
  ->  Seq Scan on audit_log  (cost=0.00..1.02 rows=1 width=72) (actual time=0.077..0.077 rows=0.00 loops=1)
"        Filter: (new_values @> '{""transaction_id"": 5}'::jsonb)"
        Rows Removed by Filter: 3
        Buffers: shared hit=1 dirtied=1
Planning:
  Buffers: shared hit=8
Planning Time: 1.469 ms
Execution Time: 0.109 ms



*/

*/


-- 1) Composite B-tree index for fast per-sender recent transactions
CREATE INDEX IF NOT EXISTS idx_transactions_from_created_desc
ON transactions (from_account_id, created_at DESC);

-- 2) Hash index for exact account_number lookups
-- Note: in modern Postgres B-tree equality is also very fast; Hash is included per task requirements.
CREATE INDEX IF NOT EXISTS idx_accounts_account_number_hash
ON accounts USING HASH (account_number);

-- 3) GIN indexes on JSONB audit_log columns
CREATE INDEX IF NOT EXISTS idx_audit_log_new_values_gin
ON audit_log USING GIN (new_values);

CREATE INDEX IF NOT EXISTS idx_audit_log_old_values_gin
ON audit_log USING GIN (old_values);

-- 4) Partial index for active accounts only
CREATE INDEX IF NOT EXISTS idx_accounts_active_partial
ON accounts (customer_id, balance)
WHERE is_active = true;

-- 5) Expression index for case-insensitive email search
CREATE INDEX IF NOT EXISTS idx_customers_email_lower
ON customers (LOWER(email));

-- 6) Covering index (INCLUDE) for the most frequent query pattern on accounts by customer
CREATE INDEX IF NOT EXISTS idx_accounts_covering
ON accounts (customer_id) INCLUDE (balance, currency, is_active);

-- Optional extra: index to help suspicious_activity queries (fast lookups by amount_kzt and status)
CREATE INDEX IF NOT EXISTS idx_transactions_amountkzt_status
ON transactions (status, amount_kzt) WHERE status = 'completed';


/*
test after indexes


--Query: recent transactions for a sender (typical pattern)
EXPLAIN ANALYZE
SELECT transaction_id, amount, currency, status, created_at
FROM transactions
WHERE from_account_id = 5
  AND created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY created_at DESC
LIMIT 10;


/*
Limit  (cost=1.43..1.43 rows=1 width=84) (actual time=0.073..0.074 rows=2.00 loops=1)
  Buffers: shared hit=1
  ->  Sort  (cost=1.43..1.43 rows=1 width=84) (actual time=0.072..0.072 rows=2.00 loops=1)
        Sort Key: created_at DESC
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=1
        ->  Seq Scan on transactions  (cost=0.00..1.42 rows=1 width=84) (actual time=0.052..0.056 rows=2.00 loops=1)
              Filter: ((from_account_id = 5) AND (created_at >= (CURRENT_DATE - '7 days'::interval)))
              Rows Removed by Filter: 19
              Buffers: shared hit=1
Planning:
  Buffers: shared hit=36 read=2 dirtied=1
Planning Time: 2.701 ms
Execution Time: 0.114 ms




*/
--Query: lookup account by account_number (exact match)

EXPLAIN ANALYZE
SELECT account_id, customer_id, balance, currency, is_active
FROM accounts
WHERE account_number = 'KZ11AAAA0000000001';

/*
Limit  (cost=1.43..1.43 rows=1 width=84) (actual time=0.073..0.074 rows=2.00 loops=1)
  Buffers: shared hit=1
  ->  Sort  (cost=1.43..1.43 rows=1 width=84) (actual time=0.072..0.072 rows=2.00 loops=1)
        Sort Key: created_at DESC
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=1
        ->  Seq Scan on transactions  (cost=0.00..1.42 rows=1 width=84) (actual time=0.052..0.056 rows=2.00 loops=1)
              Filter: ((from_account_id = 5) AND (created_at >= (CURRENT_DATE - '7 days'::interval)))
              Rows Removed by Filter: 19
              Buffers: shared hit=1
Planning:
  Buffers: shared hit=36 read=2 dirtied=1
Planning Time: 2.701 ms
Execution Time: 0.114 ms


*/


--Query: active accounts for customer (uses partial index)
EXPLAIN ANALYZE
SELECT account_id, balance, currency
FROM accounts
WHERE customer_id = 1 AND is_active = true;

/*


Seq Scan on accounts  (cost=0.00..1.12 rows=1 width=40) (actual time=0.032..0.033 rows=1.00 loops=1)
  Filter: (is_active AND (customer_id = 1))
  Rows Removed by Filter: 9
  Buffers: shared hit=1
Planning Time: 0.167 ms
Execution Time: 0.055 ms



*/


--Query: case-insensitive email lookup
EXPLAIN ANALYZE
SELECT customer_id, full_name, email
FROM customers
WHERE LOWER(email) = 'patrick@example.com';

/*

Seq Scan on customers  (cost=0.00..1.15 rows=1 width=68) (actual time=0.040..0.040 rows=0.00 loops=1)
  Filter: (lower(email) = 'patrick@example.com'::text)
  Rows Removed by Filter: 10
  Buffers: shared hit=1
Planning:
  Buffers: shared hit=18 read=1
Planning Time: 2.158 ms
Execution Time: 0.061 ms



*/
--Query: audit_log JSONB search
EXPLAIN ANALYZE
SELECT log_id, table_name, new_values
FROM audit_log
WHERE new_values @> '{"transaction_id": 5}'
LIMIT 10;

/*

Limit  (cost=0.00..1.04 rows=1 width=72) (actual time=0.042..0.043 rows=0.00 loops=1)
  Buffers: shared hit=1
  ->  Seq Scan on audit_log  (cost=0.00..1.04 rows=1 width=72) (actual time=0.040..0.040 rows=0.00 loops=1)
"        Filter: (new_values @> '{""transaction_id"": 5}'::jsonb)"
        Rows Removed by Filter: 3
        Buffers: shared hit=1
Planning:
  Buffers: shared hit=34
Planning Time: 2.913 ms
Execution Time: 0.071 ms

*/







*/



--task 4
-- DROP old if exists
DROP PROCEDURE IF EXISTS process_salary_batch(VARCHAR, JSONB, OUT JSONB);

CREATE OR REPLACE PROCEDURE process_salary_batch(
    IN  p_company_account_number VARCHAR,
    IN  p_payments                JSONB,
    OUT p_result                  JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_company_account_id   INTEGER;
    v_company_balance      NUMERIC;
    v_total_amount         NUMERIC := 0;

    v_idx                  INTEGER;
    v_iin                  VARCHAR;
    v_amount               NUMERIC;
    v_desc                 TEXT;
    v_employee_account_id  INTEGER;

    v_success_count        INTEGER := 0;
    v_fail_count           INTEGER := 0;
    v_fail_details         JSONB := '[]'::JSONB;

    v_emp_ids              INTEGER[] := '{}';
    v_emp_amts             NUMERIC[] := '{}';
    v_total_debit          NUMERIC := 0;

    v_lock_acquired        BOOLEAN;
BEGIN
    -- Acquire advisory lock
    SELECT pg_try_advisory_lock(hashtext(p_company_account_number)) INTO v_lock_acquired;
    IF NOT v_lock_acquired THEN
        p_result := jsonb_build_object('success', false, 'message', 'Concurrent batch in progress');
        RETURN;
    END IF;

    -- Lock company account
    SELECT account_id, balance
    INTO v_company_account_id, v_company_balance
    FROM accounts
    WHERE account_number = p_company_account_number AND is_active = true
    FOR UPDATE;

    IF NOT FOUND THEN
        PERFORM pg_advisory_unlock(hashtext(p_company_account_number));
        p_result := jsonb_build_object('success', false, 'message', 'Company account not found or inactive');
        RETURN;
    END IF;

    -- Validate payments JSON and compute total
    IF p_payments IS NULL OR jsonb_typeof(p_payments) <> 'array' THEN
        PERFORM pg_advisory_unlock(hashtext(p_company_account_number));
        p_result := jsonb_build_object('success', false, 'message', 'Invalid payments JSONB');
        RETURN;
    END IF;

    FOR v_idx IN 0..jsonb_array_length(p_payments)-1 LOOP
        v_amount := ((p_payments->v_idx)->>'amount')::NUMERIC;
        v_total_amount := v_total_amount + COALESCE(v_amount,0);
    END LOOP;

    IF v_company_balance < v_total_amount THEN
        PERFORM pg_advisory_unlock(hashtext(p_company_account_number));
        p_result := jsonb_build_object('success', false, 'message', 'Insufficient company balance', 'available', v_company_balance, 'required', v_total_amount);
        RETURN;
    END IF;

    -- Process payments: use nested BEGIN..EXCEPTION blocks (implicit savepoint behaviour)
    FOR v_idx IN 0..jsonb_array_length(p_payments)-1 LOOP
        v_iin := (p_payments->v_idx)->>'iin';
        v_amount := ((p_payments->v_idx)->>'amount')::NUMERIC;
        v_desc := (p_payments->v_idx)->>'description';

        -- Nested block: acts like a savepoint; failures handled here won't abort outer transaction
        BEGIN
            -- find employee KZT account
            SELECT a.account_id
            INTO v_employee_account_id
            FROM accounts a
            JOIN customers c ON a.customer_id = c.customer_id
            WHERE c.iin = v_iin
              AND a.currency = 'KZT'
              AND a.is_active = true
            LIMIT 1;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'Employee account not found for IIN: %', v_iin;
            END IF;

            -- insert transaction record (salary bypasses daily limit)
            INSERT INTO transactions (
                from_account_id, to_account_id, amount, currency,
                exchange_rate, amount_kzt, type, status, description,
                created_at, completed_at
            ) VALUES (
                v_company_account_id,
                v_employee_account_id,
                v_amount, 'KZT',
                1.0, v_amount,
                'transfer', 'completed', COALESCE(v_desc,'salary'),
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );

            -- collect for bulk balance updates
            v_emp_ids := v_emp_ids || v_employee_account_id;
            v_emp_amts := v_emp_amts || v_amount;
            v_total_debit := v_total_debit + COALESCE(v_amount,0);
            v_success_count := v_success_count + 1;

        EXCEPTION WHEN OTHERS THEN
            -- handle per-payment failure, continue loop
            v_fail_count := v_fail_count + 1;
            v_fail_details := v_fail_details || jsonb_build_object(
                'iin', v_iin,
                'amount', v_amount,
                'error', SQLERRM
            );
            -- continue to next payment
        END;
    END LOOP;

    -- Atomic balance updates at the end
    UPDATE accounts
    SET balance = balance - v_total_debit
    WHERE account_id = v_company_account_id;

    IF array_length(v_emp_ids,1) IS NOT NULL THEN
        FOR v_idx IN 1..array_length(v_emp_ids,1) LOOP
            UPDATE accounts
            SET balance = balance + v_emp_amts[v_idx]
            WHERE account_id = v_emp_ids[v_idx];
        END LOOP;
    END IF;

    -- Release advisory lock
    PERFORM pg_advisory_unlock(hashtext(p_company_account_number));

    -- Build and return result
    p_result := jsonb_build_object(
        'success', true,
        'successful_count', v_success_count,
        'failed_count', v_fail_count,
        'failed_details', v_fail_details,
        'total_debited', v_total_debit
    );

    RETURN;
END;
$$;



/*
-- Materialized view (summary)
CREATE MATERIALIZED VIEW IF NOT EXISTS batch_salary_summary AS
SELECT
    DATE(t.completed_at) AS batch_date,
    a.account_number AS company_account,
    c.full_name AS company_name,
    COUNT(*) AS total_payments,
    SUM(t.amount) AS total_amount,
    MIN(t.created_at) AS batch_start,
    MAX(t.completed_at) AS batch_end
FROM transactions t
JOIN accounts a ON t.from_account_id = a.account_id
JOIN customers c ON a.customer_id = c.customer_id
WHERE t.type = 'transfer' AND t.status = 'completed'
GROUP BY DATE(t.completed_at), a.account_number, c.full_name
ORDER BY DATE(t.completed_at) DESC, a.account_number;

SELECT * FROM batch_salary_summary
ORDER BY batch_date DESC
LIMIT 20;

/*
output
<null>,EU00BRD0001,Brad Pitt,2,100,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00HMR0001,Homer Simpson,2,6000,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00MRT0001,Morty Smith,2,400,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00PAT0001,Patrick Star,2,4000,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00RCK0001,Rick Sanchez,2,1400,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00SPG0001,SpongeBob SquarePants,2,10000,2025-12-11 17:43:20.802379 +00:00,
<null>,KZ00STK0001,Tony Stark,2,200000,2025-12-11 17:43:20.802379 +00:00,
<null>,US00GGG0001,Gennady GGG Golovkin,2,200,2025-12-11 17:43:20.802379 +00:00,
<null>,US00TYS0001,Mike Tyson,2,300,2025-12-11 17:43:20.802379 +00:00,
2025-12-12,KZ00PAT0001,Patrick Star,1,10000,2025-12-12 09:19:02.745126 +00:00,2025-12-12 09:19:02.745126 +00:00


*/


-- Minimal test call (adjust account numbers / iins if necessary)
CALL process_salary_batch(
    'KZ11AAAA0000000004',
    '[
        {"iin":"770101000001","amount":300000,"description":"January salary"},
        {"iin":"880202000002","amount":250000,"description":"January salary"},
        {"iin":"990303000003","amount":200000,"description":"January salary"}
    ]'::jsonb,
    NULL
);

--output {"message": "Company account not found or inactive", "success": false}


*/

/*


-- TASK 4 — CONCURRENCY DEMO (TWO SESSIONS)

-- This demonstrates row-level locking and concurrent access control.

-- SESSION A (terminal 1)

-- Start a transaction and lock a row:
BEGIN;
SELECT account_id, balance 
FROM accounts 
WHERE account_number = 'KZ00SPG0001' 
FOR UPDATE;

/*
output:
2,510000.00

*/



-- DO NOT commit yet — keep this session open.

-- SESSION B (terminal 2)

-- This call will block because SESSION A holds a FOR UPDATE lock:
CALL process_transfer(
    'KZ00SPG0001',
    'KZ00PAT0001',
    1000,
    'KZT',
    'concurrency test',
    NULL
);

-- Expected result:
-- Session B waits (state = 'waiting') until Session A releases the lock.

/*

output

SUCCESS: tx=42; debited=1000 KZT; credited=1000 KZT; kzt=1000.0


*/



-- SESSION A (terminal 1)
    
-- Release lock and allow Session B to proceed:
COMMIT;

-- OPTIONAL MONITORING

SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE query LIKE '%process_transfer%' 
   OR query LIKE '%FOR UPDATE%';


/*
output

880,active,<null>,<null>,"SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE query LIKE '%process_transfer%'
   OR query LIKE '%FOR UPDATE%'"


*/


*/
