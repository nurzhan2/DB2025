-- Task 1
CREATE OR REPLACE FUNCTION calculate_discount(
    original_price NUMERIC,
    discount_percent NUMERIC
)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN original_price - (original_price * discount_percent / 100);
END;
$$;

-- Task 2
CREATE OR REPLACE FUNCTION film_stats(
    p_rating VARCHAR,
    OUT total_films INTEGER,
    OUT avg_rental_rate NUMERIC
)
RETURNS RECORD
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT COUNT(*), AVG(rental_rate)
    INTO total_films, avg_rental_rate
    FROM film
    WHERE rating = p_rating;
END;
$$;

-- Task 3
CREATE OR REPLACE FUNCTION get_customer_rentals(
    p_customer_id INTEGER
)
RETURNS TABLE (
    rental_date DATE,
    film_title VARCHAR,
    return_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT r.rental_date::DATE, f.title, r.return_date::DATE
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    WHERE r.customer_id = p_customer_id
    ORDER BY r.rental_date;
END;
$$;

-- Task 4
CREATE OR REPLACE FUNCTION search_films(
    p_title_pattern VARCHAR
)
RETURNS TABLE (
    title VARCHAR,
    release_year INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT f.title, f.release_year
    FROM film f
    WHERE f.title ILIKE p_title_pattern
    ORDER BY f.title;
END;
$$;

CREATE OR REPLACE FUNCTION search_films(
    p_title_pattern VARCHAR,
    p_rating VARCHAR
)
RETURNS TABLE (
    title VARCHAR,
    release_year INTEGER,
    rating VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT f.title, f.release_year, f.rating
    FROM film f
    WHERE f.title ILIKE p_title_pattern
      AND f.rating = p_rating
    ORDER BY f.title;
END;
$$;
