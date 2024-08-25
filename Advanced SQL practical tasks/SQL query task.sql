-- Task 1: Identify Authors with the Most Published Books
WITH books_count AS (
    SELECT authors.name, COUNT(books.book_id) AS count
    FROM authors
    LEFT JOIN books ON books.author_id = authors.author_id
    GROUP BY authors.name
)
SELECT name, count
FROM books_count
WHERE count > 3;


-- Task 2: Identify Books with Titles Containing 'The' Using Regular Expressions
WITH book_the_title AS (
    SELECT *
    FROM books
    WHERE title ~* 'the'
)
SELECT 
    bt.title, 
    authors.name AS author_name, 
    genres.genre_name, 
    bt.published_date
FROM book_the_title AS bt
LEFT JOIN authors ON authors.author_id = bt.author_id
LEFT JOIN genres ON genres.genre_id = bt.genre_id;


-- Task 3:Rank Books by Price within Each Genre Using the RANK() Window Function
WITH book_rank AS (
    SELECT 
        books.*, 
        RANK() OVER(PARTITION BY genre_id ORDER BY price DESC) AS rank_number
    FROM books
)
SELECT 
    book_rank.title, 
    book_rank.price, 
    genres.genre_name, 
    authors.name AS author_name, 
    book_rank.rank_number
FROM book_rank
LEFT JOIN authors ON authors.author_id = book_rank.author_id
LEFT JOIN genres ON genres.genre_id = book_rank.genre_id
WHERE book_rank.rank_number = 1;



-- Task 4: Bulk Update Book Prices by Genre
CREATE OR REPLACE PROCEDURE sp_bulk_update_book_prices_by_genres(
	p_genre_id INTEGER,
	p_percentage_change NUMERIC(5, 2)
)
LANGUAGE plpgsql
AS $$
BEGIN
	UPDATE books
	SET price = price * (1 + p_percentage_change / 100)
	WHERE genre_id=p_genre_id;
END; $$;

-- Check books with genre_id 3
SELECT * FROM books WHERE genre_id=3;

-- Update price for 5% with books with id 3
CALL sp_bulk_update_book_prices_by_genres(3, 5.00);



--Task 5: Update Customer Join Date Based on First Purchase
CREATE OR REPLACE sp_update_customer_join_date()
LANGUAGE plpgsql
AS $$
BEGIN
	UPDATE customers
	SET join_date=s.first_purchase_date
	FROM (
		SELECT customer_id, MIN(sale_date) AS first_purchase_date
		FROM sales
		GROUP BY customer_id
	) s 
	WHERE customers.customer_id=s.customer_id AND 
	customers.join_date > s.first_purchase_date;
END;
$$;

CALL sp_update_customer_join_date();


-- Task 6: Calculate Average Book Price by Genre
CREATE OR REPLACE FUNCTION fn_avg_price_by_genre(
	p_genre_id INTEGER
)
RETURNS NUMERIC(10, 2)
LANGUAGE plpgsql
AS $$
DECLARE
    avg_price NUMERIC(10, 2);
BEGIN
    SELECT AVG(price)
    INTO avg_price
    FROM books
    WHERE genre_id = p_genre_id;

    RETURN avg_price;
END;
$$;

SELECT fn_avg_price_by_genre(1);


-- Task 7: Get Top N Best-Selling Books by Genre
CREATE OR REPLACE FUNCTION fn_get_top_n_books_by_genre(
    p_genre_id INTEGER,
    p_top_n INTEGER
)
RETURNS TABLE(
    book_id INT,
    book_title VARCHAR,
    genre_name VARCHAR,
    total_sales_revenue NUMERIC
)
LANGUAGE plpgsql
AS 
$$
BEGIN 
    RETURN QUERY
        SELECT 
            books.book_id, 
            books.title, 
            genre.genre_name, 
            SUM(sales.quantity * books.price) AS total_sales_revenue
        FROM sales
        INNER JOIN books ON books.book_id = sales.book_id
        INNER JOIN genres genre ON books.genre_id = genre.genre_id
        WHERE books.genre_id = p_genre_id
        GROUP BY books.book_id, books.title, genre.genre_name
        ORDER BY total_sales_revenue DESC
        LIMIT p_top_n;
END;
$$;


SELECT * FROM fn_get_top_n_books_by_genre(1, 5);

--Task 8: Log Changes to Sensitive Data
CREATE TABLE CustomersLog (
	log_id SERIAL PRIMARY KEY,
	column_name VARCHAR(50),
	old_value TEXT,
	new_value TEXT,
	changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	changed_by VARCHAR(50) -- This assumes you can track the user making the change
);

CREATE OR REPLACE FUNCTION log_sensetive_data_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN 
	IF NEW.first_name IS DISTINCT FROM OLD.first_name THEN
  --IF NEW.first_name <> OLD.first_name THEN
		INSERT INTO CustomersLog(column_name, old_value, new_value, changed_by)
		VALUES ('first_name', OLD.first_name, NEW.first_name, current_user);
	END IF;

	IF NEW.last_name IS DISTINCT FROM OLD.last_name THEN
  --IF NEW.last_name <> OLD.last_name THEN
		INSERT INTO CustomersLog(column_name, old_value, new_value, changed_by)
		VALUES ('last_name', OLD.last_name, NEW.last_name, current_user);
	END IF;

	IF NEW.email IS DISTINCT FROM OLD.email THEN
  --IF NEW.email <> OLD.email THEN
		INSERT INTO CustomersLog(column_name, old_value, new_value, changed_by)
		VALUES ('email', OLD.email, NEW.email, current_user);
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER tr_log_sensetive_data_changes
AFTER UPDATE
ON customers
FOR EACH ROW
EXECUTE PROCEDURE log_sensetive_data_changes();

-- Update customers table
UPDATE customers
SET last_name='Walker'
-- Williams, Walker
where customer_id=1

-- Check log
SELECT * FROM CustomersLog

-- Task 9: Automatically Adjust Book Prices Based on Sales Volume
CREATE OR REPLACE FUNCTION adjust_book_price()
RETURNS TRIGGER
LANGUAGE plpgsql
AS 
$$
DECLARE 
	total_product_quantity INTEGER;
BEGIN
	SELECT sum(quantity) into total_product_quantity
	FROM sales
	WHERE book_id=NEW.book_id;

	IF total_product_quantity >= 10 THEN 
		UPDATE books
		SET price = price * 1.1
		WHERE book_id = NEW.book_id;
	END IF;
	RETURN NEW;
END;
$$;

CREATE TRIGGER tr_adjust_book_price
AFTER INSERT ON sales
FOR EACH ROW
EXECUTE FUNCTION adjust_book_price();

-- Check sales with 1 book_id
SELECT *
FROM sales
WHERE book_id=1;
-- only 6 books saled

 -- Check price --> 19.99
SELECT * FROM books WHERE book_id=1 

INSERT INTO sales(book_id, customer_id, quantity, sale_date)
VALUES (1, 4, 5, now())

-- Check price -> 21.99
SELECT * FROM books WHERE book_id=1 

-- Task 10: Archive Old Sales Records
CREATE TABLE SalesArchive (
    sale_id SERIAL PRIMARY KEY,
    book_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    FOREIGN KEY (book_id) REFERENCES Books(book_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE OR REPLACE PROCEDURE sp_archive_old_sales(p_cutoff_date DATE)
LANGUAGE plpgsql
AS
$$
DECLARE
    sales_cursor CURSOR FOR 
        SELECT * FROM Sales WHERE sale_date < p_cutoff_date;
    sales_record Sales%ROWTYPE;
BEGIN
    -- Open the cursor
    OPEN sales_cursor;

    -- Loop through each record fetched by the cursor
    LOOP
        -- Fetch the next row into sales_record
        FETCH sales_cursor INTO sales_record;
        EXIT WHEN NOT FOUND;

        -- Insert the record into the SalesArchive table
        INSERT INTO SalesArchive (
            sale_id,
            book_id,
			customer_id,
            quantity,
            sale_date
            -- Add other columns here based on your Sales table structure
        )
        VALUES (
            sales_record.sale_id,
            sales_record.book_id,
			sales_record.customer_id,
            sales_record.quantity,
            sales_record.sale_date
            -- Add other columns here based on your Sales table structure
        );

        -- Delete the record from the Sales table
        DELETE FROM Sales WHERE sale_id = sales_record.sale_id;
    END LOOP;

    -- Close the cursor
    CLOSE sales_cursor;
END;
$$;

-- Call function
CALL sp_archive_old_sales('2023-01-01');

-- Check SalesArchive
SELECT * FROM SalesArchive
	