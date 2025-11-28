-- Создаем таблицы --

-- Таблица customer --

CREATE TABLE IF NOT EXISTS customer (

    customer_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    DOB date,
    job_title text,
    job_industry_category text,
    wealth_segment text,
    deceased_indicator text,
    owns_car text,
    address text,
    postcode varchar,
    state text,
    country text,
    property_valuation integer
   
);

-- Таблица product --

CREATE TABLE IF NOT EXISTS product (

   product_id integer,
   brand text,
   product_line text,
   product_class text,
   product_size text,
   list_price numeric(10,2),
   standard_cost numeric(10,2)
  
);

-- Очищаем таблицу product от дубликатов --

CREATE TABLE product_cor AS
 SELECT *
 FROM (
  SELECT *
   ,ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY list_price DESC) AS rn
  FROM product)
 WHERE rn = 1;

-- Устанавливаем первичный ключ в таблице product_cor --

ALTER TABLE product_cor 
ADD PRIMARY KEY (product_id);

-- Таблица orders --

CREATE TABLE IF NOT EXISTS orders (

   order_id integer PRIMARY KEY,
   customer_id integer,
   order_date date,
   online_order boolean,
   order_status text

);

-- Таблица order_items --

CREATE TABLE IF NOT EXISTS order_items (

   order_item_id integer PRIMARY KEY,
   product_id integer,
   order_id integer,
   quantity integer,
   item_list_price_at_sale numeric(10,2),
   item_standard_cost_at_sale numeric(10,2)

);

-- Добавляем внешние ключи --

-- Связь orders -> customer

DELETE FROM orders 
WHERE customer_id NOT IN (SELECT customer_id FROM customer);

ALTER TABLE orders 
ADD CONSTRAINT fk_orders_customer 
FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- Связь order_items -> orders
DELETE FROM order_items 
WHERE order_id NOT IN (SELECT order_id FROM orders);

ALTER TABLE order_items 
ADD CONSTRAINT fk_order_items_orders 
FOREIGN KEY (order_id) REFERENCES orders(order_id);

-- Связь order_items -> product_cor

DELETE FROM order_items 
WHERE product_id NOT IN (SELECT product_id FROM product_cor);

ALTER TABLE order_items 
ADD CONSTRAINT fk_order_items_product 
FOREIGN KEY (product_id) REFERENCES product_cor(product_id);

-- Запрос 1 --
-- Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества --
SELECT
   job_industry_category,
   COUNT(customer_id) AS customer_cnt
FROM customer
GROUP BY job_industry_category
ORDER BY COUNT(customer_id) DESC;

-- Запрос 2 --
-- Найти общую сумму дохода (list_price*quantity) по всем подтвержденным заказам за каждый месяц по сферам деятельности клиентов. --
-- Отсортировать по году, месяцу и сфере деятельности --
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    job_industry_category, 
    SUM(list_price * quantity) AS revenue_ttl
FROM order_items AS oi
JOIN product AS p
ON oi.product_id = p.product_id
JOIN orders AS o
ON oi.order_id = o.order_id
JOIN customer AS c
ON o.customer_id = c.customer_id
WHERE order_status = 'Approved'
GROUP BY DATE_TRUNC('month', order_date), job_industry_category
ORDER BY DATE_TRUNC('month', order_date), job_industry_category;

-- Запрос 3 --
-- Вывести количество уникальных онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT. -- 
-- Включить бренды, у которых нет онлайн-заказов от IT-клиентов, с количеством 0 --
SELECT
    brand,
    COUNT (DISTINCT CASE
    	WHEN job_industry_category = 'IT'
    	    AND order_status = 'Approved'
    	    AND online_order = TRUE
    	THEN oi.order_id
    END
    ) AS order_cnt
FROM product AS p
LEFT JOIN order_items AS oi
ON p.product_id = oi.product_id
LEFT JOIN orders AS o
ON oi.order_id = o.order_id
LEFT JOIN customer AS c
ON o.customer_id = c.customer_id
GROUP BY brand;
-- Запрос 4 --
-- Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум и количество заказов, а также среднюю --
-- сумму заказа по каждому клиенту. Отсортировать результат по убыванию суммы всех заказов и количества заказов. Выполнить --
-- двумя способами: используя только GROUP BY и используя только оконные функции. Сравнить результат --
SELECT
    c.customer_id,
    SUM(quantity * item_list_price_at_sale) AS total_sales,
    MIN(quantity * item_list_price_at_sale) AS min_sales,
    MAX(quantity * item_list_price_at_sale) AS max_sales,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(quantity * item_list_price_at_sale) AS average_order_amount
FROM order_items AS oi
JOIN orders AS o
ON oi.order_id = o.order_id
JOIN customer AS c
ON o.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY total_sales DESC, order_count DESC;
--
WITH window_calculation AS (
    SELECT
        c.customer_id,
        SUM(quantity * item_list_price_at_sale) OVER (PARTITION BY c.customer_id) AS total_sales,
        MIN(quantity * item_list_price_at_sale) OVER (PARTITION BY c.customer_id) AS min_sales,
        MAX(quantity * item_list_price_at_sale) OVER (PARTITION BY c.customer_id) AS max_sales,
        AVG(quantity * item_list_price_at_sale) OVER (PARTITION BY c.customer_id) AS average_order_amount
    FROM order_items AS oi
    JOIN orders AS o ON oi.order_id = o.order_id
    JOIN customer AS c ON o.customer_id = c.customer_id
)
SELECT DISTINCT
    customer_id,
    total_sales,
    min_sales,
    max_sales,
    average_order_amount,
    (SELECT COUNT(DISTINCT order_id) 
     FROM orders 
     WHERE customer_id = window_calculation.customer_id) AS order_count
FROM window_calculation
ORDER BY total_sales DESC, order_count DESC;
-- Запрос 5 --
-- Найти имена и фамилии клиентов с топ-3 минимальной и топ-3 максимальной суммой транзакций за весь период --
-- (учесть клиентов, у которых нет заказов) --
WITH customer_sales AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_sales
    FROM customer c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
),
ranked_customers AS (
    SELECT
        first_name,
        last_name,
        total_sales,
        ROW_NUMBER() OVER (ORDER BY total_sales ASC) AS lowest_rank,
        ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS highest_rank
    FROM customer_sales
)
SELECT 
    first_name,
    last_name,
    total_sales,
    'Минимум' as type
FROM ranked_customers
WHERE lowest_rank <= 3
UNION ALL
SELECT 
    first_name,
    last_name,
    total_sales,
    'Максимум' as type
FROM ranked_customers
WHERE highest_rank <= 3
ORDER BY type, total_sales;
-- Запрос 6 --
-- Вывести только вторые транзакции клиентов (если они есть). Решить с помощью оконных функций. --
-- Если у клиента меньше двух транзакций, он не должен попасть в результат --
SELECT
    order_id,
    customer_id,
    order_date,
    online_order
FROM(
    SELECT
        order_id,
        customer_id,
        order_date,
        online_order,
        COUNT(*) OVER (PARTITION BY customer_id) AS transaction_count,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS transaction_num
    FROM orders
) AS ranked_orders
WHERE transaction_count >= 2
    AND transaction_num = 2;
-- Запрос 7 --
-- Вывести имена, фамилии и профессии клиентов, а также длительность максимального интервала (в днях) --
-- между двумя последовательными заказами. Исключить клиентов, у которых только один или меньше заказов --
WITH days_diff AS (
    SELECT 
        customer_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
        order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS days_diff,
        COUNT(*) OVER (PARTITION BY customer_id) AS order_count
    FROM orders
),
customer_max_intervals AS (
    SELECT 
        customer_id,
        MAX(days_diff) AS max_interval
    FROM days_diff
    WHERE days_diff IS NOT NULL
    GROUP BY customer_id
    HAVING COUNT(*) >= 1
)
SELECT 
    c.first_name,
    c.last_name,
    cmi.max_interval
FROM customer_max_intervals cmi
JOIN customer c ON cmi.customer_id = c.customer_id
ORDER BY c.first_name, c.last_name;
-- Запрос 8 --
-- Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния (wealth_segment). --
-- Вывести имя, фамилию, сегмент и общий доход. Если в сегменте менее 5 клиентов, вывести всех --
WITH customer_info AS (
    SELECT
        ROW_NUMBER() OVER(PARTITION BY wealth_segment ORDER BY revenue_generated DESC) AS top_position,
        first_name,
        last_name,
        wealth_segment
    FROM (
        SELECT
            c.customer_id,
            wealth_segment,
            SUM(quantity * oi.item_list_price_at_sale) AS revenue_generated,
            first_name,
            last_name
        FROM order_items AS oi
        JOIN orders AS o
        ON oi.order_id = o.order_id 
        JOIN customer AS c
        ON o.customer_id = c.customer_id 
        GROUP BY c.customer_id, wealth_segment
        )
)
SELECT
    first_name,
    last_name,
    wealth_segment,
    top_position
FROM customer_info
WHERE top_position <= 5
ORDER BY wealth_segment, top_position;
