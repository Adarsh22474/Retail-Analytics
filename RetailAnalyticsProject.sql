
/* Checking the no of rows in each table, useful for table summarisation */
SELECT 'Customers' AS TableName, COUNT(*) AS TotalRows FROM Customers
UNION ALL SELECT 'Orders', COUNT(*) FROM Orders
UNION ALL SELECT 'OrderPayments', COUNT(*) FROM Payments
UNION ALL SELECT 'OrderReview_Ratings', COUNT(*) FROM OrderReview
UNION ALL SELECT 'ProductsInfo', COUNT(*) FROM Products
UNION ALL SELECT 'Stores', COUNT(*) FROM Stores
ORDER BY TableName

/* Customer Table Primary Key Integrity Check */
select * from Customers where Custid is Null

select Custid, count(*) from Customers
group by Custid
having Count(*)>1
/* We have got no CustID that has null values or has duplicates*/


/* Order Table Primary Key Integrity Check */
SELECT order_id, COUNT(*) AS cnt
FROM Orders
GROUP BY order_id
HAVING COUNT(*) > 1 
/* We see that there are 9803 duplicate OrderID's. The data is given at item level. Even if we consider composite key of orderID 
and ProductID we see duplicates of 10225. It is only when we consider a composite key of OrderId, ProductID and Quantity that 
we get unique and non missing values */
select distinct order_id, product_id, Quantity 
from Orders
select * from Orders where order_id is Null or product_id is null or quantity is null

/* Product Table Primary Key Integrity Check */
SELECT product_id, COUNT(*) AS cnt
FROM Products
GROUP BY product_id
HAVING COUNT(*) > 1

/* Stores Table Primary Key Integrity Check */
SELECT StoreID, COUNT(*) AS cnt
FROM Stores
GROUP BY StoreID
HAVING COUNT(*) > 1;

/* Payment Table Primary Key Integrity Check */
SELECT order_id, payment_type, payment_value, COUNT(*) AS cnt
FROM Payments
GROUP BY order_id, payment_type, payment_value
HAVING COUNT(*) > 1;
/* In this payment table we get 615 duplicated records. This avoids us to use any column/s as primary key.*/


/*Foreign Key Consistency Check in Orders  ----> Customers */

SELECT *
FROM Orders o
Left JOIN Customers c ON o.Customer_id = c.Custid
where c.Custid is null

/*Foreign Key Consistency Check in Orders  ----> Products */
SELECT o.product_id
FROM Orders o
LEFT JOIN Products p ON o.product_id = p.product_id
WHERE p.product_id IS NULL AND o.product_id IS NOT NULL;

/*Foreign Key Consistency Check in Orders  ----> Stores */
SELECT o.Delivered_StoreID
FROM Orders o
LEFT JOIN Stores s ON o.Delivered_StoreID = s.StoreID
WHERE s.StoreID IS NULL AND o.Delivered_StoreID IS NOT NULL


/*Foreign Key Consistency Check in Orders  ----> Payments */
select distinct o.order_id from Orders as o
left join payments as p
on o.order_id= p.order_id
where p.order_id is null
/*Here, we have found out that the orders table contains 1 order_id from Orders table for which we dont have payments information  */


/*Foreign Key Consistency Check in Orders  ----> OrderReview */
select distinct o.order_id from Orders as o
left join OrderReview as Ord
on o.order_id= Ord.order_id
where Ord.order_id is null


/*Checks for Completeness/ Null Values in Tables*/
SELECT 
    SUM(CASE WHEN Customer_id IS NULL THEN 1 ELSE 0 END) AS Null_Customers,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS Null_Product,
    SUM(CASE WHEN Delivered_StoreID IS NULL THEN 1 ELSE 0 END) AS Null_Store,
    SUM(CASE WHEN Bill_date_timestamp IS NULL THEN 1 ELSE 0 END) AS Null_Date
FROM Orders;

SELECT 
    SUM(CASE WHEN Category = '#N/A' THEN 1 ELSE 0 END) AS Null_Category,
    SUM(CASE WHEN product_weight_g is null THEN 1 ELSE 0 END) AS Null_Weight
FROM Products;

update products set category = 'others' where category = '#N/A'
/*There are 623 #N/A entries in Category section. In order to get better consistency we replace it with Others as above */


/* Checking for Outliers in Orders table */


with cte1 
as
(
select order_id, Sum(Quantity) as TotalQty from Orders 
group by order_id
),
cte2 as
( select * from cte1
where TotalQty not between ( select avg(TotalQty)-(3*STDEV(TotalQty)) from cte1 ) and
 ( select avg(TotalQty)+(3*STDEV(TotalQty)) from cte1 )
 ),


/* Using Normality assumption and CLT, we find that there are 965 order_id's that are not within 3 times the Std Deviation from mean quantity
so these can be considered as outliers */
 cte3 
as
(
select order_id, Avg(MRP) as Avgprice from Orders 
group by order_id
),
cte4 as
( select * from cte3
where Avgprice not between ( select avg(Avgprice)-(3*STDEV(AvgPrice)) from cte3 ) and
 ( select avg(Avgprice)+(3*STDEV(Avgprice)) from cte3 )
 ),

 /* we find that there are 1757 order_id's that are not within 3 times the Std Deviation from mean quantity
so these can be considered as outliers */

cte5 as
( 
select order_id from cte4 
where order_id in ( select order_id from cte2)
)
select * from cte5

/* But if we find out outliers in terms of both quanity and MRP at the order level then we find that there are only 4 order_id's
that have prices as well as quantities as outliers.*/

select * from 
(
Select o.order_id,sum([Total Amount]) as TotAmt,sum(payment_value) as TotPmt from orders as o
left join payments as p
on o.order_id= p.order_id
group by o.order_id
) as x
where abs(TotAmt - TotPmt) > 1 



 with cte7 as 
 (
select order_id, avg(Discount) as avgdis from Orders
group by order_id
),
cte8 as
(
select * from cte7
where avgdis not between (select avg(avgdis)-(3*stdev(avgdis)) from cte7) 
and (select avg(avgdis)+(3*stdev(avgdis)) from cte7)
)
select * from cte8

/* These are 1365 records in which discounts can be considered as outliers*/

SELECT 
    MIN(Bill_date_timestamp) AS EarliestOrder,
    MAX(Bill_date_timestamp) AS LatestOrder
FROM Orders;
 /* We see that the Billtimestamp dates are in dmy format as well as mdy format, we need to resolve it in excel itself before importing */

 
 /* Checking if each order_id is associated with one bill_date_time_stamp*/
 SELECT 
    order_id,
    COUNT(DISTINCT Bill_date_timestamp) AS DistinctTimestamps
FROM Orders
GROUP BY order_id
HAVING COUNT(DISTINCT Bill_date_timestamp) > 1;
/* We see that there are 334 order_id's that have different bill_date_time_stamp. Since this is very less compared to our datset 
we can remove these records*/

/* Checking if each order_id is associated with one StoreID */
select order_id, count(distinct Delivered_StoreID), count(distinct Channel),COUNT(DISTINCT Bill_date_timestamp) from Orders
group by order_id
having count(distinct Delivered_StoreID)>1 or count(distinct Channel)>1  or COUNT(DISTINCT Bill_date_timestamp) > 1
/* We see that there are 1007 order_id's that have different StoreID. Since this is very less compared to our datset 
we can remove these records*/




-----------------------------------------------------------------------
----------------------- Data Cleaning Orders Table---------------------- 

select * from Orders_Final2
SELECT *
INTO Orders_Copy
FROM Orders;

--1 Deleting rows not betweeen 1st Sept,2021 and 1st december,2023--------
delete from Orders_Copy2
where Bill_date_timestamp not between '2021-09-01' and '2023-12-31'
--13 records are there that have been dropped 

--2. cumulative count problem----------
WITH QtyCheck AS (
    SELECT 
        order_id,
        product_id,
        Bill_date_timestamp,
        Quantity,
        SUM(Quantity) OVER (PARTITION BY order_id, product_id ORDER BY Bill_date_timestamp) AS cumulative_qty,
        LAG(Quantity) OVER (PARTITION BY order_id, product_id ORDER BY Bill_date_timestamp) AS prev_qty
    FROM Orders_Copy
)
SELECT *
FROM QtyCheck
WHERE Quantity > ISNULL(prev_qty, 0)
  AND Quantity <> cumulative_qty;  -- flag where recorded qty = cumulative pattern
  -- I have found 17183 records with cumulative count problem


-- Final cleaned table: keep all non-problem rows and for problem groups keep only the row with max quantity
WITH qty_seq AS (
    SELECT
        order_id,
        product_id,
        Bill_date_timestamp,
        Quantity,
        LAG(Quantity) OVER (PARTITION BY order_id, product_id ORDER BY Bill_date_timestamp) AS prev_qty
    FROM Orders_Copy
),
problem_pairs AS (
    -- pairs where we observed a cumulative increase at least once
    SELECT DISTINCT order_id, product_id
    FROM qty_seq
    WHERE prev_qty IS NOT NULL
      AND Quantity > prev_qty
),
ranked AS (
    SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY o.order_id, o.product_id
            ORDER BY o.Quantity DESC, o.Bill_date_timestamp DESC
        ) AS rn,
        CASE WHEN p.order_id IS NOT NULL THEN 1 ELSE 0 END AS is_problem
    FROM Orders_Copy o
    LEFT JOIN problem_pairs p
      ON o.order_id = p.order_id
     AND o.product_id = p.product_id
)

SELECT *
INTO Orders_Final
FROM ranked
WHERE (is_problem = 0)      -- keep all rows from non-problem groups
   OR (is_problem = 1 AND rn = 1)  -- from problem groups keep only the highest-qty row
ORDER BY order_id, product_id, Bill_date_timestamp;

alter table Orders_final
drop column rn, is_problem; 

select count(*)- (select count(*) from Orders_Final) from Orders_Copy;
--10145 rows are left out in treating cumulative problem, leaving us with 102492 records


--3. Each Order_id should be updated with Delivered_StoreID with highest Amount
WITH store_rank AS (
    SELECT
        order_id,
        Delivered_StoreID,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY SUM([Total Amount]) DESC
        ) AS rn
    FROM Orders_Final
    GROUP BY order_id,Delivered_StoreID
)
UPDATE o
SET o.Delivered_StoreID = sr.Delivered_StoreID
FROM Orders_Final o
INNER JOIN store_rank sr
    ON o.order_id = sr.order_id
WHERE sr.rn = 1;

SELECT order_id, COUNT(DISTINCT Delivered_StoreID) AS store_count
FROM Orders_Final
GROUP BY order_id
HAVING COUNT(DISTINCT Delivered_StoreID ) > 1;--Checking for order_id's with multiple store_id after cleaning.


--4. Each Order_id should be updated with Billtimestamp of earliest instance
WITH earliest AS (
    SELECT
        order_id,
        MIN(Bill_date_timestamp) AS earliest_ts
    FROM Orders_Final
    GROUP BY order_id
)
UPDATE o
SET o.Bill_date_timestamp = e.earliest_ts
FROM Orders_Final o
JOIN earliest e
  ON o.order_id = e.order_id
WHERE o.Bill_date_timestamp <> e.earliest_ts;

-- any order_ids still having multiple distinct bill timestamps?
SELECT order_id
FROM Orders_Final
GROUP BY order_id
HAVING COUNT(DISTINCT Bill_date_timestamp) > 1;


--5. Dropping orderid that do not have OrderPayments info
DELETE o
FROM Orders_Final o
LEFT JOIN Payments p
  ON o.order_id = p.order_id
WHERE p.order_id IS NULL; --1 row is deleted further leaving us with 102491 records


--6. Checking Order_id's with more than 1 Customer_id
SELECT order_id, COUNT(DISTINCT Customer_id) AS distinct_customers
FROM Orders_Final
GROUP BY order_id
HAVING COUNT(DISTINCT Customer_id) > 1;



WITH cust_agg AS (
    SELECT order_id, Customer_id, SUM([Total Amount]) AS total_amount, Max(Bill_date_timestamp) AS latest_ts
    FROM Orders_Final
    GROUP BY order_id, Customer_id
),
cust_rank AS (
    SELECT order_id, Customer_id,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY total_amount DESC, latest_ts DESC) AS rn
    FROM cust_agg
),
chosen AS (
    SELECT order_id, Customer_id AS chosen_customer
    FROM cust_rank
    WHERE rn = 1
)
UPDATE o
SET o.Customer_id = c.chosen_customer
FROM Orders_Final o
JOIN chosen c
  ON o.order_id = c.order_id
WHERE o.Customer_id <> c.chosen_customer
-- 1 row is updated now no order_id has more than one Customer_id

 


--Note: Matching Total Amount and Payment Value at order level after cleaning Orders table into Orders_final 
with ordercte as
( select order_id,sum([Total Amount]) as TotAmt from Orders_Final
group by order_id
),
paymentcte as
( select order_id,sum(Payment_value) as TotPmt from Payments
group by order_id
)
select o.order_id,TotAmt,TotPmt
from ordercte as o
inner join paymentcte as p
on p.order_id = o.order_id
where abs(TotAmt- TotPmt) >1 
-- After cleaning we get 3476 order_id's for which Total Amount and Total Payment is not matching.

--------------------------------------------------------------------------
--Data Cleaning on Store Table--------------------------------------------
SELECT *
--INTO Store_Final
FROM Store_Final;



SELECT *
FROM Store_Final
WHERE StoreID IN (
    SELECT StoreID
    FROM Store_Final
    GROUP BY StoreID
    HAVING COUNT(*) > 1
)
ORDER BY StoreID;
-- 1 duplicated record

WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY StoreID
            ORDER BY StoreID   
        ) AS rn
    FROM Store_Final
)
DELETE FROM ranked
WHERE rn > 1;




-------------------------------------------------------------------------------------------
--Data Cleaning on Order Review table------------------------------------------------------
SELECT
    order_id,
    AVG(CAST(Customer_Satisfaction_Score AS FLOAT)) AS avg_rating
INTO OrderReview_Aggregated
FROM OrderReview
GROUP BY order_id;

select * from OrderReview_Aggregated --Here we have 99441 records.

----------------------------------------------------------------------------------------------
-------------------- Data Cleaning on OrderPayments table---------------------------------------
SELECT
  order_id,
  SUM(CASE WHEN payment_type = 'Voucher' THEN payment_value ELSE 0 END)     AS Voucher,
  SUM(CASE WHEN payment_type = 'UPI/Cash' THEN payment_value ELSE 0 END)    AS [UPI/Cash],
  SUM(CASE WHEN payment_type = 'Credit_card' THEN payment_value ELSE 0 END) AS Credit_card,
  SUM(CASE WHEN payment_type = 'Debit_card' THEN payment_value ELSE 0 END)  AS Debit_card,
  -- total across known payment methods
  SUM(payment_value) AS [Total Amount]
into Payment_aggregated
FROM Payments
GROUP BY order_id
ORDER BY order_id;

select * from Payment_aggregated;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------- Customer 360 table-----------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

WITH orders_cust AS (
    SELECT
        o.Customer_id,
        COUNT(DISTINCT o.order_id)                          AS total_orders,
        SUM(o.[Total Amount])                               AS total_revenue,
        AVG(CAST(o.[Total Amount] AS FLOAT))                AS avg_order_value,
        SUM(o.Quantity)                                     AS total_quantity,
        COUNT(DISTINCT o.product_id)                        AS distinct_products,
        MIN(o.Bill_date_timestamp)                          AS first_order_date,
        MAX(o.Bill_date_timestamp)                          AS last_order_date,
        DATEDIFF(DAY, MIN(o.Bill_date_timestamp), MAX(o.Bill_date_timestamp)) AS tenure_days,
        SUM(o.[Total Amount])-SUM(o.[Cost per Unit])        AS total_profit,
        SUM(o.[Discount])                                   AS total_discount
    FROM Orders_Final o
    GROUP BY o.Customer_id
),
-- 2) Frequency & recency specifics (inactive days = recency using current max date)
max_date AS (
    SELECT MAX(Bill_date_timestamp) AS max_ts FROM Orders_Final
),

recency_freq AS (
    SELECT
      Customer_id,
      COUNT(DISTINCT o.order_id) AS frequency,   -- number of transactions (distinct orders)
      DATEDIFF(DAY, MAX(o.Bill_date_timestamp), (SELECT max_ts FROM max_date)) AS inactive_days
    FROM Orders_Final o
    group by Customer_id
),
-- 3) Payment aggregates per customer (sum by payment method + count of payment methods)
payments_cust AS (
    SELECT
        o.Customer_id,
        SUM(CASE WHEN p.Payment_type = 'Voucher' THEN p.payment_value ELSE 0 END)     AS amt_voucher,
        SUM(CASE WHEN p.Payment_type = 'UPI/Cash' THEN p.payment_value ELSE 0 END)    AS amt_upi_cash,
        SUM(CASE WHEN p.Payment_type = 'Credit_card' THEN p.payment_value ELSE 0 END) AS amt_credit_card,
        SUM(CASE WHEN p.Payment_type = 'Debit_card' THEN p.payment_value ELSE 0 END)  AS amt_debit_card,
        SUM(p.payment_value) AS payment_total,
        COUNT(DISTINCT p.Payment_type) AS num_payment_types
    FROM Payments p
    JOIN Orders_Final o ON p.order_id = o.order_id
    GROUP BY o.Customer_id  
),

-- 4) preferred payment method (by highest amount)
preferred_payment AS (
    SELECT
        Customer_id,
        Payment_type AS preferred_payment
    FROM (
        SELECT
            o.Customer_id,
            p.Payment_type,
            SUM(p.payment_value) AS total_by_method,
            ROW_NUMBER() OVER (PARTITION BY o.Customer_id ORDER BY SUM(p.Payment_value) DESC) AS rn
        FROM Payments p
        JOIN Orders_Final o ON p.order_id = o.order_id
        GROUP BY o.Customer_id, p.Payment_type
    ) t
    WHERE rn = 1
),

-- 5) Payment counts (number of transactions paid via each method) per customer
payment_counts AS (
    SELECT
        o.Customer_id,
        SUM(CASE WHEN p.Payment_type = 'Voucher' THEN 1 ELSE 0 END)     AS cnt_voucher,
        SUM(CASE WHEN p.Payment_type = 'UPI/Cash' THEN 1 ELSE 0 END)    AS cnt_upi_cash,
        SUM(CASE WHEN p.Payment_type = 'Credit_card' THEN 1 ELSE 0 END) AS cnt_credit_card,
        SUM(CASE WHEN p.Payment_type = 'Debit_card' THEN 1 ELSE 0 END)  AS cnt_debit_card
    FROM Orders_Final o
    LEFT JOIN Payments p ON o.order_id = p.order_id
    GROUP BY o.Customer_id
),

-- 6) Channel / instore/online/calling metrics per customer
channel_cust AS (
    SELECT
        Customer_id,
        SUM(CASE WHEN Channel = 'Instore' THEN 1 ELSE 0 END) AS trx_instore,
        SUM(CASE WHEN Channel = 'Online' THEN 1 ELSE 0 END)  AS trx_online,
        SUM(CASE WHEN Channel = 'Phone Delivery' THEN 1 ELSE 0 END)  AS trx_calling,
        COUNT(DISTINCT Channel) AS num_channels_used
    FROM Orders_Final
    GROUP BY Customer_id
),

-- 7) Distinct stores / cities used by customer
geo_cust AS (
    SELECT
        Customer_id,
        COUNT(DISTINCT Delivered_StoreID) AS distinct_stores
    FROM Orders_Final
    GROUP BY Customer_id
),

cities_cust AS (
    SELECT o.Customer_id,
           COUNT(DISTINCT c.customer_city) AS distinct_cities
    FROM Orders_Final o
    LEFT JOIN Customers as c ON o.Customer_id = c.Custid
    GROUP BY o.Customer_id
),

-- 8) category-level aggregates (distinct categories per customer, per-category metrics)
cust_category AS (
    SELECT
       o.Customer_id,
       COUNT(DISTINCT p.Category) AS distinct_categories,
       SUM(CASE WHEN p.Category IS NOT NULL THEN o.[Total Amount] ELSE 0 END) AS revenue
    FROM Orders_Final o
    LEFT JOIN Products p ON o.product_id = p.product_id
    GROUP BY o.Customer_id
),

-- 9) transactions with discount 
txn_flags AS (
    SELECT
        Customer_id,
        SUM(CASE WHEN Discount > 0 THEN 1 ELSE 0 END) AS txns_with_discount
    FROM Orders_Final
    GROUP BY Customer_id
),

-- 10) time-bucketed transactions and weekday/weekend
time_buckets AS (
    SELECT
        Customer_id,
        SUM(CASE WHEN DATEPART(HOUR, Bill_date_timestamp) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) AS MorningOrders,
        SUM(CASE WHEN DATEPART(HOUR, Bill_date_timestamp) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS AfternoonOrders,
        SUM(CASE WHEN DATEPART(HOUR, Bill_date_timestamp) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) AS NightOrders,
        SUM(CASE WHEN DATEPART(HOUR, Bill_date_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS Late_nightorders,
        SUM(CASE WHEN DATEPART(WEEKDAY, Bill_date_timestamp) IN (2,3,4,5,6) THEN 1 ELSE 0 END) AS Weekday_Count,
        SUM(CASE WHEN DATEPART(WEEKDAY, Bill_date_timestamp) IN (1,7) THEN 1 ELSE 0 END) AS Weekend_Count
    FROM Orders_Final
    GROUP BY Customer_id
),

-- 11) review aggregation per customer
reviews_cust AS (
    SELECT
        o.Customer_id,
        AVG(CAST(r.Customer_Satisfaction_Score AS FLOAT)) AS avg_rating
    FROM OrderReview r
    JOIN Orders_Final o ON r.order_id = o.order_id
    GROUP BY o.Customer_id
),

-- 12) preferred categories top category by revenue
preferred_category AS (
    SELECT Customer_id, Category AS top_category
    FROM (
        SELECT
            o.Customer_id,
            p.Category,
            SUM(o.[Total Amount]) AS cat_rev,
            ROW_NUMBER() OVER (PARTITION BY o.Customer_id ORDER BY SUM(o.[Total Amount]) DESC) AS rn
        FROM Orders_Final o
        LEFT JOIN Products p ON o.product_id = p.product_id
        GROUP BY o.Customer_id, p.Category
    ) t
    WHERE rn = 1
),

-- 13) Terciles by revenue
cust_revenue AS (
    SELECT Customer_id, total_revenue
    FROM orders_cust
),
segmentation AS (
    SELECT
        Customer_id,
        NTILE(3) OVER (ORDER BY total_revenue) AS revenue_tercile  -- 1=low,2=mid,3=high
    FROM cust_revenue
),

base_cust AS (
  -- choose the canonical set of customers to build 1 row each for.
  -- Use Customers master if you want all customers (including those with zero orders).
  -- Here I use distinct customers present in Orders_Final.
  SELECT DISTINCT Customer_id
  FROM Orders_Final
  WHERE Customer_id IS NOT NULL
)

SELECT
    bc.Customer_id AS Customer_id,              -- primary id
    sf.customer_city,
    sf.customer_state,
    sf.Gender,

    oc.first_order_date,
    oc.last_order_date,
    oc.tenure_days,
    rf.inactive_days AS recency_days,
    rf.frequency AS frequency,   -- number of distinct orders
    oc.total_revenue AS monetary_total_revenue,
    oc.total_profit AS total_profit,
    oc.total_discount AS total_discount,
    oc.total_quantity AS total_quantity,
    cust_category.distinct_categories AS distinct_categories,
    oc.distinct_products AS distinct_products, -- using oc alias

    txn_flags.txns_with_discount AS txns_with_discount,
    channel_cust.num_channels_used AS num_channels_used,
    cities_cust.distinct_cities AS distinct_cities,

    payments_cust.num_payment_types AS num_payment_types_used,
    payment_counts.cnt_voucher AS transactions_paid_voucher,
    payment_counts.cnt_credit_card AS transactions_paid_credit_card,
    payment_counts.cnt_debit_card AS transactions_paid_debit_card,
    payment_counts.cnt_upi_cash AS transactions_paid_upi_cash,
    preferred_payment.preferred_payment AS preferred_payment_method,

    tb.Weekday_Count AS transactions_weekday,
    tb.Weekend_Count AS transactions_weekend,
    tb.NightOrders AS NightOrders,
    tb.AfternoonOrders AS AfternoonOrders,
    tb.MorningOrders AS MorningOrders,
    tb.Late_nightorders   AS LateNightOrders,

    rev.avg_rating AS avg_rating,

    seg.revenue_tercile AS revenue_segment  -- 1 low,2 mid,3 high

INTO Customer_360
FROM base_cust bc
LEFT JOIN Customers sf ON bc.Customer_id = sf.Custid
LEFT JOIN orders_cust oc ON bc.Customer_id = oc.Customer_id
LEFT JOIN recency_freq rf ON bc.Customer_id = rf.Customer_id
LEFT JOIN payments_cust ON bc.Customer_id = payments_cust.Customer_id
LEFT JOIN preferred_payment ON bc.Customer_id = preferred_payment.Customer_id
LEFT JOIN payment_counts ON bc.Customer_id = payment_counts.Customer_id
LEFT JOIN channel_cust ON bc.Customer_id = channel_cust.Customer_id
LEFT JOIN cities_cust ON bc.Customer_id = cities_cust.Customer_id
LEFT JOIN cust_category ON bc.Customer_id = cust_category.Customer_id
LEFT JOIN txn_flags ON bc.Customer_id = txn_flags.Customer_id
LEFT JOIN time_buckets tb ON bc.Customer_id = tb.Customer_id
LEFT JOIN reviews_cust rev ON bc.Customer_id = rev.Customer_id
LEFT JOIN segmentation seg ON bc.Customer_id = seg.Customer_id
ORDER BY bc.Customer_id;




select * from Customer_360
where monetary_total_revenue<0

select count(distinct Customer_id) from Orders_Final






-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------Orders_360---------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


 
-- 1) Base aggregation per order (sums across order lines)
WITH order_base AS (
    SELECT
        o.order_id,
        MIN(o.Bill_date_timestamp)                    AS bill_ts,   -- earliest timestamp for order (or MAX if you prefer)
        MIN(o.Customer_id)                            AS Customer_id,
        MIN(o.Delivered_StoreID)                      AS Store_ID,
        MIN(o.Channel)                                AS Channel,
        COUNT(*)                                       AS records_count,       -- number of records
        SUM(o.Quantity)                                AS total_quantity,
        SUM(ISNULL(o.[Total Amount],0))                AS total_amount,
        SUM(ISNULL(o.[Discount],0))                    AS total_discount,
        SUM([Total Amount]) - sum([Cost Per Unit])     AS total_profit,
        AVG(CASE WHEN o.Quantity > 0 THEN (o.[Total Amount] / o.Quantity) END) AS avg_price_per_unit,
        COUNT(DISTINCT o.product_id)                   AS distinct_products
    FROM Orders_Final o
    GROUP BY o.order_id
),

-- 2) Category fields: distinct categories, top category by revenue, top product by qty
order_products AS (
    SELECT
        o.order_id,
        COUNT(DISTINCT p.Category) AS distinct_categories,
        SUM(o.[Total Amount]) AS order_rev -- used in ranking
    FROM Orders_Final o
    LEFT JOIN Products p ON o.product_id = p.product_id
    GROUP BY o.order_id
),
top_category AS (
    SELECT order_id, Category AS top_category
    FROM (
        SELECT o.order_id, p.Category,
               SUM(o.[Total Amount]) AS cat_rev,
               ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY SUM(o.[Total Amount]) DESC) AS rn
        FROM Orders_Final o
        LEFT JOIN Products p ON o.product_id = p.product_id
        GROUP BY o.order_id, p.Category
    ) t
    WHERE rn = 1
),
top_product AS (
    SELECT order_id, product_id AS top_product_by_qty
    FROM (
        SELECT order_id, product_id, SUM(Quantity) AS qty,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY SUM(Quantity) DESC) AS rn
        FROM Orders_Final
        GROUP BY order_id, product_id
    ) x
    WHERE rn = 1
),

-- 3) Payment breakdown per order and payment-mix percentages
 payments_by_order AS (
    SELECT
        order_id,
        SUM(CASE WHEN Payment_type = 'Voucher' THEN payment_value ELSE 0 END)     AS amt_voucher,
        SUM(CASE WHEN payment_type = 'UPI/Cash' THEN payment_value ELSE 0 END)    AS amt_upi_cash,
        SUM(CASE WHEN payment_type = 'Credit_card' THEN payment_value ELSE 0 END) AS amt_credit_card,
        SUM(CASE WHEN payment_type = 'Debit_card' THEN payment_value ELSE 0 END)  AS amt_debit_card,
        SUM(payment_value) AS total_paid,
        COUNT(DISTINCT payment_type) AS num_payment_methods
    FROM Payments
    GROUP BY order_id
),
payments_pct AS (
    SELECT
        p.*,
        CASE WHEN total_paid = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_voucher,0) / total_paid,2) END AS pct_voucher,
        CASE WHEN total_paid = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_upi_cash,0) / total_paid,2) END AS pct_upi_cash,
        CASE WHEN total_paid = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_credit_card,0) / total_paid,2) END AS pct_credit_card,
        CASE WHEN total_paid = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_debit_card,0) / total_paid,2) END AS pct_debit_card
    FROM payments_by_order p
),

-- 4) Review info per order
reviews_by_order AS (
    SELECT order_id,
           AVG(CAST(Customer_Satisfaction_Score AS FLOAT)) AS avg_rating,
           COUNT(*) AS review_count
    FROM OrderReview 
    GROUP BY order_id
),

-- 5) Flags and checks per order
order_flags AS (
    SELECT
        ob.order_id,
        CASE WHEN ob.total_discount > 0 THEN 1 ELSE 0 END AS has_discount,
        CASE WHEN ob.total_profit < 0 THEN 1 ELSE 0 END AS has_loss,
        CASE WHEN ob.total_amount = 0 THEN 1 ELSE 0 END AS zero_amount_flag,
        CASE WHEN ob.total_quantity > 1 THEN 1 ELSE 0 END AS multi_item_flag
    FROM order_base ob
),

-- 6) Time features per order
time_features AS (
    SELECT
        order_id,
        DATEPART(YEAR, bill_ts) AS order_year,
        DATEPART(MONTH, bill_ts) AS order_month,
        DATEPART(DAY, bill_ts) AS order_day,
        DATEPART(HOUR, bill_ts) AS order_hour,
        DATENAME(WEEKDAY, bill_ts) AS weekday_name,
        CASE WHEN DATEPART(WEEKDAY, bill_ts) IN (2,3,4,5,6) THEN 'Weekday' ELSE 'Weekend' END AS day_type,
        CASE
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 6 AND 11 THEN '06-12'
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 12 AND 17 THEN '12-18'
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 18 AND 23 THEN '18-24'
            ELSE '00-06'
        END AS time_bucket
    FROM (
        SELECT order_id, MIN(Bill_date_timestamp) AS bill_ts
        FROM Orders_Final
        GROUP BY order_id
    ) x
),

-- 7) Customer-level snapshots for each order
customer_info AS (
    SELECT Custid AS Customer_id, customer_city, customer_state, Gender
    FROM Customers
),

-- 8) Store info
store_info AS (
    SELECT StoreID, seller_city AS store_city, seller_state AS store_state
    FROM Store_Final
)

-- 9) Final assembly into Orders_Level_Enhanced
SELECT
    ob.order_id,
    ob.Customer_id,
    ci.customer_city,
    ci.customer_state,
    ci.Gender,
    ob.Store_ID,
    si.store_city,
    si.store_state,
    ob.Channel,
    ob.bill_ts as Bill_date_timestamp,
    ob.records_count,
    ob.total_quantity,
    ob.distinct_products,
    op.distinct_categories,
    tc.top_category,
    tp.top_product_by_qty,
    ob.total_amount,
    ob.total_discount,
    ob.total_profit,
    ob.avg_price_per_unit,
    ROUND(CASE WHEN ob.total_amount = 0 THEN 0 ELSE (ob.total_profit / ob.total_amount) * 100 END,2) AS profit_margin_pct,
    -- Payments
   pp.amt_voucher    AS amt_voucher,
   pp.amt_upi_cash    AS amt_upi_cash,
   pp.amt_credit_card AS amt_credit_card,
   pp.amt_debit_card  AS amt_debit_card,
   pp.total_paid     AS total_paid,
   pp.num_payment_methods AS num_payment_methods,
   pp.pct_voucher     AS pct_voucher,
   pp.pct_upi_cash   AS pct_upi_cash,
   pp.pct_credit_card AS pct_credit_card,
   pp.pct_debit_card  AS pct_debit_card,
    -- Reviews
    ISNULL(r.avg_rating, NULL)   AS avg_rating,
    ISNULL(r.review_count, 0)    AS review_count,
    -- flags
    ISNULL(f.has_discount,0)     AS has_discount,
    ISNULL(f.has_loss,0)         AS has_loss,
    ISNULL(f.zero_amount_flag,0) AS zero_amount_flag,
    ISNULL(f.multi_item_flag,0)  AS multi_item_flag,
    -- time features
    tf.order_year,
    tf.order_month,
    tf.order_day,
    tf.order_hour,
    tf.weekday_name,
    tf.day_type,
    tf.time_bucket
INTO Orders_360
FROM order_base as ob
LEFT JOIN order_products as op         ON ob.order_id = op.order_id
LEFT JOIN top_category as tc           ON ob.order_id = tc.order_id
LEFT JOIN top_product as tp            ON ob.order_id = tp.order_id
LEFT JOIN payments_by_order as p       ON ob.order_id = p.order_id
LEFT JOIN payments_pct as pp           ON p.order_id = pp.order_id
LEFT JOIN reviews_by_order as r        ON ob.order_id = r.order_id
LEFT JOIN order_flags as f             ON ob.order_id = f.order_id
LEFT JOIN time_features as tf          ON ob.order_id = tf.order_id
LEFT JOIN customer_info as ci          ON ob.Customer_id = ci.Customer_id
LEFT JOIN store_info as si             ON ob.Store_ID = si.StoreID
ORDER BY ob.order_id;

select order_id, count(*) from Orders_360 group by order_id having count(*)>1;




-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------Stores_360---------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 1) Basic aggregations per store
with 
store_base as 
(
    SELECT
        o.Delivered_StoreID,
        COUNT(DISTINCT o.order_id)                                AS total_orders,
        SUM(o.[Total Amount])                                     AS total_revenue,
        SUM(o.[Total Amount])-sum([Cost per Unit])                AS total_profit,
        SUM(o.[Discount])                                         AS total_discount,
        SUM(o.Quantity)                                           AS total_quantity,
        AVG(CAST(o.[Total Amount] AS FLOAT))                      AS avg_order_value,
        AVG(CAST(o.Quantity AS FLOAT))                            AS avg_items_per_store,
        COUNT(DISTINCT o.product_id)                              AS distinct_products,
        COUNT(DISTINCT o.Customer_id)                             AS distinct_customers,
        MIN(o.Bill_date_timestamp)                                AS first_txn_date,
        MAX(o.Bill_date_timestamp)                                AS last_txn_date,
        DATEDIFF(DAY, MIN(o.Bill_date_timestamp), MAX(o.Bill_date_timestamp)) AS active_days
    FROM Orders_Final o
    GROUP BY o.Delivered_StoreID
),

-- 2) Payment sums per store

payments_store AS (
    SELECT
        o.Delivered_StoreID,
        SUM(CASE WHEN p.payment_type = 'Voucher' THEN p.payment_value ELSE 0 END)     AS amt_voucher,
        SUM(CASE WHEN p.payment_type = 'UPI/Cash' THEN p.payment_value ELSE 0 END)    AS amt_upi_cash,
        SUM(CASE WHEN p.payment_type = 'Credit_card' THEN p.payment_value ELSE 0 END) AS amt_credit_card,
        SUM(CASE WHEN p.payment_type = 'Debit_card' THEN p.payment_value ELSE 0 END)  AS amt_debit_card,
        SUM(p.payment_value) AS total_payments
    FROM Payments p
    JOIN Orders_Final o ON p.order_id = o.order_id
    GROUP BY o.Delivered_StoreID
),

-- 3) Payment percentage mix per store

payments_pct AS (
    SELECT
        ps.*,
        CASE WHEN total_payments = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_voucher,0) / total_payments,2) END AS pct_voucher,
        CASE WHEN total_payments = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_upi_cash,0) / total_payments,2) END AS pct_upi_cash,
        CASE WHEN total_payments = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_credit_card,0) / total_payments,2) END AS pct_credit_card,
        CASE WHEN total_payments = 0 THEN 0 ELSE ROUND(100.0 * ISNULL(amt_debit_card,0) / total_payments,2) END AS pct_debit_card
    FROM payments_store ps
),

-- 4) Channel breakdown per store (Instore/Online/Calling)
channel_store AS (
    SELECT
        Delivered_StoreID,
        SUM(CASE WHEN Channel = 'Instore' THEN 1 ELSE 0 END) AS trx_instore,
        SUM(CASE WHEN Channel = 'Online' THEN 1 ELSE 0 END)  AS trx_online,
        SUM(CASE WHEN Channel = 'Phone Delivery' THEN 1 ELSE 0 END)  AS trx_calling,
        COUNT(DISTINCT Channel) AS num_channels
    FROM Orders_Final
    GROUP BY Delivered_StoreID
),

-- 5) Time buckets per store
time_buckets_store AS (
    SELECT
        Delivered_StoreID,
        SUM(CASE WHEN DATEPART(HOUR,Bill_date_timestamp) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) AS cnt_06_12,
        SUM(CASE WHEN DATEPART(HOUR,Bill_date_timestamp) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS cnt_12_18,
        SUM(CASE WHEN DATEPART(HOUR,Bill_date_timestamp) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) AS cnt_18_24,
        SUM(CASE WHEN DATEPART(HOUR,Bill_date_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS cnt_00_06,
        SUM(CASE WHEN DATEPART(WEEKDAY,Bill_date_timestamp) IN (2,3,4,5,6) THEN 1 ELSE 0 END) AS cnt_weekday,
        SUM(CASE WHEN DATEPART(WEEKDAY,Bill_date_timestamp) IN (1,7) THEN 1 ELSE 0 END) AS cnt_weekend
    FROM Orders_Final
    GROUP BY Delivered_StoreID
),

-- 6) Discounted and loss transactions per store
flags_store AS (
    SELECT
        Delivered_StoreID,
        SUM(CASE WHEN [Discount] > 0 THEN 1 ELSE 0 END) AS txns_with_discount,
        SUM(CASE WHEN [Total Amount]-[Cost Per Unit]  < 0 THEN 1 ELSE 0 END) as txns_with_loss
    FROM Orders_Final
    GROUP BY Delivered_StoreID
),

-- 7) Top categories per store by revenue
top_category_store AS (
    SELECT Delivered_StoreID, Category AS top_category
    FROM (
        SELECT o.Delivered_StoreID, p.Category,
               SUM(o.[Total Amount]) AS cat_rev,
               ROW_NUMBER() OVER (PARTITION BY o.Delivered_StoreID ORDER BY SUM(o.[Total Amount]) DESC) AS rn
        FROM Orders_Final o
        LEFT JOIN Products p ON o.product_id = p.product_id
        GROUP BY o.Delivered_StoreID, p.Category
    ) t
    WHERE rn = 1
),

-- 8) Top product by quantity per store
top_product_store AS (
    SELECT Delivered_StoreID, product_id AS top_product_by_qty
    FROM (
        SELECT Delivered_StoreID, product_id, SUM(Quantity) AS qty,
               ROW_NUMBER() OVER (PARTITION BY Delivered_StoreID ORDER BY SUM(Quantity) DESC) AS rn
        FROM Orders_Final
        GROUP BY Delivered_StoreID, product_id
    ) x
    WHERE rn = 1
),

-- 9) Avg ticket at store-level
ticket_store AS (
    SELECT
        Delivered_StoreID,
        AVG(CAST([Total Amount] AS FLOAT)) AS avg_ticket_value
    FROM Orders_Final
    GROUP BY Delivered_StoreID
),

-- 10) Customer metrics at store level
cust_repeat AS (
    SELECT
        Delivered_StoreID,
        COUNT(DISTINCT order_id) AS orders_count,
        COUNT(DISTINCT Customer_id) AS customers_count,
        CASE WHEN COUNT(DISTINCT Customer_id) = 0 THEN 0 ELSE CAST(COUNT(DISTINCT order_id) AS FLOAT) / COUNT(DISTINCT Customer_id) END AS orders_per_customer
    FROM Orders_Final
    GROUP BY Delivered_StoreID
),

-- 11) Store master info
store_master AS (
    SELECT StoreID,seller_city AS store_city, seller_state AS store_state
    FROM Store_Final
)

-- Final assembly into Store_Level table
SELECT
    sb.Delivered_StoreID,
    sm.store_city,
    sm.store_state,

    sb.total_orders,
    sb.total_revenue,
    sb.total_profit,
    sb.total_discount,
    sb.total_quantity,
    sb.avg_order_value,
    sb.distinct_products,
    sb.distinct_customers,
    sb.first_txn_date,
    sb.last_txn_date,
    sb.active_days,

    -- payment sums + mix
    ISNULL(pp.amt_voucher,0)     AS amt_voucher,
    ISNULL(pp.amt_upi_cash,0)    AS amt_upi_cash,
    ISNULL(pp.amt_credit_card,0) AS amt_credit_card,
    ISNULL(pp.amt_debit_card,0)  AS amt_debit_card,
    ISNULL(pp.total_payments,0)  AS total_payments,
    ISNULL(pct.pct_voucher,0)    AS pct_voucher,
    ISNULL(pct.pct_upi_cash,0)   AS pct_upi_cash,
    ISNULL(pct.pct_credit_card,0) AS pct_credit_card,
    ISNULL(pct.pct_debit_card,0) AS pct_debit_card,

    -- channel
    ISNULL(ch.trx_instore,0) AS trx_instore,
    ISNULL(ch.trx_online,0)  AS trx_online,
    ISNULL(ch.trx_calling,0) AS trx_calling,
    ISNULL(ch.num_channels,0) AS num_channels,

    -- time buckets
    ISNULL(tb.cnt_06_12,0) AS cnt_06_12,
    ISNULL(tb.cnt_12_18,0) AS cnt_12_18,
    ISNULL(tb.cnt_18_24,0) AS cnt_18_24,
    ISNULL(tb.cnt_00_06,0) AS cnt_00_06,
    ISNULL(tb.cnt_weekday,0) AS cnt_weekday,
    ISNULL(tb.cnt_weekend,0) AS cnt_weekend,

    -- flags
    ISNULL(fl.txns_with_discount,0) AS txns_with_discount,
    ISNULL(fl.txns_with_loss,0)     AS txns_with_loss,

    -- top category/product
    tc.top_category,
    tp.top_product_by_qty,

    -- ticket & repeatability
    ISNULL(tk.avg_ticket_value,0) AS avg_ticket_value,
    ISNULL(cu.orders_per_customer,0) AS orders_per_customer

INTO Store_360
FROM store_base as sb
LEFT JOIN store_master as sm ON sb.Delivered_StoreID = sm.StoreID
LEFT JOIN payments_store as pp ON sb.Delivered_StoreID = pp.Delivered_StoreID
LEFT JOIN payments_pct as pct ON sb.Delivered_StoreID = pct.Delivered_StoreID
LEFT JOIN channel_store as ch ON sb.Delivered_StoreID = ch.Delivered_StoreID
LEFT JOIN time_buckets_store as tb ON sb.Delivered_StoreID = tb.Delivered_StoreID
LEFT JOIN flags_store as fl ON sb.Delivered_StoreID = fl.Delivered_StoreID
LEFT JOIN top_category_store as tc ON sb.Delivered_StoreID = tc.Delivered_StoreID
LEFT JOIN top_product_store as tp ON sb.Delivered_StoreID = tp.Delivered_StoreID
LEFT JOIN ticket_store as tk ON sb.Delivered_StoreID = tk.Delivered_StoreID
LEFT JOIN cust_repeat as cu ON sb.Delivered_StoreID = cu.Delivered_StoreID
ORDER BY sb.Delivered_StoreID;

select * from Store_360;





------------------------------------------------------------------------------
---------------------------Analysis-------------------------------------------

-------------------------------------------------------------------------------
------------------Exploratory Data Analysis------------------------------------
-------------------------------------------------------------------------------


--1) Total & average metrics
   --a)
SELECT
    COUNT(DISTINCT order_id)          AS total_orders,
    COUNT(DISTINCT Customer_id)       AS total_customers,
    COUNT(DISTINCT product_id)        AS total_products,
    SUM([Total Amount])               AS total_revenue,
    SUM([Total Amount]) / COUNT(DISTINCT order_id) AS avg_revenue_per_order,
    SUM([Cost per Unit])              AS total_cost,
    SUM([Total Amount]) - SUM([Cost per Unit]) AS total_profit,
    ((SUM([Total Amount]) - SUM([Cost per Unit]))*100.00)/ SUM([Total Amount]) as pct_profit,
    SUM(Discount)                     AS total_discount,
    (SUM(Discount)*100.00)/SUM([Total Amount]) as pct_discount,
    SUM(Quantity)                     AS total_quantity,
    SUM([Total Amount]) / COUNT(DISTINCT Customer_id) AS avg_revenue_per_customer,
    SUM(Quantity) / COUNT(DISTINCT Customer_id) AS avg_qty_sold_per_customer,
    SUM(Discount)/ COUNT(DISTINCT Customer_id) AS avg_discount_per_customer,
    (SUM([Total Amount]) - SUM([Cost per Unit])) / COUNT(DISTINCT Customer_id) AS avg_profit_per_customer,
    COUNT(*)*1.00 / COUNT(DISTINCT Customer_id) AS avg_orders_per_Customer
FROM Orders_Final


   --b) Other order level metrics
SELECT 
    SUM(total_amount)/ COUNT(*)        AS avg_revenue_per_order,
    SUM(total_quantity)/ COUNT(*)      AS avg_quantity_per_order,
    SUM(Total_discount)/ COUNT(*)      AS avg_discount_per_order,
    AVG(distinct_categories*1.00)      AS avg_category_per_order,
    AVG(total_profit)                  AS avg_profit_per_order,
    COUNT(distinct Store_ID)           AS Stores_count,
    COUNT(distinct Customer_State)     AS Customer_state_count,
    Count(distinct store_state)        AS StoreStates_count,
    Count(distinct Customer_city)      AS Customer_city_count,
    Count(distinct store_city)         AS StoreCity_count,
    COUNT(DISTINCT Channel)            AS total_channels,
    COUNT(DISTINCT Gender)             AS Gender_count
 FROM Orders_360

 --Median revenue each order
 Select PERCENTILE_CONT(0.50) within group (order by total_amount asc) over() from Orders_360
 
 
 --c) Number of payment types
SELECT
   Count(Distinct payment_type)  as payment_type_Count from Payments as p
   inner join Orders_360 as o
   on o.order_id = p.order_id

 --d) Number of Product categories
 SELECT 
   COUNT(DISTINCT Category) as catgory_count
   from Products as p
   inner join Orders_Final as o
   on o.product_id = p.product_id

    
 --e) One-time vs Repeated Buyers
     SELECT
         SUM(CASE WHEN frequency = 1 THEN 1 ELSE 0 END) AS one_time_buyers,
         SUM(CASE WHEN frequency > 1 THEN 1 ELSE 0 END) AS repeat_customers,
         CAST(SUM(CASE WHEN frequency > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,4)) AS pct_repeat_Customers
     FROM Customer_360;
         
 --f) Average Tenure between orders for each Customer in days
    SELECT 
    Avg(Cast(tenure_days/frequency as decimal)) as Average_tenure_between_orders
    FROM Customer_360
    where frequency>1

 
 
 --g) Retention metrics

     --i) New Customers by Month
    SELECT  
    SUM(CASE WHEN Month(first_order_date)=1 THEN 1 ELSE 0 END) AS Jan_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=2 THEN 1 ELSE 0 END) AS Feb_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=3 THEN 1 ELSE 0 END) AS Mar_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=4 THEN 1 ELSE 0 END) AS Apr_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=5 THEN 1 ELSE 0 END) AS May_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=6 THEN 1 ELSE 0 END) AS June_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=7 THEN 1 ELSE 0 END) AS July_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=8 THEN 1 ELSE 0 END) AS Aug_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=9 THEN 1 ELSE 0 END) AS Sept_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=10 THEN 1 ELSE 0 END) AS Oct_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=11 THEN 1 ELSE 0 END) AS Nov_New_Customers,
    SUM(CASE WHEN Month(first_order_date)=12 THEN 1 ELSE 0 END) AS Dec_New_Customers
    FROM Customer_360
    
   
    
    --ii) Monthly Customer Distribution  
    SELECT Year(Bill_date_timestamp) as Years, MONTH(Bill_date_timestamp) as Month_number,
    COUNT(distinct Customer_id) as Customer_count
    FROM Orders_360 
    GROUP BY Year(Bill_date_timestamp),MONTH(Bill_date_timestamp)
    ORDER BY Year(Bill_date_timestamp),MONTH(Bill_date_timestamp);


    --iii) Existing VS New Customer Revenue Trend
    WITH first_txn AS (
    SELECT Customer_id, MIN(Bill_date_timestamp) AS first_txn_date, SUM(SUM(total_amount)) over() as TotalRev
    FROM Orders_360
    GROUP BY Customer_id
    )
    SELECT
        Year(o.Bill_date_timestamp) as year, MONTH(o.Bill_date_timestamp) AS Month,
        SUM(CASE WHEN o.Bill_date_timestamp = f.first_txn_date THEN o.[Total Amount] ELSE 0 END) AS new_customer_revenue,
        SUM(CASE WHEN o.Bill_date_timestamp > f.first_txn_date THEN o.[Total Amount] ELSE 0 END) AS existing_customer_revenue,
        SUM(CASE WHEN o.Bill_date_timestamp = f.first_txn_date THEN o.[Total Amount] ELSE 0 END)/(SUM(CASE WHEN o.Bill_date_timestamp = f.first_txn_date THEN o.[Total Amount] ELSE 0 END)+ SUM(CASE WHEN o.Bill_date_timestamp > f.first_txn_date THEN o.[Total Amount] ELSE 0 END))*100 AS pct_newcustomer_revenue,
        SUM(CASE WHEN o.Bill_date_timestamp > f.first_txn_date THEN o.[Total Amount] ELSE 0 END)/(SUM(CASE WHEN o.Bill_date_timestamp = f.first_txn_date THEN o.[Total Amount] ELSE 0 END)+ SUM(CASE WHEN o.Bill_date_timestamp > f.first_txn_date THEN o.[Total Amount] ELSE 0 END))*100 AS pct_oldcustomer_revenue
    FROM Orders_Final as o
    JOIN first_txn as f ON o.Customer_id = f.Customer_id
    GROUP BY Year(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp)
    ORDER BY year, MONTH;


--h) Revenue By Channel
   SELECT
   Channel,
   COUNT(DISTINCT order_id) AS total_orders,
   SUM([Total Amount]) AS total_revenue,
   CAST(SUM([Total Amount]) * 100.0 / SUM(SUM([Total Amount])) OVER () AS DECIMAL(5,2)) AS revenue_share_pct
   FROM Orders_Final
   GROUP BY Channel
   ORDER BY total_revenue DESC;

 --i) Top 10 performing stores 
   SELECT TOP 10 
   Delivered_StoreID, Sum(total_revenue) as Revenue, Sum(total_revenue) * 100 /SUM(SUM(total_revenue)) over() as stores_share_in_totalrevenue
   FROM Store_360
   GROUP BY Delivered_StoreID
   ORDER BY Revenue Desc;

  --j) Bottom 10 performing Stores
   SELECT TOP 10 
   Delivered_StoreID, Sum(total_revenue) as Revenue, Sum(total_revenue) * 100 /SUM(SUM(total_revenue)) over() as stores_share_in_totalrevenue
   FROM Store_360
   GROUP BY Delivered_StoreID
   ORDER BY Revenue Asc;

  --k) Payment method analysis
   SELECT
      Payment_type,
      COUNT(DISTINCT order_id) AS transactions,
      SUM(payment_value) AS total_payment_value,
      CAST(SUM(payment_value) * 100.0 / SUM(SUM(payment_value)) OVER () AS DECIMAL(5,2)) AS contribution_pct
   FROM Payments 
   GROUP BY Payment_type
   ORDER BY total_payment_value DESC;

  --l) Monthly revenue trend
    SELECT
       YEAR(Bill_date_timestamp) AS Year,
       MONTH(Bill_date_timestamp) AS Month,
       SUM(total_amount) AS Total_amount,
       SUM(total_quantity) AS total_quantity
    FROM Orders_360
    GROUP BY Year(Bill_date_timestamp), MONTH(Bill_date_timestamp)
    ORDER BY Year(Bill_date_timestamp), MONTH(Bill_date_timestamp)

    
    --m) Month on Month Revenue Growth
    SELECT
    Years, months,
    Current_revenue- lag(Current_revenue) over ( order by years, Months) as revenue_growth
    FROM
    (
       Select 
       Year(Bill_date_timestamp) AS Years,
       Month(Bill_date_timestamp) AS Months,
       SUM(Total_amount) as Current_revenue
       FROM Orders_360
       GROUP BY Year(Bill_date_timestamp), Month(Bill_date_timestamp)
    ) X
   
   
  --n) Weekday vs Weekend Analysis
   SELECT
   SUM(CASE WHEN day_type = 'Weekday' THEN 1 ELSE 0 END) as Weekday_order_count,
   SUM(CASE WHEN day_type = 'Weekend' THEN 1 ELSE 0 END) as Weekend_order_count,
   SUM(CASE WHEN day_type = 'Weekday' THEN 1 ELSE 0 END) * 100.00 / COUNT(*) AS pct_Weekday,
   SUM(CASE WHEN day_type = 'Weekend' THEN 1 ELSE 0 END) * 100.00 / COUNT(*) AS pct_Weekend
   FROM Orders_360

   --o) Orders_ hour Analysis
   SELECT
   CAST(MorningOrders*100.00/Total_orders AS float) as pct_Morning_Orders,
   CAST(AfternoonOrders*100.00/Total_orders AS float) as pct_Afternoon_Orders,
   CAST(NightOrders*100.00/Total_orders AS Float) as pct_Night_Orders,
   CAST(Late_NightOrders*100.00/Total_orders AS Float) as pct_Latenight_Orders

   FROM
      (
      SELECT 
      SUM(CASE WHEN time_bucket = '06-12' THEN 1 ELSE 0 END) AS MorningOrders,
      SUM(CASE WHEN time_bucket = '12-18' THEN 1 ELSE 0 END) AS AfternoonOrders,
      SUM(CASE WHEN time_bucket = '18-24' THEN 1 ELSE 0 END) AS NightOrders,
      SUM(CASE WHEN time_bucket = '00-06' THEN 1 ELSE 0 END) AS Late_NightOrders,
      COUNT(*) AS Total_orders
      FROM orders_360
      ) x

      
  
  --p) Payment Channel Analysis
       SELECT
      Channel,
      COUNT(DISTINCT order_id) AS orders_count,
      SUM(total_amount) AS total_revenue,
      CAST(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER () AS DECIMAL(5,2)) AS contribution_pct
   FROM Orders_360 
   GROUP BY Channel
   ORDER BY total_revenue DESC;

   
   --q) 10 most expensive products
        With expensiveprod
        as
        (
        SELECT Distinct Top 10
        Product_id, MRP
        From Orders_Final
        Order by MRP Desc
        )
        SELECT Product_id, SUM([Total Amount]) as Revenue_most_expensive
        FROM Orders_Final
        where product_id in (select product_id from expensiveprod)
        group by product_id;

   
   
   --r) Category wise comparison
     SELECT p.category,
     SUM(o.[Total amount]) as category_wise_revenue,
     SUM(o.[Total amount])*100.00 / SUM( SUM(o.[Total amount])) over() as pct_category_revenue,
     SUM(o.Quantity) as category_wise_quantity,
     SUM(o.Quantity) *100.00 / SUM(SUM(o.Quantity)) over() as pct_category_quantity
     FROM Products as p
     inner join Orders_final as o
     on p.product_id = o.product_id
     Group by p.Category
     order by pct_category_Revenue desc
    -- We get that on the basis of 
    
    --s) Store State comparison
     SELECT Distinct store_state,
     SUM(total_amount) as state_wise_rvenue,
     SUM(total_amount)*100.00 / SUM( SUM(total_amount)) over() as pct_state_revenue,
     SUM(total_quantity) as state_wise_quantity,
     SUM(total_quantity) *100.00 / SUM(SUM(total_quantity)) over() as pct_state_quantity,
     COUNT(Distinct Customer_id ) as Statewise_Customer_footfall,
     COUNT(Distinct Customer_id) *100.00 / (Select count(*) from Customer_360) as Statewise_pct_customer
     from Orders_360
     GROUP BY store_state

    
    --t) Store city comparison
    SELECT Distinct store_city,
     SUM(total_amount) as city_wise_revenue,
      SUM(total_amount)*100.00 / SUM( SUM(total_amount)) over() as pct_city_revenue,
      SUM(total_quantity) as city_wise_quantity,
    SUM(total_quantity) *100.00 / SUM(SUM(total_quantity)) over() as pct_City_quantity
    from Orders_360
    GROUP BY store_city


    --u) StoreID comparison
    select Delivered_StoreID,
    SUM(total_revenue) *100.00 / SUM(Sum(total_revenue)) over() as Revenue_share_of_Store,
    SUM(total_orders) *100.00 / SUM(Sum(total_orders)) over() as Orders_share_of_Store,
    SUM(total_quantity) *100.00 / SUM(Sum(total_orders)) over() as quantity_share_of_Store,
    SUM(distinct_customers) *100.00 / SUM(SUM(distinct_customers)) over() as Share_of_customers_engaged
    from Store_360
    group by Delivered_StoreID
    order by Revenue_share_of_Store desc
    
    
    
    
    ------------------------------------------------------------------------------
    ----------------------------Customer Level Analysis---------------------------
    ------------------------------------------------------------------------------

    -- 1) Customer segmentation based on RFM
    SELECT distinct Revenue_segment, Count(Customer_id) as Customer_count
    from
    (
    SELECT
    Customer_ID,
    Total_Revenue,
    CASE
        WHEN Total_Revenue > 150 THEN 'High Value'
        WHEN Total_Revenue BETWEEN 75 AND 150 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS Revenue_Segment
FROM (
    SELECT
        Customer_ID,
        monetary_total_revenue AS Total_Revenue
    FROM Customer_360
    ) x
  ) y
  Group by Revenue_Segment



    --2) RFM segmentation and Segment wise Customer analysis
SELECT
  revenue_segmentation,
  Count(*) as Customer_count,
  Count(*)*100.00 / (select count(distinct customer_id) from Customer_360) as pct_Customer_count,
  SUM(Monetary_total_revenue) as segment_revenue,
  SUM(Monetary_total_revenue) * 100.00 / SUM(SUM(Monetary_total_revenue)) over() as pct_segment_revenue,
  SUM(Monetary_total_revenue) *1.00 / SUM(frequency) as Avg_order_value,
  AVG(Recency_days * 1.00) as avg_recency_days,
  AVG(frequency * 1.00) as avg_frequency
From
(
SELECT
  customer_id,
  monetary_total_revenue,
  frequency, recency_days,
  NTILE(5) OVER (ORDER BY recency_days  ASC)                               AS r_score,
  CASE WHEN frequency = 1 then 2 ELSE 1 END                                AS f_score,
  NTILE(5) OVER (ORDER BY monetary_total_revenue DESC)                     AS m_score,
  (NTILE(5) OVER (ORDER BY recency_days ASC)
   + CASE WHEN frequency = 1 then 2 ELSE 1 END 
   + NTILE(5) OVER (ORDER BY monetary_total_revenue DESC))                 AS rfm_score,
  CASE
    WHEN (NTILE(5) OVER (ORDER BY recency_days ASC)
          + CASE WHEN frequency = 1 then 2 ELSE 1 END 
          + NTILE(5) OVER (ORDER BY monetary_total_revenue DESC)) >= 12 THEN 'Standard'
    WHEN (NTILE(5) OVER (ORDER BY recency_days ASC)
          + CASE WHEN frequency = 1 then 2 ELSE 1 END 
          + NTILE(5) OVER (ORDER BY monetary_total_revenue DESC)) BETWEEN 9 AND 11 THEN 'Silver'
    WHEN (NTILE(5) OVER (ORDER BY recency_days ASC)
          + CASE WHEN frequency = 1 then 2 ELSE 1 END 
          + NTILE(5) OVER (ORDER BY monetary_total_revenue DESC)) BETWEEN 6 AND 8 THEN 'Gold'
    ELSE 'Premium'
  END AS revenue_segmentation
FROM customer_360
) x
Group by revenue_segmentation
  --3) Number of Channels used by each customer
  SELECT distinct Customer_id,
  count(distinct Channel) as Channels_for_each_Customer
  FROM Orders_360
  GROUP BY Customer_id
  Order by Channels_for_each_Customer desc

  /* There is only 1 Cutstomer that uses all the channels and there are only 8 Customers that use 2 channels. 
  Rest all the customers are using a single channel. Most of the remaining must be placing orders Instore as 87% of total orders are instore.*/
  


  --4) Discount Seekers vs Non Discount Seekers
   Select 
   Count(*) as Discount_seekers_count,
   Avg(monetary_total_revenue) as Avg_revenue_discountseekers,
   SUM(monetary_total_revenue)*1.00/ SUM(frequency) as AOV_Discountseekers,
   AVG(total_quantity * 1.00) as Avg_qunatity_Discountseekers,
   Count(*) * 100.00 / (select Count(*) from Customer_360) as pct_Discountseekers,
   SUM(total_profit) *100.00 /SUM(monetary_total_revenue) as avg_profit_Discountseekers
   From customer_360
   where total_discount>0 --40191 Customers are discount seekers
   
   --Non_Discount Seekers
   Select 
   Count(*) as NonDiscount_seekers_count,
   Avg(monetary_total_revenue) as Avg_revenue_Nondiscountseekers,
   SUM(monetary_total_revenue)*1.00/ SUM(frequency) as AOV_NonDiscountseekers,
   AVG(total_quantity * 1.00) as Avg_qunatity_NonDiscountseekers,
   Count(*) * 100.00 / (select Count(*) from Customer_360) as pct_NonDiscountseekers,
   SUM(total_profit) *100.00 /SUM(monetary_total_revenue) as avg_profit_NonDiscountseekers
   From customer_360
   where total_discount<=0 -- 58372 customers are not discount seekers

   --5) Gender wise classification 
   select 
   distinct Gender,
   Count(distinct Customer_id) as Gender_wise_Customer_count,
   SUM(total_amount) *100.0 / SUM(SUM(total_amount)) over() as Gender_wise_revenue_share
   from orders_360
   Group by Gender
   
    
   --6) Customers purchasing one category vs customers purchasing multiple categories

   select
   Count(*) as OneCat_Cust_count,
   SUM(monetary_total_revenue)*1.00/ SUM(frequency) as AOV_OneCat,
   AVG(total_quantity * 1.00) as Avg_qunatity_OneCat,
   Count(*) * 100.00 / (select Count(*) from Customer_360) as pct_OneCat,
   SUM(total_profit) *100.00 /SUM(monetary_total_revenue) as avg_profit_OneCat
   from Customer_360
   where distinct_categories = 1

    select
   Count(*) as MultipleCat_Cust_count,
   SUM(monetary_total_revenue)*1.00/ SUM(frequency) as AOV_MultipleCat,
   AVG(total_quantity * 1.00) as Avg_qunatity_MultipleCat,
   Count(*) * 100.00 / (select Count(*) from Customer_360) as pct_MultipleCat,
   SUM(total_profit) *100.00 /SUM(monetary_total_revenue) as avg_profit_MultipleCat
   from Customer_360
   where distinct_categories > 1

 --7) Gender wisde customer share and revenue contribution
 Select distinct Gender,
 count(customer_id) *100.00 / (select count(*) from Customer_360)as Gender_cust_share,
 Sum(monetary_total_revenue) *100.00 / SUM(SUM(monetary_total_revenue)) over() Gender_revenue_contribution,
 Sum(monetary_total_revenue)/ Count(customer_id) as avg_rev
 from Customer_360
 Group by Gender

 --8) Customers using various payment methods
   select 
   SUM(Case when transactions_paid_voucher=0 then 0 else 1 end) as customers_voucher,
   SUM(Case when transactions_paid_credit_card=0 then 0 else 1 end) as customers_credit_card,
   SUM(Case when transactions_paid_debit_card=0 then 0 else 1 end) as customers_debit_card,
   SUM(Case when transactions_paid_upi_cash=0 then 0 else 1 end) as customers_upi
   from customer_360

   --9) Customer prefernce of store state
   Select store_state,
   count(distinct Customer_id) as Cust_count
   from Orders_360
   group by Store_state
  ------------------------------------------------------------------------------------------------------
  ------------------------------------Cross-Selling ----------------------------------------------------
  ------------------------------------------------------------------------------------------------------

  WITH distinct_categories AS (
  SELECT DISTINCT order_id, Category
  FROM Orders_Final o
  INNER JOIN Products p
  ON o.product_id = p.product_id
)
SELECT distinct TOP 10
  di1.Category AS p1_id,
  di2.Category AS p2_id,
  COUNT(DISTINCT di1.order_id) AS orders_together
FROM distinct_categories di1
JOIN distinct_categories di2
  ON di1.order_id = di2.order_id
  AND di1.category < di2.category     -- ensures each unordered pair counted once
GROUP BY di1.category, di2.category
ORDER BY orders_together DESC;

With category3 
as (
select order_id from Orders_360 where distinct_categories>2
)
select o.*, Category from Orders_Final o
inner join Products p
on o.product_id = p.product_id
where order_id in (select order_id from Orders_360)









--------------------------------------------------------------------------------------
-------------------------------Category Level analysis--------------------------------
--------------------------------------------------------------------------------------

--1) Total Sales & Percentage of sales by category ( Pareto Analysis)

SELECT
    Category,
    SUM([Total Amount]) AS Total_Revenue,
    ROUND(SUM([Total Amount]) * 100.0 / SUM(SUM([Total Amount])) OVER (), 2) AS Pct_of_Total_Revenue,
    ROUND(SUM(SUM([Total Amount])) OVER (ORDER BY SUM([Total Amount]) DESC) 
          * 100.0 / SUM(SUM([Total Amount])) OVER (), 2) AS Cumulative_Pct_Revenue
FROM Orders_final as o
inner join Products as p
on o.product_id=p.product_id
GROUP BY Category
ORDER BY Total_Revenue DESC;

--2) Most profitable category and its contribution

SELECT
    category,
    SUM([Total Amount] - [Cost Per Unit]) AS total_profit,
    ROUND(SUM([Total Amount] - [Cost Per Unit]) * 100.0
          / SUM(SUM([Total Amount] - [Cost Per Unit])) OVER (), 2) AS pct_profit_contribution
FROM Orders_Final o
inner join Products p
on o.product_id = p.product_id
GROUP BY category
ORDER BY total_profit DESC;


--3) Category Penetration Analysis by month on month (Category Penetration = number of orders containing the category/number of orders)

WITH monthly_totals AS (
    SELECT
        YEAR(bill_date_timestamp) AS yr,
        MONTH(bill_date_timestamp) AS mth,
        COUNT(DISTINCT order_id) AS total_orders
    FROM orders_360
    GROUP BY YEAR(bill_date_timestamp), MONTH(bill_date_timestamp)
),
category_monthly AS (
    SELECT
        YEAR(bill_date_timestamp) AS yr,
        MONTH(bill_date_timestamp) AS mth,
        category,
        COUNT(DISTINCT order_id) AS category_orders
    FROM Orders_Final o
    Inner Join Products p
    on o.product_id = p.product_id
    GROUP BY YEAR(bill_date_timestamp), MONTH(bill_date_timestamp), category
),
penetration AS (
    SELECT
        c.yr,
        c.mth,
        c.category,
        c.category_orders,
        m.total_orders,
        CAST(c.category_orders AS FLOAT) / m.total_orders * 100.0 AS penetration_pct
    FROM category_monthly c
    JOIN monthly_totals m
      ON c.yr = m.yr AND c.mth = m.mth
)
SELECT
    yr AS [year],
    mth AS [month],
    category,
    ROUND(penetration_pct, 2) AS category_penetration_pct,
    ROUND(
      penetration_pct
      - LAG(penetration_pct) OVER (PARTITION BY category ORDER BY yr, mth),2) AS MoM_change_pct
FROM penetration
ORDER BY category, yr, mth;


--4) Overall monthly category wise avearge
  SELECT
    Datepart(Year,Bill_date_timestamp) yr,
    Datepart(month,Bill_date_timestamp) as month,
    COUNT(*) AS total_bills,
    ROUND(AVG(CAST(distinct_categories AS FLOAT)), 3) AS avg_categories_per_bill
  FROM Orders_360
  GROUP BY Datepart(Year,Bill_date_timestamp), Datepart(month,Bill_date_timestamp)
  Order By yr asc, month asc

--5) Category average by state

  SELECT
    Datepart(Year,Bill_date_timestamp) as yr,
    Datepart(month,Bill_date_timestamp) as month,
    customer_state,
    COUNT(*) AS total_bills,
    ROUND(AVG(CAST(distinct_categories AS FLOAT)), 3) AS avg_categories_per_bill
  FROM Orders_360
  GROUP BY Datepart(Year,Bill_date_timestamp) ,Datepart(month,Bill_date_timestamp) , customer_state
  Order By yr, month,avg_categories_per_bill

  
  --6) Category average by store
  SELECT
   store_state,
   COUNT(*) AS total_bills,
    ROUND(AVG(CAST(distinct_categories AS FLOAT)), 3) AS avg_categories_per_bill
  FROM Orders_360
  Group by store_state
  Order by avg_categories_per_bill desc;


  --7)  First_order_categories analysis

    With first_orders as
   (
   select * from
   (
   select distinct *,
   Row_Number() over(partition by Customer_id order by bill_date_timestamp asc) as rn2
   from Orders_final
   ) x
   where rn2 =1
   ),
   first_orders_with_category as
   (
   select f.*,Category 
   from first_orders as f
   inner join products as p
   on  f.product_id = p.product_id
   )
   select distinct Category, count(*) as first_purchase_category_count,
   COUNT(*) *100.00 / SUM(COUNT(*)) over() as first_purchase_category_pct
   from first_orders_with_category
   group by Category
   Order by first_purchase_category_count desc

   
   --8) Penetration rate by category

   Select  distinct category, Count(*) * 100.00/ (select count(*) FROM Orders_360) AS penatration_pct
   from(
   Select o.*, Category
   from Orders_final as o
   inner join Products as p
   on o.product_id = p.product_id
   ) x
   Group by Category.

  

  ------------------------------------------------------------------------------------------------------------------------
  --------------------------------------------------Customer Satisfaction-------------------------------------------------
  ------------------------------------------------------------------------------------------------------------------------


  --1) Categories that are maximum rated & minimum rated 
  Select
  distinct category, AVG(avg_rating) as category_rating_average
  from
  (
  select o.*,Category,Avg_rating from Orders_final as o
  inner join products as p
  on o.product_id = p.product_id
  inner join OrderReview_Aggregated as r
  on o.order_id=r.order_id
  ) x
  group by category
  order by category_rating_average desc

  --2) Avearge rating by store
  Select Store_ID,AVG(avg_rating) as Avg_rating
  FROM Orders_360
  Group by Store_ID
  Order by Avg_rating desc


   --3) Average rating by State
   SELECT distinct customer_state, AVG(avg_rating) as AVG_rating
   from Orders_360
   Group by customer_state
   Order by AVG_rating desc

   --4) Average rating by Month
   Select distinct Datepart(YEAR,Bill_date_timestamp) Year, Month(Bill_date_timestamp) Month, AVG(avg_rating) as AVG_rating
   FROM Orders_360
   Group by Datepart(YEAR,Bill_date_timestamp), Month(Bill_date_timestamp)
   Order by year, month





   ----------------------------------------------------------------------------------------------------------
   ---------------------------------Sales, Trend, Seasonality & Patterns-------------------------------------
   ----------------------------------------------------------------------------------------------------------

   --1) Yearly and monthly sales percentage contribution
   Select year(Bill_date_timestamp) as year, month(Bill_date_timestamp) as Month, SUM(total_amount) as Revenue,
   SUM(total_amount) *100.00 /SUM(SUM(total_amount)) over() as pct_rev
   from Orders_360 
   Group by year(Bill_date_timestamp), month(Bill_date_timestamp)
   Order by year, month

   --2) Total sales by month
   Select distinct month(Bill_date_timestamp) as Month , SUM(total_amount) as Revenue,
   SUM(total_amount) *100.00 /SUM(SUM(total_amount)) over() as pct_rev
   from Orders_360
   Group by month(Bill_date_timestamp)
   order by month(Bill_date_timestamp) asc

   --3) Total sales by weekday
     Select distinct weekday_name as DayName , SUM(total_amount) as Revenue,
   SUM(total_amount) *100.00 /SUM(SUM(total_amount)) over() as pct_rev
   from Orders_360
   Group by weekday_name
   order by revenue desc

   --4) Total sales by time of day
    Select 
    distinct time_bucket,  SUM(total_amount) as Revenue,
   SUM(total_amount) *100.00 /SUM(SUM(total_amount)) over() as pct_rev
   from Orders_360
   group by time_bucket 
   order by revenue desc

   -------------------------------------------------------------------------------
   --------------------------------Cohort Analysis---------------------------------
  ---------------------------------------------------------------------------------

 --1) 
 WITH Customer_Cohort AS (
    -- Step 1: Determine each customer's first purchase (and cohort month)
    SELECT
        customer_id,
        MIN(CAST(Bill_date_timestamp AS DATE)) AS First_Purchase_Date,
        MAX(CAST(Bill_date_timestamp AS DATE)) AS Last_Purchase_Date,
        DATEFROMPARTS(
            YEAR(MIN(CAST(Bill_date_timestamp AS DATE))),
            MONTH(MIN(CAST(Bill_date_timestamp AS DATE))),
            1
        ) AS Cohort_Month
    FROM dbo.Orders_Final
    GROUP BY customer_id
),
Customer_Level AS (
    -- Step 2: For each customer, split their activity into cohort-month orders vs repeat orders
    SELECT
        cc.customer_id,
        cc.Cohort_Month,
        cc.First_Purchase_Date,
        cc.Last_Purchase_Date,
        DATEDIFF(DAY, cc.First_Purchase_Date, cc.Last_Purchase_Date) AS Gap_Days,
        CASE WHEN cc.First_Purchase_Date <> cc.Last_Purchase_Date THEN 1 ELSE 0 END AS Is_Repeat_Customer,
        SUM(CASE 
              WHEN DATEFROMPARTS(YEAR(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp), 1) = cc.Cohort_Month
              THEN 1 ELSE 0 
            END) AS Orders_In_Cohort_Month,
        SUM(CASE 
              WHEN DATEFROMPARTS(YEAR(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp), 1) = cc.Cohort_Month
              THEN o.[Total Amount] ELSE 0 
            END) AS Revenue_In_Cohort_Month,
        SUM(CASE 
              WHEN DATEFROMPARTS(YEAR(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp), 1) > cc.Cohort_Month
              THEN 1 ELSE 0 
            END) AS Orders_Repeat,
        SUM(CASE 
              WHEN DATEFROMPARTS(YEAR(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp), 1) > cc.Cohort_Month
              THEN o.[Total Amount] ELSE 0 
            END) AS Revenue_Repeat
    FROM Customer_Cohort cc
    JOIN dbo.Orders_Final o
      ON o.customer_id = cc.customer_id
    GROUP BY
        cc.customer_id, cc.Cohort_Month, cc.First_Purchase_Date, cc.Last_Purchase_Date
)
SELECT
    cl.Cohort_Month,
    COUNT(DISTINCT cl.customer_id) AS Cohort_Customers,
    SUM(CASE WHEN cl.Is_Repeat_Customer = 1 THEN 1 ELSE 0 END) AS Repeat_Customers,
    ROUND(SUM(CASE WHEN cl.Is_Repeat_Customer = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT cl.customer_id), 2) AS Retention_Pct,
    SUM(cl.Orders_In_Cohort_Month) AS Total_Orders_Cohort_Month,
    SUM(cl.Revenue_In_Cohort_Month) AS Total_Revenue_Cohort_Month,
    SUM(cl.Orders_Repeat) AS Total_Orders_Repeat,
    SUM(cl.Revenue_Repeat) AS Total_Revenue_Repeat,
    ROUND(AVG(CAST(cl.Gap_Days AS FLOAT)), 1) AS Avg_Gap_Days
FROM Customer_Level cl
GROUP BY cl.Cohort_Month
ORDER BY cl.Cohort_Month;



--2) Cohort matrix: one row per cohort, columns Month_0..Month_12 (counts + %)

WITH customer_first AS (
    -- first purchase date and cohort month for each customer
    SELECT
        customer_id,
        DATEFROMPARTS(YEAR(MIN(CAST(Bill_date_timestamp AS DATE))),
                      MONTH(MIN(CAST(Bill_date_timestamp AS DATE))), 1) AS cohort_month
    FROM dbo.Orders_Final
    GROUP BY customer_id
),
order_months AS (
    -- map each order to month-start
    SELECT
        o.customer_id,
        DATEFROMPARTS(YEAR(o.Bill_date_timestamp), MONTH(o.Bill_date_timestamp), 1) AS order_month
    FROM dbo.Orders_Final o
),
-- distinct customer activity per cohort_month and order_month
cohort_activity AS (
    SELECT
        cf.cohort_month,
        om.order_month,
        DATEDIFF(MONTH, cf.cohort_month, om.order_month) AS cohort_age_months,
        COUNT(DISTINCT cf.customer_id) AS distinct_customers_in_bucket
    FROM customer_first cf
    JOIN order_months om
      ON cf.customer_id = om.customer_id
    WHERE DATEDIFF(MONTH, cf.cohort_month, om.order_month) BETWEEN 0 AND 30
    GROUP BY cf.cohort_month, om.order_month
),
cohort_sizes AS (
    -- cohort size is number of customers whose first purchase was in cohort_month
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM customer_first
    GROUP BY cohort_month
)
-- pivot counts into wide form and compute percentages
SELECT
    cs.cohort_month,
    cs.cohort_size,
    -- counts Month_0 .. Month_12
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 0 THEN ca.distinct_customers_in_bucket END),0) AS Month_0,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 1 THEN ca.distinct_customers_in_bucket END),0) AS Month_1,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 2 THEN ca.distinct_customers_in_bucket END),0) AS Month_2,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 3 THEN ca.distinct_customers_in_bucket END),0) AS Month_3,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 4 THEN ca.distinct_customers_in_bucket END),0) AS Month_4,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 5 THEN ca.distinct_customers_in_bucket END),0) AS Month_5,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 6 THEN ca.distinct_customers_in_bucket END),0) AS Month_6,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 7 THEN ca.distinct_customers_in_bucket END),0) AS Month_7,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 8 THEN ca.distinct_customers_in_bucket END),0) AS Month_8,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 9 THEN ca.distinct_customers_in_bucket END),0) AS Month_9,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 10 THEN ca.distinct_customers_in_bucket END),0) AS Month_10,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 11 THEN ca.distinct_customers_in_bucket END),0) AS Month_11,
    ISNULL(MAX(CASE WHEN ca.cohort_age_months = 12 THEN ca.distinct_customers_in_bucket END),0) AS Month_12,

    -- percentages Month_0_pct .. Month_12_pct (rounded 2 decimals)
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 0 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M0_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 1 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M1_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 2 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M2_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 3 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M3_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 4 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M4_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 5 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / NULLIF(cs.cohort_size,0), 2) AS M5_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 6 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M6_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 7 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M7_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 8 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / NULLIF(cs.cohort_size,0), 2) AS M8_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 9 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M9_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 10 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M10_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 11 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M11_pct,
    ROUND(
      ISNULL(MAX(CASE WHEN ca.cohort_age_months = 12 THEN ca.distinct_customers_in_bucket END),0) * 100.0
      / cs.cohort_size, 2) AS M12_pct
FROM cohort_sizes cs
LEFT JOIN cohort_activity ca
  ON ca.cohort_month = cs.cohort_month
GROUP BY cs.cohort_month, cs.cohort_size
ORDER BY cs.cohort_month;


