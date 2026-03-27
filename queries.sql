-- ============================================================
-- Online Retail Sales Analysis — SQL Window Functions
-- Dataset: UCI Online Retail (541,909 rows | Dec 2010 – Dec 2011)
-- Database: PostgreSQL 18
-- Author: Stefano Noventa
-- ============================================================

-- ============================================================
-- DATA QUALITY AUDIT
-- Verification of data completeness across all fields
-- to document known gaps and inform query design.
-- ============================================================

SELECT
    SUM(CASE WHEN invoice_no   IS NULL THEN 1 ELSE 0 END) AS null_invoice_no,
    SUM(CASE WHEN stock_code   IS NULL THEN 1 ELSE 0 END) AS null_stock_code,
    SUM(CASE WHEN description  IS NULL THEN 1 ELSE 0 END) AS null_description,
    SUM(CASE WHEN quantity     IS NULL THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN invoice_date IS NULL THEN 1 ELSE 0 END) AS null_invoice_date,
    SUM(CASE WHEN unit_price   IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN customer_id  IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN country      IS NULL THEN 1 ELSE 0 END) AS null_country
FROM retail;

-- Findings: customer_id has 135,080 NULLs (about 25% of rows).
-- These represent guest or untracked purchases.
-- All customer-level queries exclude NULLs with documented rationale.


-- ============================================================
-- QUERY 1: Regional Customer Value Ranking
-- Business Question: Who are the highest-value customers in each
-- country, and how do they rank relative to their regional peers?
-- Use Case: Regional sales prioritisation, VIP identification
-- Window Function: DENSE_RANK()
-- ============================================================

WITH spending AS (
    SELECT
        country,
        customer_id,
        SUM(quantity * unit_price) AS total_spend
    FROM retail
    -- Exclude NULL customer_id — no meaningful rank can be assigned to unidentified customers
    WHERE customer_id IS NOT NULL
    GROUP BY country, customer_id
)
SELECT
    *,
    -- DENSE_RANK() ensures no gaps in ranking when tied spend values are present
    DENSE_RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) AS customer_rank
FROM spending;


-- ============================================================
-- QUERY 2: Purchase History of Top 5 Global Customers
-- Business Question: What does the full purchase history look like
-- for our highest-spending customers, and how have their orders
-- evolved over time?
-- Use Case: Key account management, high-value customer retention
-- Window Functions: DENSE_RANK()
-- ============================================================

WITH top5_customers AS (
    SELECT
        customer_id,
        SUM(quantity * unit_price) AS total_spend,
        -- If two customers rank equally, both are included — query may return more than 5 customers
        DENSE_RANK() OVER (ORDER BY SUM(quantity * unit_price) DESC) AS customer_global_rank
    FROM retail
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    t5.customer_id,
    t5.customer_global_rank,
    r.invoice_date,
    -- Invoice rank per customer: 1 = most recent, increasing = older
    DENSE_RANK() OVER (PARTITION BY t5.customer_id ORDER BY r.invoice_date DESC, r.invoice_no) AS invoice_rank,
    r.description,
    r.quantity,
    r.unit_price
FROM top5_customers AS t5
JOIN retail AS r
    ON (t5.customer_id = r.customer_id) AND (r.customer_id IS NOT NULL)
WHERE customer_global_rank IN (1, 2, 3, 4, 5)
ORDER BY t5.customer_id, invoice_rank;


-- ============================================================
-- QUERY 3: Customer First Purchase Date
-- Business Question: When did each customer first engage with the
-- business, and what did they buy?
-- Use Case: Customer acquisition analysis, onboarding tracking,
-- cohort analysis preparation
-- Window Function: RANK()
-- ============================================================

WITH purchases AS (
    SELECT
        customer_id,
        invoice_date,
        description,
        quantity,
        -- RANK() assigns the same rank to all rows on the same day,
        -- ensuring all line items from the first purchase day are included
        RANK() OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('day', invoice_date)) AS purchase_rank
    FROM retail
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    invoice_date,
    description,
    quantity
FROM purchases
WHERE purchase_rank = 1
ORDER BY invoice_date;


-- ============================================================
-- QUERY 4: Top 3 Best-Selling Products by Country
-- Business Question: Which products drive the most volume in each
-- country, and does product popularity vary by region?
-- Use Case: Regional inventory planning, localised marketing strategy
-- Window Function: DENSE_RANK()
-- ============================================================

WITH top_products AS (
    SELECT
        country,
        description,
        SUM(quantity) AS total_qty,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY SUM(quantity) DESC) AS product_rank
    FROM retail
    WHERE description IS NOT NULL
    GROUP BY country, description
)
SELECT *
FROM top_products
WHERE product_rank IN (1, 2, 3)
ORDER BY country, product_rank;


-- ============================================================
-- QUERY 5: Customer Monthly Spending Rank
-- Business Question: Which months represent each customer's
-- highest-spend periods, and how do their spending months compare?
-- Use Case: Seasonal campaign targeting, spend pattern analysis
-- Window Function: DENSE_RANK()
-- ============================================================

SELECT
    customer_id,
    DATE_TRUNC('month', invoice_date)          AS invoice_month,
    SUM(unit_price * quantity)                 AS total_spend,
    -- Rank 1 = highest spending month for that customer
    DENSE_RANK() OVER (
        PARTITION BY customer_id
        ORDER BY SUM(unit_price * quantity) DESC
    )                                          AS monthly_rank
FROM retail
WHERE customer_id IS NOT NULL
GROUP BY
    customer_id,
    DATE_TRUNC('month', invoice_date)
ORDER BY
    customer_id,
    monthly_rank;


-- ============================================================
-- QUERY 6: Month-Over-Month Spending Change per Customer
-- Business Question: How is each customer's spending changing
-- month to month — are they growing, declining, or flat?
-- Use Case: Customer health monitoring, churn risk detection
-- Window Function: LAG()
-- ============================================================

WITH monthly_spending AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', invoice_date) AS invoice_month,
        SUM(quantity * unit_price)        AS cm_total_spend
    FROM retail
    WHERE customer_id IS NOT NULL
    GROUP BY
        customer_id,
        DATE_TRUNC('month', invoice_date)
)
SELECT
    *,
    -- The first month per customer has no prior month: COALESCE replaces NULL with 0
    COALESCE(
        LAG(cm_total_spend) OVER (PARTITION BY customer_id ORDER BY invoice_month),
        0
    ) AS pm_total_spend,
    COALESCE(
        cm_total_spend - LAG(cm_total_spend) OVER (PARTITION BY customer_id ORDER BY invoice_month),
        0
    ) AS cm_pm_difference
FROM monthly_spending
ORDER BY customer_id, invoice_month;


-- ============================================================
-- QUERY 7: Forward-Looking Spend Trend Flag per Customer
-- Business Question: Based on their next recorded month of activity,
-- is each customer's spending trending up, down, or flat?
-- Use Case: Proactive retention campaigns, next-period revenue forecasting
-- Window Function: LEAD()
-- ============================================================

WITH monthly_spending AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', invoice_date) AS invoice_month,
        SUM(quantity * unit_price) AS cm_total_spend
    FROM retail
    WHERE customer_id IS NOT NULL
    GROUP BY
        customer_id,
        DATE_TRUNC('month', invoice_date)
),
cm_nm_spending AS (
    SELECT
        *,
        -- COALESCE replaces NULL with 0 when LEAD finds no next month for that customer
        COALESCE(
            LEAD(cm_total_spend) OVER (PARTITION BY customer_id ORDER BY invoice_month),
            0
        ) AS nm_total_spend
    FROM monthly_spending
)
SELECT
    *,
    CASE
        WHEN (nm_total_spend - cm_total_spend) < 0 THEN 'Decrease'
        WHEN (nm_total_spend - cm_total_spend) = 0 THEN 'Same'
        WHEN (nm_total_spend - cm_total_spend) > 0 THEN 'Increase'
    END AS trend_flag
FROM cm_nm_spending
ORDER BY customer_id, invoice_month;


-- ============================================================
-- QUERY 8: Product Active Selling Period
-- Business Question: How long has each product been on the market,
-- and which products have had the longest or shortest selling windows?
-- Use Case: Product lifecycle management, catalogue optimisation
-- Functions: MIN(), MAX(), EPOCH date arithmetic
-- ============================================================

SELECT
    stock_code,
    -- Some products have minor description variations (likely data entry inconsistencies)
    -- MAX() arbitrarily selects one description per stock_code to avoid duplicate rows
    MAX(description) AS description,
    MIN(invoice_date) AS first_sold,
    MAX(invoice_date) AS last_sold,
    -- EPOCH converts the interval to seconds; dividing by 86400 gives days
    ROUND(EXTRACT(EPOCH FROM MAX(invoice_date) - MIN(invoice_date)) / 86400, 2) AS active_days
FROM retail
WHERE description IS NOT NULL
GROUP BY stock_code
ORDER BY active_days DESC;


-- ============================================================
-- QUERY 9: Cumulative Business Revenue Over Time
-- Business Question: How has total revenue accumulated month by
-- month, and what does the overall growth trajectory look like?
-- Use Case: Executive reporting, revenue milestone tracking
-- Window Function: SUM() running total
-- ============================================================

SELECT
    DATE_TRUNC('month', invoice_date) AS invoice_month,
    ROUND(SUM(quantity * unit_price), 0) AS monthly_revenue,
    -- ORDER BY makes this a running frame from the start of the partition up to the current row
    -- SUM(SUM()) — inner SUM aggregates rows into monthly totals, outer SUM applies the window
    -- NULL customer_id rows are retained: they represent valid revenue from guest purchases
    SUM(SUM(quantity * unit_price)) OVER (
        ORDER BY DATE_TRUNC('month', invoice_date)
    ) AS cumulative_revenue
FROM retail
GROUP BY DATE_TRUNC('month', invoice_date)
ORDER BY invoice_month;


-- ============================================================
-- QUERY 10: 3-Month Moving Average of Customer Spending
-- Business Question: What is each customer's smoothed spending
-- trend over time, filtering out single-month anomalies?
-- Use Case: Spend trend analysis, anomaly detection baseline
-- Window Function: AVG() with sliding frame
-- ============================================================

SELECT
    customer_id,
    DATE_TRUNC('month', invoice_date) AS invoice_month,
    SUM(quantity * unit_price) AS monthly_spending,
    ROUND(
        AVG(SUM(quantity * unit_price)) OVER (
            PARTITION BY customer_id
            ORDER BY DATE_TRUNC('month', invoice_date)
            -- 2 preceding + current row = 3 months total
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_3month
FROM retail
WHERE customer_id IS NOT NULL
GROUP BY customer_id, DATE_TRUNC('month', invoice_date)
ORDER BY customer_id, invoice_month;


-- ============================================================
-- QUERY 11: 2-Month Moving Average of Product Quantity Sold
-- Business Question: Which products show stable demand versus
-- erratic sales patterns over time?
-- Use Case: Demand forecasting, replenishment planning
-- Window Function: AVG() with sliding frame
-- ============================================================

SELECT
    stock_code,
    -- Some products have minor description variations (likely data entry inconsistencies)
    -- MAX() arbitrarily selects one description per stock_code to avoid duplicate rows
    MAX(description) AS description,
    DATE_TRUNC('month', invoice_date) AS invoice_month,
    SUM(quantity) AS qty_sold,
    ROUND(
        AVG(SUM(quantity)) OVER (
            PARTITION BY stock_code
            ORDER BY DATE_TRUNC('month', invoice_date)
            -- 1 preceding + current row = 2 months total
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ), 0
    ) AS moving_avg_2month
FROM retail
WHERE description IS NOT NULL
GROUP BY
    stock_code,
    DATE_TRUNC('month', invoice_date)
ORDER BY stock_code, invoice_month;


-- ============================================================
-- QUERY 12: 8-Day Forward-Looking Spending Window per Customer
-- Business Question: For any given day, how much is each customer
-- expected to spend over the following 8 days based on past activity?
-- Use Case: Short-term revenue forecasting, targeted promotions
-- Window Function: SUM() with forward frame
-- ============================================================

SELECT
    customer_id,
    DATE_TRUNC('day', invoice_date) AS invoice_day,
    SUM(quantity * unit_price) AS daily_spend,
    -- 8-row forward-looking spending window per customer
    SUM(SUM(quantity * unit_price)) OVER (
        PARTITION BY customer_id
        ORDER BY DATE_TRUNC('day', invoice_date)
        ROWS BETWEEN CURRENT ROW AND 7 FOLLOWING
    ) AS total_spend_8days_fwd
FROM retail
WHERE customer_id IS NOT NULL
GROUP BY
    customer_id,
    DATE_TRUNC('day', invoice_date)
ORDER BY
    customer_id,
    invoice_day;


-- ============================================================
-- QUERY 13: Median Monthly Spend per Customer
-- Business Question: What is each customer's typical monthly
-- spending level, excluding the distortion of outlier months?
-- Use Case: Customer segmentation, spend benchmark definition
-- Function: PERCENTILE_CONT()
-- ============================================================

WITH monthly_revenue AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', invoice_date) AS invoice_month,
        SUM(quantity * unit_price) AS monthly_spending
    FROM retail
    WHERE customer_id IS NOT NULL
    GROUP BY
        customer_id,
        DATE_TRUNC('month', invoice_date)
),
customers_median AS (
    SELECT
        customer_id,
        -- PERCENTILE_CONT finds the 50th percentile — the true statistical median
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_spending) AS median_monthly_spend
    FROM monthly_revenue
    GROUP BY customer_id
)
SELECT
    mr.customer_id,
    mr.invoice_month,
    mr.monthly_spending,
    -- CAST to NUMERIC required: PERCENTILE_CONT returns float8 which ROUND() does not accept directly
    ROUND(CAST(cm.median_monthly_spend AS NUMERIC), 2) AS median_monthly_spend
FROM monthly_revenue AS mr
JOIN customers_median AS cm ON mr.customer_id = cm.customer_id
ORDER BY
    mr.customer_id,
    mr.invoice_month;


-- ============================================================
-- QUERY 14: Country Revenue — First vs Last Month Comparison
-- Business Question: How does each country's current monthly revenue
-- compare to where it started and where it ended up?
-- Use Case: Regional growth assessment, period-over-period benchmarking
-- Window Functions: FIRST_VALUE(), LAST_VALUE()
-- ============================================================

SELECT
    country,
    DATE_TRUNC('month', invoice_date) AS invoice_month,
    SUM(quantity * unit_price) AS monthly_revenue,
    -- Default frame (UNBOUNDED PRECEDING to CURRENT ROW) is sufficient for FIRST_VALUE
    -- as the first value never changes regardless of how the frame grows
    FIRST_VALUE(SUM(quantity * unit_price)) OVER (
        PARTITION BY country
        ORDER BY DATE_TRUNC('month', invoice_date)
    ) AS first_month_revenue,
    -- LAST_VALUE requires explicit full-partition frame — without it, the default frame
    -- stops at the current row, returning the current row's value instead of the true last
    LAST_VALUE(SUM(quantity * unit_price)) OVER (
        PARTITION BY country
        ORDER BY DATE_TRUNC('month', invoice_date)
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_month_revenue
FROM retail
GROUP BY
    country,
    DATE_TRUNC('month', invoice_date)
ORDER BY
    country,
    invoice_month;


-- ============================================================
-- QUERY 15: Country Monthly Revenue — Full Analytical Summary
-- Business Question: For each country, what is the monthly revenue,
-- how does it trend (centered average), how has it accumulated
-- over time, and how much does each month contribute to the
-- country's annual total?
-- Use Case: Executive dashboard, country-level P&L reporting
-- Window Functions: AVG() centered frame, SUM() running total,
--                   SUM() full-partition for percentage calculation
-- ============================================================

WITH countries_revenue AS (
    SELECT
        country,
        DATE_TRUNC('month', invoice_date) AS invoice_month,
        SUM(quantity * unit_price) AS monthly_revenue,
        -- Centered 3-month average: 1 preceding + current + 1 following
        ROUND(
            AVG(SUM(quantity * unit_price)) OVER (
                PARTITION BY country
                ORDER BY DATE_TRUNC('month', invoice_date)
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ), 2
        ) AS centered_moving_avg,
        -- Running total: default ORDER BY frame (UNBOUNDED PRECEDING to CURRENT ROW)
        SUM(SUM(quantity * unit_price)) OVER (
            PARTITION BY country
            ORDER BY DATE_TRUNC('month', invoice_date)
        ) AS cumulative_revenue,
        -- Full partition total used as denominator for percentage calculation
        SUM(SUM(quantity * unit_price)) OVER (
            PARTITION BY country
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS total_country_revenue
    FROM retail
    GROUP BY
        country,
        DATE_TRUNC('month', invoice_date)
)
SELECT
    country,
    invoice_month,
    monthly_revenue,
    centered_moving_avg,
    cumulative_revenue,
    -- Numeric version for downstream calculations or visualisation tools
    ROUND((monthly_revenue / total_country_revenue) * 100, 2) AS pct_of_total_revenue,
    -- Text version for direct display in reports
    ROUND((monthly_revenue / total_country_revenue) * 100, 2) || '%' AS pct_of_total_revenue_txt
FROM countries_revenue
ORDER BY
    country,
    invoice_month;