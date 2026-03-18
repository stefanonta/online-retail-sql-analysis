# Online Retail Sales Analysis - SQL Window Functions

## Overview
Exploratory analysis of 541,909 transactional records from a UK-based online retailer (2010–2011).
This project demonstrates advanced SQL querying techniques applied to real-world e-commerce data,
focusing on customer behaviour, product performance, and revenue trends.

## Dataset
- **Source:** [UCI Online Retail Dataset via Kaggle](https://www.kaggle.com/datasets/vijayuv/onlineretail)
- **Rows:** 541,909
- **Period:** December 2010 to December 2011
- **Key fields:** InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

## Data Quality Notes
- 135,080 rows (about 25%) have no CustomerID, representing guest or untracked purchases
- Queries involving customer-level analysis exclude NULL CustomerIDs with documented rationale
- Some products share the same StockCode with slightly different descriptions (likely data entry inconsistencies), I handled this incosistency via MAX(description) aggregation

## Key Findings

- **Regional customer value is highly concentrated**: in most countries, a small number of customers account for a disproportionate share of revenue *(Query 1)*
- **Top 5 global customers show varied purchasing patterns**: some place frequent small orders, others make infrequent high-value purchases *(Query 2)*
- **Most customers made their first purchase in 2011**: indicating rapid growth of the retailer's customer base in that year *(Query 3)*
- **Product popularity varies significantly by country**: the top 3 best-selling products differ across regions, suggesting country-specific demand patterns *(Query 4)*
- **Customer spending is rarely consistent month to month**: ranking monthly spend per customer reveals strong fluctuations with few customers showing stable patterns *(Query 5)*
- **Month-over-month spending changes are highly variable**: most customers show irregular spend deltas with no clear upward or downward trend *(Query 6)*
- **Forward-looking spend trends show limited predictability**: a large proportion of customers are flagged as "Decrease" in the following month, consistent with one-time or seasonal buyers *(Query 7)*
- **Product lifecycles vary widely**: some products were sold across the full 12-month period while others appear only briefly, suggesting seasonal or discontinued items *(Query 8)*
- **Cumulative revenue grew steadily through 2011**: with a notable acceleration in Q4, consistent with seasonal retail demand peaks *(Query 9)*
- **3-month moving averages reveal smoothed spending patterns**: helping distinguish genuine customer growth from one-off purchase spikes *(Query 10)*
- **Product demand volatility differs by item**: the 2-month moving average of quantity sold highlights products with stable demand vs. erratic sales *(Query 11)*
- **8-day forward spending windows identify high-activity periods**: useful for targeted marketing or inventory planning at the customer level *(Query 12)*
- **Median monthly spend per customer is consistently lower than peak months**: confirming that high-spend months are outliers rather than the norm *(Query 13)*
- **First and last month revenue differ significantly for most countries**: suggesting growth or decline in regional markets over the observed period *(Query 14)*
- **Monthly revenue contribution and centered moving averages expose seasonality**: Q4 months consistently contribute a higher percentage of annual revenue across most countries *(Query 15)*

## Queries

| # | Business Question | Concepts Used |
|---|---|---|
| 1 | Which customers spend the most in each country? | DENSE_RANK, PARTITION BY, CTE |
| 2 | What is the full purchase history of the top 5 customers? | DENSE_RANK, CTE, JOIN |
| 3 | When did each customer make their very first purchase? | RANK, DATE_TRUNC, PARTITION BY |
| 4 | What are the top 3 best-selling products per country? | DENSE_RANK, PARTITION BY |
| 5 | How does each customer's monthly spending rank over time? | DENSE_RANK, DATE_TRUNC |
| 6 | How does each customer's spending change month over month? | LAG, COALESCE, CTE |
| 7 | Is each customer's spending expected to increase or decrease next month? | LEAD, CASE, CTE |
| 8 | What is each product's active selling period? | MIN, MAX, DATE arithmetic, EPOCH |
| 9 | How has cumulative business revenue grown over time? | SUM OVER, running total frame |
| 10 | What is each customer's 3-month moving average spending? | AVG OVER, ROWS BETWEEN, nested aggregation |
| 11 | What is each product's 2-month moving average quantity sold? | AVG OVER, ROWS BETWEEN, nested aggregation |
| 12 | What is each customer's total spending over an 8-day forward window? | SUM OVER, ROWS BETWEEN CURRENT ROW AND FOLLOWING |
| 13 | What is each customer's median monthly spending? | PERCENTILE_CONT, CTE, JOIN |
| 14 | How does each country's monthly revenue compare to its first and last months? | FIRST_VALUE, LAST_VALUE, frame clauses |
| 15 | What is each country's monthly revenue contribution, trend, and cumulative total? | AVG/SUM OVER, centered frame, running total, percentage calculation |

## Technical Highlights
- Consistent NULL handling across all 15 queries with documented rationale
- Deliberate frame clause selection — default, forward-looking, centered, and full-partition frames each used where appropriate
- CTEs used for pre-aggregation before window function application
- Nested aggregation pattern (SUM(SUM(...))) applied where window functions operate on already-aggregated data
- Date truncation used throughout to handle timestamp granularity consistently

## Tools
- PostgreSQL 18
- pgAdmin

## Author
Stefano Noventa
[LinkedIn](https://www.linkedin.com/in/stefanonta/) | [GitHub](https://github.com/stefanonta)
