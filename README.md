# Online Retail Sales Analysis — SQL Window Functions

## Overview
Exploratory analysis of 541,909 transactional records from a UK-based online retailer
(2010–2011). This project demonstrates advanced SQL querying techniques applied to
real-world e-commerce data, focusing on customer behaviour, product performance,
and sales trends.

## Dataset
- **Source:** [UCI Online Retail Dataset via Kaggle](https://www.kaggle.com/datasets/vijayuv/onlineretail)
- **Rows:** 541,909
- **Period:** December 2010 – December 2011
- **Key fields:** InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

## Key Findings
- Top 5 customers globally account for a disproportionate share of total revenue
- The United Kingdom dominates transaction volume across all product categories
- Customer purchasing frequency varies significantly — some customers show clear seasonal peaks
- 25% of records have no CustomerID, indicating a significant volume of guest transactions

## Queries
| # | Business Question | Concepts Used |
|---|---|---|
| 1 | Which customers spend the most in each country? | DENSE_RANK, PARTITION BY |
| 2 | What is the purchase history of the top 5 customers? | DENSE_RANK, CTE, JOIN |
| 3 | When did each customer make their first purchase? | RANK, DATE_TRUNC |
| 4 | What are the top 3 best-selling products per country? | DENSE_RANK, PARTITION BY |
| 5 | How does each customer's monthly spending rank over time? | DENSE_RANK, DATE_TRUNC |

## Technical Highlights
- Consistent NULL handling across all queries (135,080 NULL CustomerIDs excluded with documented rationale)
- Use of CTEs for pre-aggregation before applying window functions
- Date truncation to handle time-component inconsistencies in timestamp data

## Tools
- PostgreSQL 18
- pgAdmin

## Author
Stefano [Your Last Name]
[Linkedin](https://www.linkedin.com/in/stefanonta/) | [GitHub URL]