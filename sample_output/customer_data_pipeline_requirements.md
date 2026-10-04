# Customer Data Pipeline — Business Requirements

## Overview

We need a pipeline that ingests raw customer records from a CSV export and produces a clean, standardized customer dimension table in our lakehouse.

## Source System

- **System**: CRM Export (daily extract)
- **Format**: CSV files
- **Location**: `/mnt/raw/crm_customers/`
- **Delimiter**: comma
- **Header**: first row contains column headers
- **Encoding**: UTF-8

## Source Fields

| Column Name       | Description                          | Example Values           |
|-------------------|--------------------------------------|--------------------------|
| cust_id           | Raw customer identifier              | C-10001, C-10002         |
| fname             | First name                           | Alice, Bob               |
| lname             | Last name                            | Smith, Johnson           |
| signup_dt         | Sign-up date (mixed formats)         | 20230115, 2023-01-15     |
| bal_cents         | Account balance in cents             | 125050, 0, -500          |
| active_flag       | Whether account is active            | Y, N, y, n               |
| acct_close_dt     | Account closure date (may be "N/A")  | 2024-06-15, N/A, (empty) |

## Target Requirements

The final customer dimension table should have these columns:

1. **customer_id** — The raw `cust_id`, trimmed and cast to string. This is the primary key.
2. **full_name** — Concatenation of `fname` and `lname` separated by a space.
3. **signup_date** — The `signup_dt` parsed into a standard `yyyy-MM-dd` date. The source may send dates in formats like `yyyyMMdd`, `yyyy-MM-dd`, or `MM/dd/yyyy`.
4. **account_balance** — The `bal_cents` value divided by 100, stored as a decimal. If the value is missing or non-numeric, default to `0.00`.
5. **is_active** — Map the `active_flag` to a boolean: `Y` → `true`, `N` → `false`. Case-insensitive.
6. **account_closed_date** — Parse the `acct_close_dt` as a date. If the value is `"N/A"`, treat it as null.

## Data Quality Rules

- `customer_id` must not be null. Rows without a customer ID should be **quarantined** for review.
- `account_balance` must be >= 0 after conversion. Rows with negative balances should be **dropped** (they are refund artifacts).

## Target Table

- **Catalog**: `main`
- **Schema**: `silver`
- **Table**: `dim_customers`
- **Format**: Delta
- **Write mode**: Merge (upsert on `customer_id`)

## Deployment

- Three environments: `dev`, `staging`, `prod`
- Job should run on Databricks with Spark 15.4
