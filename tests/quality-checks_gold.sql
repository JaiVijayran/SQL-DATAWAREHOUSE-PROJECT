/* 
Specification: This SQL script performs data validation and integration tasks for a data warehouse in the gold schema:
1. Integrates gender data from silver.crm_cust_info (priority) and silver.erp_cust_AZ12, creating a unified gender column with a fallback to 'N/A'.
2. Checks for duplicate customer keys in gold.dim_customers; expects no results for a valid dataset.
3. Checks for duplicate product keys in gold.dim_products; expects no results for a valid dataset.
4. Validates data model connectivity by checking for orphaned records in gold.facts_sales that do not link to gold.dim_customers or gold.dim_products.

Warnings:
- Ensure source tables (silver.crm_cust_info, silver.erp_cust_AZ12, silver.erp_LOC_A101, gold.dim_customers, gold.dim_products, gold.facts_sales) exist and are populated.
- The gender integration query uses LEFT JOINs, which may include NULL values for non-matching records; verify this aligns with requirements.
- Duplicate checks expect no results; any returned rows indicate data quality issues that need resolution.
- The connectivity check identifies orphaned sales records; NULLs in product_key or customer_key suggest data inconsistencies.
- The DISTINCT clause in the gender query may impact performance on large datasets; consider indexing cst_key and CID.
*/

-- Integrating gender columns from CRM and ERP tables
SELECT DISTINCT
    ci.cst_gender, -- Gender from CRM table
    ca.GEN, -- Gender from ERP table
    CASE 
        WHEN ci.cst_gender IS NOT NULL THEN ci.cst_gender -- Prioritizing CRM gender as master
        ELSE COALESCE(ca.GEN, 'N/A') -- Fallback to ERP gender or 'N/A' if both are NULL
    END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_AZ12 AS ca
    ON ci.cst_key = ca.CID -- Joining on customer key
LEFT JOIN silver.erp_LOC_A101 AS la
    ON ci.cst_key = la.CID -- Including location data (though not used in output)
ORDER BY 1, 2 -- Sorting by CRM and ERP gender for clarity

-- Checking for duplicate customer keys in dim_customers (expects no results)
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1 -- Identifying any customer keys appearing multiple times

-- Checking for duplicate product keys in dim_products (expects no results)
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1 -- Identifying any product keys appearing multiple times

-- Checking data model connectivity between facts_sales and dimensions
SELECT *
FROM gold.facts_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON c.customer_key = f.customer_key -- Linking sales to customers
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key -- Linking sales to products
WHERE p.product_key IS NULL OR c.customer_key IS NULL -- Identifying orphaned sales records
