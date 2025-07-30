/* 
Specification: This SQL script creates three views in the gold schema for a data warehouse:
1. dim_customer: A dimension view for customer data, combining CRM and ERP data with a generated customer_key.
2. dim_products: A dimension view for product data, including category details and filtering out historical records.
3. facts_sales: A fact view for sales data, linking to dim_customer and dim_products using their respective keys.

Warnings: 
- Ensure that the source tables in the silver schema (crm_cust_info, erp_cust_AZ12, erp_LOC_A101, crm_prd_info, erp_PX_CAT_G1V2, crm_sales_details) exist and are populated.
- The ROW_NUMBER() function in dim_customer and dim_products generates surrogate keys based on specific ORDER BY clauses; ensure the ordering logic aligns with business requirements.
- The LEFT JOINs may result in NULL values for non-matching records, which could affect downstream analytics.
- The gender CASE statement prioritizes ci.cst_gender over ca.GEN, with a fallback to 'N/A' for NULL values.
- The dim_products view filters out historical data where prd_end_dt is not null; verify this aligns with reporting needs.
- Ensure that the join keys (e.g., cst_key, cat_id, sls_prd_key, sls_cust_id) are consistent across tables to avoid data mismatches.
*/

-- Creating the customer dimension view
CREATE VIEW gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key generated for customer dimension
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.COUNTRY AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gender IS NOT NULL THEN ci.cst_gender -- Prioritizing CRM gender over ERP gender
        ELSE COALESCE(ca.GEN, 'N/A') 
    END AS gender, -- Fallback to 'N/A' for missing gender values
    ca.BDATE AS birth_date,
    ci.cst_create_date AS creation_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_AZ12 AS ca
    ON ci.cst_key = ca.CID -- Joining CRM and ERP customer data
LEFT JOIN silver.erp_LOC_A101 AS la
    ON ci.cst_key = la.CID -- Joining location data for country information

-- Creating the product dimension view
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pn.prd_start, pn.prd_key) AS product_key, -- Surrogate key ordered by start date and product key
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    px.CAT AS category,
    px.SUB_CATEGORY AS sub_category,
    px.MAINTENANCE AS maintenance,
    pn.prd_cost AS product_cost,
    pn.prd_line AS product_line,
    pn.prd_start AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS px
    ON pn.cat_id = px.id -- Joining product info with category details
WHERE prd_end_dt IS NULL -- Excluding historical products with end dates

-- Creating the sales fact view
CREATE VIEW gold.facts_sales AS
SELECT
    sl.sls_ord_num AS order_number,
    pr.product_key, -- Referencing product_key from dim_products
    cu.customer_key, -- Referencing customer_key from dim_customer
    sl.sls_order_dt AS order_date,
    sl.sls_ship_dt AS ship_date,
    sl.sls_due_dt AS due_date,
    sl.sls_sales AS sales,
    sl.sls_quantity AS quantity,
    sl.sls_price AS price
FROM silver.crm_sales_details AS sl
LEFT JOIN gold.dim_products AS pr
    ON sl.sls_prd_key = pr.product_number -- Linking sales to products
LEFT JOIN gold.dim_customer AS cu
    ON sl.sls_cust_id = cu.customer_id -- Linking sales to customers
