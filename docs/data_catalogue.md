
Data Catalogue for Gold Schema Tables

This catalogue describes the structure and purpose of the tables in the gold schema, including two dimension tables (gold.dim_customers, gold.dim_products) and one fact table (gold.facts_sales). Each table below includes the column name, data type, and description, formatted for clarity. The purpose of each table is also provided.
gold.dim_customers
Purpose: The gold.dim_customers table serves as a dimension table that provides detailed information about customers. It consolidates data from CRM and ERP systems to support customer-related analytics, such as customer segmentation, demographic analysis, and purchase behavior tracking.



Column Name
Data Type
Description



customer_key
INT
Surrogate key generated using ROW_NUMBER() ordered by cst_id, uniquely identifying each customer.


customer_id
VARCHAR
Unique identifier for the customer from the CRM system (sourced from cst_id).


customer_number
VARCHAR
Customer key from the CRM system (sourced from cst_key), used for joining with other tables.


first_name
VARCHAR
Customer's first name from the CRM system (sourced from cst_firstname).


last_name
VARCHAR
Customer's last name from the CRM system (sourced from cst_lastname).


country
VARCHAR
Customer's country from the ERP location table (sourced from la.COUNTRY).


marital_status
VARCHAR
Customer's marital status from the CRM system (sourced from cst_marital_status).


gender
VARCHAR
Customer's gender, prioritizing CRM (cst_gender), falling back to ERP (GEN), or 'N/A' if both are NULL.


birth_date
DATE
Customer's birth date from the ERP system (sourced from ca.BDATE).


creation_date
DATE
Date the customer record was created in the CRM system (sourced from cst_create_date).


gold.dim_products
Purpose: The gold.dim_products table is a dimension table that stores detailed information about products. It supports product-related analytics, such as sales performance by category, cost analysis, and product lifecycle tracking, by consolidating CRM and ERP product data.



Column Name
Data Type
Description



product_key
INT
Surrogate key generated using ROW_NUMBER() ordered by prd_start and prd_key, uniquely identifying each product.


product_id
VARCHAR
Unique identifier for the product from the CRM system (sourced from prd_id).


product_number
VARCHAR
Product key from the CRM system (sourced from prd_key), used for joining with other tables.


product_name
VARCHAR
Name of the product from the CRM system (sourced from prd_nm).


category_id
VARCHAR
Identifier for the product category from the CRM system (sourced from cat_id).


category
VARCHAR
Category name from the ERP system (sourced from px.CAT).


sub_category
VARCHAR
Sub-category name from the ERP system (sourced from px.SUB_CATEGORY).


maintenance
VARCHAR
Maintenance information for the product from the ERP system (sourced from px.MAINTENANCE).


product_cost
DECIMAL
Cost of the product from the CRM system (sourced from prd_cost).


product_line
VARCHAR
Product line from the CRM system (sourced from prd_line).


start_date
DATE
Date the product became active from the CRM system (sourced from prd_start).


gold.facts_sales
Purpose: The gold.facts_sales table is a fact table that captures sales transactions. It links to gold.dim_customers and gold.dim_products to enable detailed sales analysis, such as revenue trends, customer purchasing patterns, and product performance over time.



Column Name
Data Type
Description



order_number
VARCHAR
Unique identifier for the sales order from the CRM system (sourced from sls_ord_num).


product_key
INT
Foreign key referencing the product_key in gold.dim_products, linking to the product sold.


customer_key
INT
Foreign key referencing the customer_key in gold.dim_customers, linking to the customer who made the purchase.


order_date
DATE
Date the sales order was placed (sourced from sls_order_dt).


ship_date
DATE
Date the sales order was shipped (sourced from sls_ship_dt).


due_date
DATE
Due date for the sales order (sourced from sls_due_dt).


sales
INT
Total sales amount for the order (sourced from sls_sales).


quantity
INT
Quantity of products sold in the order (sourced from sls_quantity).


price
INT
Price per unit for the product in the order (sourced from sls_price).



