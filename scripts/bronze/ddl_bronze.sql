/*
Script Purpose: 
This SQL script creates tables in the bronze schema to store customer, product, and sales data from both CRM and ERP systems. 
It includes tables for customer information, product details, sales details, and additional ERP-specific data such as customer demographics, location, and product catalog information.
The script drops existing tables before creating new ones to ensure a clean setup.

Warning:
- This script will DROP existing tables with the specified names in the bronze schema if they exist, potentially causing data loss.
- Ensure that any critical data in these tables is backed up before execution.
- Verify that the schema 'bronze' exists in the database prior to running this script.
- Ensure that the data types and field lengths are appropriate for the data being stored to avoid truncation or type mismatch errors.
*/

-- THE BELOW 3 TABLES ARE FROM THE CRM SYSTEM.

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (

	 cst_id int,
	 cst_key nvarchar(50),
	 cst_firstname nvarchar(50),
	 cst_lastname nvarchar(50),
	 cst_marital_status nvarchar(50),
	 cst_gender nvarchar(50),
	 cst_create_date date

);

IF OBJECT_ID ('bronze.crm__prd_info','U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;


CREATE TABLE bronze.crm_prd_info (

	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start nvarchar(50),
	prd_end_dt nvarchar(50)

);

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;


CREATE TABLE bronze.crm_sales_details (

	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int

);

-- THE BELOW 3 TABLES ARE FROM THE ERP SYSTEM.

IF OBJECT_ID ('bronze.erp_cust_AZ12 ','U') IS NOT NULL
DROP TABLE bronze.erp_cust_AZ12 ;


CREATE TABLE bronze.erp_cust_AZ12 (

	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)

);

IF OBJECT_ID ('bronze.erp_LOC_A101','U') IS NOT NULL
DROP TABLE bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101 (

	CID nvarchar(50),
	COUNTRY varchar(50)

);

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
DROP TABLE bronze.erp_PX_CAT_G1V2;


CREATE TABLE bronze.erp_PX_CAT_G1V2 (

	ID nvarchar(50),
	CAT varchar(50),
	SUB_CATEGORY varchar(50),
	MAINTENANCE varchar(50)

)

