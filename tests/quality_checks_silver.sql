/*
================================================================================
Quality Checks
================================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema.It includes checks for:
    - Null or duplicate primary keys.
    -Unwanted spaces in string fields
    -Data standardization and consistency
    -Invalid date ranges and orders.
    -Data consistency between related fields.

Usage Notes:
      -Run these checks after data loading Silver Layer.#
      -Investigate and resolve any discrepancies found during the checks.
================================================================================
*/


-------Quality Check---------
-----Check For Nulls or Duplicates in Primary Key
-----Expectations : No Results
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
Having COUNT(*) > 1 OR prd_id IS NULL

------Check Unwanted spaces
------Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

----Check for NULLS or Negative Numbers
----Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

----Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

----Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

----Check for Invalid Dates
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

-----Check for Invalid Date Orders
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

----Check Data Consistency: Between Sales,Quantity and Price
---->> Sales = Quantity * Price
---->> Values must not be NULL, zero Or negative

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_qauntity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_qauntity * ABS(sls_price)
         THEN sls_qauntity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_qauntity * sls_price
OR sls_sales IS NULL OR sls_qauntity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_qauntity <= 0 OR sls_price <= 0

---Identify Out-of-Range Dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate<'1924-01-01' OR bdate > GETDATE()

---Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12
