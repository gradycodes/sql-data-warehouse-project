/*
====================================================================================
Quality Checks
====================================================================================
Purpose of Script:
  This script performs quality checks to confirm uniqueness, consistency, and accuracy
  of the data within the Gold Layer.
    - Uniqueness of surrogate keys in dimensions tables.
    - Normalizing data names within columns.
    - Finding duplicate data.

Usage:
  Run these checks after loading the Silver Layer.
====================================================================================
*/

-- Data Correction & Normalizing
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid
ORDER BY 1, 2

-- Check the quality (uniqueness) of the data - finding duplicates
SELECT prd_key, COUNT(*) FROM (
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- Filter out old historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1

-- Foreign Key Integrity (Dimensions) - make sure all of the keys are matching and have no nulls
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
