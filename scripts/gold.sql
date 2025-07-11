IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO
create view gold.dim_customer as 
	select 
		row_number() over( order by cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as firstname,
		ci.cst_lastname as lastname,
		la.cntry as country,
		ci.cst_marital_status as status,
		case when ci.cst_gndr !='n/a' then ci.cst_gndr
		else coalesce(ca.gen,'n/a')
		end as gender,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date
	from silver.crm_cust_info  ci
	left join silver.erp_cust_az12  ca
	on	ci.cst_key=ca.cid
	left join silver.erp_loc_a101  la
	on  ci.cst_key=la.cid


IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as 
	select
		ROW_NUMBER() over (order by pr.prd_start_dt,pr.prd_key) as product_key,
		pr.prd_id as product_id,
		pr.prd_key as product_number,
		pr.prd_nm as product_name,
		pr.cat_id as category_id,
		pn.cat as category,
		pn.subcat as subcategory,
		pn.maintenance,
		pr.prd_cost as cost,
		pr.prd_line as product_line,
		pr.prd_start_dt as start_date
	from silver.crm_prd_info pr
	left join silver.erp_px_cat_g1v2 pn
	on	pr.cat_id=pn.id
	where pr.prd_end_dt is null

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as
	SELECT
		  si.sls_ord_num as order_number,
		  pt.product_key  AS product_key,
		  ct.customer_key AS customer_key,
		  si.sls_order_dt as order_date,
		  si.sls_ship_dt as shipping_date,
		  si.sls_due_dt as due_date,
		  si.sls_sales as sales_amount,
		  si.sls_quantity as quantity,
		  si.sls_price as price
	  FROM silver.crm_sales_details si
	  left join gold.dim_products pt
	  on si.sls_prd_key=pt.product_number
	  left join gold.dim_customer ct
	  on si.sls_cust_id=ct.customer_id



