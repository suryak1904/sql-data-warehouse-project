
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
	cat_id		 NVARChar(50),	
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO
--Procedure silver.load_silver
create or alter procedure silver.load_silver as
begin
	declare @start_time datetime,@end_time datetime,@batch_start_time datetime, @batch_end_time datetime
	begin try
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		set @start_time=getdate()
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>> Inserting Data Into: silver.crm_cust_info'; 
			insert into silver.crm_cust_info(
					cst_id, 
					cst_key, 
					cst_firstname, 
					cst_lastname, 
					cst_marital_status, 
					cst_gndr,
					cst_create_date
			)
			select 
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname, 
				trim(cst_lastname) as cst_lastname,
				CASE
					WHEN upper(trim(cst_marital_status))='M' then 'Married'
					WHEN upper(trim(cst_marital_status))='S' then 'single'
					ELSE 'n/a'
				END AS cst_marital_status,
				CASE
					WHEN upper(trim(cst_gndr))='M' then 'Male'
					WHEN upper(trim(cst_gndr))='F' then 'Female'
					ELSE 'n/a'
				END AS cst_gndr,
				cst_create_date
			from (
				select 
					*,
					ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
				from bronze.crm_cust_info
				where cst_id is not null
			)t
			where flag_last=1;
			set @end_time=getdate()
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		PRINT '------------------------------------------------';
		PRINT 'Loading crm_prd_table';
		PRINT '------------------------------------------------';
		set @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_line,
				prd_cost,
				prd_start_dt,
				prd_end_dt
			)
			select 
				prd_id,
				replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
				SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
				prd_nm,
				case upper(trim(prd_line))
					when 'M' then 'Mountain'
					When 'R' then 'Road'
					When 'S' then 'Other Sales'
					when 'T' then 'Touring'
					else 'n/a'
				end as prd_line,
				isnull(prd_cost,0) as prd_cost,
				cast(prd_start_dt as date) as prd_start_dt,
				cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt desc)-1 as date) as prd_end_dt
			from bronze.crm_prd_info
			set @end_time=GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		set @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
			select 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				case when sls_order_dt=0 or len(sls_order_dt)!=8 then null
					 else cast(cast(sls_order_dt as varchar) as date) 
				end as sls_order_dt,
				case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null
					 else cast(cast(sls_ship_dt as varchar) as date) 
				end as sls_ship_dt,
				case when sls_due_dt=0 or len(sls_due_dt)!=8 then null
					 else cast(cast(sls_due_dt as varchar) as date) 
				end as sls_due_dt,
				case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price)
					 then sls_quantity*abs(sls_price)
					 else sls_sales
				end as sls_sales,
				sls_quantity,
				case when sls_price<=0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
					 else sls_price
				end as sls_price
				from bronze.crm_sales_details;
				set @end_time=GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		set @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12 ';
		TRUNCATE TABLE silver.erp_cust_az12 ;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12 ';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
			)
			select
				case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
					 else cid
				end as cid,
				case when bdate>getdate() then NULL
				else bdate
				end as bdate,
				case 
					when Upper(trim(gen)) in ('F','FEMAlE') then 'Female'
					when Upper(trim(gen)) in ('M','MALE') then 'Male'
					else 'n/a'
				end as gen
				from bronze.erp_cust_az12;
				set @end_time=GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		set @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (
				cid,
				cntry
			)
			SELECT 
				replace(cid,'-','') as cid,
				case when trim(cntry)='DE' then 'Germany'
					 when trim(cntry) in ('US','USA') then 'United States'
					 when trim(cntry)='' or cntry is null then 'n/a'
					 else cntry
					 end as cntry
				from bronze.erp_loc_a101
				set @end_time=GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		set @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
				)
				SELECT 
					id,
					cat,
					subcat,
					maintenance
				from bronze.erp_px_cat_g1v2
				set @end_time=getdate()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
	end try
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print '========================================='
	END catch
	print '......................................................'
	set @batch_end_time=getdate()
	print '>>> Total Load Duration'+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds'
	print '-----------------------------------------------------'
end

