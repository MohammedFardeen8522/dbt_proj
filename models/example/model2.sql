
with store_sales_cte as (
    select ss_sold_date_sk as sold_date, ss_item_sk, ss_quantity, ss_sales_price, ss_customer_sk
    from {{source('snowflake_sample_data', 'store_sales')}}
),
date_dim_cte as (
    select d_date_sk as date_sk, d_year, d_date, d_moy from {{source('snowflake_sample_data', 'date_dim')}}
),
item_cte as (
    select i_item_sk from {{source('snowflake_sample_data', 'item')}}
),
customer_cte as (
    select c_customer_sk, c_first_name, c_last_name from {{source('snowflake_sample_data', 'customer')}}
),
catalog_sales_cte as (
    select cs_quantity, cs_list_price, cs_sold_date_sk, cs_item_sk, cs_bill_customer_sk from {{source('snowflake_sample_data', 'catalog_sales')}}
),
web_sales_cte as (
    select ws_quantity, ws_list_price, ws_sold_date_sk, ws_item_sk, ws_bill_customer_sk from {{source('snowflake_sample_data', 'web_sales')}}
),


store_sales_join_customer as (
    select cc.c_customer_sk, ssc.ss_quantity, ssc.ss_sales_price, ssc.ss_customer_sk, ssc.sold_date
    from store_sales_cte AS ssc
    join customer_cte AS cc
    on ssc.ss_customer_sk = cc.c_customer_sk
),


store_sales_customer_datedim AS (
	SELECT ssjc.c_customer_sk, ssjc.ss_quantity, ssjc.sold_date, ssjc.ss_sales_price, ssjc.ss_customer_sk, ddc.date_sk, ddc.d_year
	FROM store_sales_join_customer AS ssjc
	JOIN date_dim_cte AS ddc
	ON ssjc.sold_date = ddc.date_sk
),

store_sales_join_date_dim as (
    select ssc.sold_date, ssc.ss_item_sk, ddc.date_sk, ddc.d_date, ssc.ss_quantity, ssc.ss_sales_price, ssc.ss_customer_sk
    from store_sales_cte AS ssc
    join date_dim_cte AS ddc
    on ssc.sold_date = ddc.date_sk
    where ddc.d_year in (2000, 2001, 2002, 2003)
),


storesales_datedim_item AS (
	select ssjdd.sold_date, ssjdd.ss_item_sk, ssjdd.date_sk, ssjdd.d_date, ssjdd.ss_quantity, ssjdd.ss_sales_price, ssjdd.ss_customer_sk, 
	itc.i_item_sk item_sk
	FROM store_sales_join_date_dim AS ssjdd
	JOIN 
	item_cte AS itc
	ON ssjdd.ss_item_sk = itc.i_item_sk
),

frequent_ss_items as (
    select ssddi.item_sk, ssddi.d_date, count(*) as cnt
    from storesales_datedim_item AS ssddi
    group by ssddi.item_sk, ssddi.d_date
    having count(*) > 4
),


max_store_sales_sub_query AS (

	select sscdd.c_customer_sk,sum(sscdd.ss_quantity*sscdd.ss_sales_price) csales
	        from store_sales_customer_datedim AS sscdd
	         where sscdd.d_year in (2000,2000+1,2000+2,2000+3)
	        group by sscdd.c_customer_sk
),

 max_store_sales as
 (select max(msssq.csales) tpcds_cmax
  from max_store_sales_sub_query AS msssq
 ),


best_ss_customer as (
    select ssjc.c_customer_sk, sum(ssjc.ss_quantity * ssjc.ss_sales_price) as ssales
    from store_sales_join_customer AS ssjc
    group by ssjc.c_customer_sk
    having sum(ssjc.ss_quantity * ssjc.ss_sales_price) > (50/100.0) * (select
  count(*)
 from max_store_sales)
),

catalog_sales_customer as (
    select cuc.c_last_name, cuc.c_first_name, csc.cs_quantity, csc.cs_list_price, csc.cs_sold_date_sk
    from catalog_sales_cte AS csc
    join customer_cte AS cuc
    on csc.cs_bill_customer_sk = cuc.c_customer_sk
    join frequent_ss_items AS fssi
    on csc.cs_item_sk = fssi.item_sk
    join best_ss_customer AS bssc
    on csc.cs_bill_customer_sk = bssc.c_customer_sk
    group by cuc.c_last_name, cuc.c_first_name, csc.cs_quantity, csc.cs_list_price, csc.cs_sold_date_sk
),
catalog_sales_customer_date_dim as (
    select casc.c_last_name, casc.c_first_name, casc.cs_quantity, casc.cs_list_price, casc.cs_sold_date_sk
    from catalog_sales_customer AS casc
    join date_dim_cte AS ddc
    on casc.cs_sold_date_sk = ddc.date_sk
    where ddc.d_year = 2000 and ddc.d_moy = 2
),
customer_web_sales as (
    select cc.c_last_name, cc.c_first_name, wsc.ws_quantity, wsc.ws_list_price, wsc.ws_sold_date_sk
    from web_sales_cte AS wsc
    join customer_cte AS cc
    on wsc.ws_bill_customer_sk = cc.c_customer_sk
    join frequent_ss_items AS fssi
    on wsc.ws_item_sk = fssi.item_sk
    join best_ss_customer AS bssc
    on wsc.ws_bill_customer_sk = bssc.c_customer_sk
    group by cc.c_last_name, cc.c_first_name, wsc.ws_quantity, wsc.ws_list_price, wsc.ws_sold_date_sk
),
customer_web_sales_date_dim as (
    select cws.c_last_name, cws.c_first_name, cws.ws_quantity, cws.ws_list_price, cws.ws_sold_date_sk, ddc.date_sk
    from customer_web_sales AS cws
    join date_dim_cte AS ddc
    on cws.ws_sold_date_sk = ddc.date_sk
    where ddc.d_year = 2000 and ddc.d_moy = 2
),
final as (
    select cscdd.c_last_name, cscdd.c_first_name, sum(cscdd.cs_quantity * cscdd.cs_list_price) as sales
    from catalog_sales_customer_date_dim AS cscdd
    group by cscdd.c_last_name, cscdd.c_first_name
    union all
    select cwsdd.c_last_name, cwsdd.c_first_name, sum(cwsdd.ws_quantity * cwsdd.ws_list_price) as sales
    from customer_web_sales_date_dim AS cwsdd
    group by cwsdd.c_last_name, cwsdd.c_first_name
)
select c_first_name, c_last_name, sales from final
order by c_last_name, c_first_name, sales
