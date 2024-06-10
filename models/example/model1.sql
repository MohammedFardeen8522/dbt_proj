-- Configuration
-- {% if target.name == 'dev' %}
--     {{ config(
--         materialized='table',
--         schema='TPCDS_SF10TCL',
--         database='DBT_DATABASE1'
--     ) }}
-- {% elif target.name == 'qa' %}
--     {{ config(
--         materialized='table',
--         schema='TPCDS_SF10TCL',
--         database='DBT_DATABASE2'
--     ) }}
-- {% endif %}
--  COLOR.1 = peach

-- Setting parameters
{% set MARKET = 8 %}
{% set COLOR1 = 'peach' %}
{% set COLOR2 = 'saddle' %}

with store_sales_cte as (   
	select ss_sold_date_sk as sold_date, ss_item_sk, ss_quantity, ss_sales_price, 
	ss_customer_sk, ss_ticket_number, ss_store_sk, ss_net_paid
        from {{source('snowflake_sample_data', 'store_sales')}}
),

item_cte as (
	select i_item_sk, i_color, i_current_price, i_manager_id, i_units, i_size
	from {{source('snowflake_sample_data', 'item')}}
),


customer_cte as (
 	select c_customer_sk, c_first_name, c_last_name, c_current_addr_sk,
	c_birth_country 
	from {{source('snowflake_sample_data', 'customer')}}
),

store_returns_cte as(
	select sr_ticket_number, sr_item_sk 
	from {{source('snowflake_sample_data', 'store_returns')}}
),

store_cte as (
	select s_state, s_store_sk, s_zip, s_market_id, s_store_name, 	
	from {{source('snowflake_sample_data', 'store')}}
),

customer_address_cte as (
	select ca_state, ca_address_sk, ca_country, ca_zip 
	from {{source('snowflake_sample_data', 'customer_address')}}
),

--------------------------------------------------------------------------------------------

--JOINING THE CTES

-----COLOR.1 = peach

ss_sr AS (
    SELECT ss_net_paid, ss_store_sk, ss_item_sk, ss_customer_sk
    FROM store_sales_cte
    JOIN store_returns_cte
    ON store_sales_cte.ss_ticket_number = store_returns_cte.sr_ticket_number
    AND store_sales_cte.ss_item_sk = store_returns_cte.sr_item_sk
),

ss_sr_s as (
	select ss_net_paid, ss_store_sk, ss_item_sk, s_store_name, s_state, s_store_sk,
	s_market_id, ss_customer_sk, s_zip
	from ss_sr
	join 
	store_cte
	on ss_store_sk = s_store_sk
	where s_market_id= {{MARKET}}
),

ss_sr_s_i as (
	select ss_net_paid, ss_store_sk, ss_item_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, ss_customer_sk, s_zip
	from ss_sr_s
	join
	item_cte
	on ss_item_sk = i_item_sk
),

ss_sr_s_i_c as (
	select ss_net_paid, ss_store_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, c_last_name, ss_customer_sk, c_current_addr_sk,
	c_first_name, c_birth_country, s_zip
	from ss_sr_s_i 
	join
	customer_cte
	on ss_customer_sk = c_customer_sk
),

ss_sr_s_i_c_ca as (
	select ss_net_paid, ss_store_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, ca_state, c_current_addr_sk,
	c_last_name, c_first_name, c_birth_country, s_zip
	from ss_sr_s_i_c 
	join
	customer_address_cte
	on c_current_addr_sk = ca_address_sk
	where c_birth_country <> upper(ca_country)
	and s_zip = ca_zip
	
),

ssales as(
    select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_paid) netpaid
from ss_sr_s_i_c_ca
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
        
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = {{COLOR1}}
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid) from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name





----COLOR.2 = saddle



ss_sr AS (
    SELECT ss_net_paid, ss_store_sk, ss_item_sk, ss_customer_sk
    FROM store_sales_cte
    JOIN store_returns_cte
    ON store_sales_cte.ss_ticket_number = store_returns_cte.sr_ticket_number
    AND store_sales_cte.ss_item_sk = store_returns_cte.sr_item_sk
),

ss_sr_s as (
	select ss_net_paid, ss_store_sk, ss_item_sk, s_store_name, s_state, s_store_sk,
	s_market_id, ss_customer_sk, s_zip
	from ss_sr
	join 
	store_cte
	on ss_store_sk = s_store_sk
	where s_market_id= {{MARKET}}
),

ss_sr_s_i as (
	select ss_net_paid, ss_store_sk, ss_item_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, ss_customer_sk, s_zip
	from ss_sr_s
	join
	item_cte
	on ss_item_sk = i_item_sk
),

ss_sr_s_i_c as (
	select ss_net_paid, ss_store_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, c_last_name, ss_customer_sk, c_current_addr_sk,
	c_first_name, c_birth_country, s_zip
	from ss_sr_s_i 
	join
	customer_cte
	on ss_customer_sk = c_customer_sk
),

ss_sr_s_i_c_ca as (
	select ss_net_paid, ss_store_sk, s_store_name, s_state, s_store_sk,
	i_color, i_current_price, i_manager_id, i_units, i_size, ca_state, c_current_addr_sk,
	c_last_name, c_first_name, c_birth_country, s_zip
	from ss_sr_s_i_c 
	join
	customer_address_cte
	on c_current_addr_sk = ca_address_sk
	where c_birth_country <> upper(ca_country)
	and s_zip = ca_zip
	
),

ssales as(
    select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_paid) netpaid
from ss_sr_s_i_c_ca
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
        
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = {{COLOR2}}
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid) from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name