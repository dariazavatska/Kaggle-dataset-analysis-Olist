-- 1 order count over years
-- 2 insight: our business grows, especially between 2016 and 2017
-- 3 next question: perhaps if i try aggregating by month i will see the details of this growth?
select extract(year from order_purchase_timestamp::date) as year, count(distinct order_id) as order_count
from olist_orders_dataset ood 
where order_status = 'delivered'
group by year
order by year

-- 1 order count over months
-- 2 insight: data is incomplete - it's just that there's data for only 3 months of 2016 - so the results over years are not correct 
-- there's a growth trend peaking between November 2017 and January 2018 - perhaps it's holidays?
-- 3 next question: let's see if it corelates with number of active users
select date_trunc('month' , order_purchase_timestamp::date)::date as month, count(distinct order_id) as order_count
from olist_orders_dataset ood 
where order_status = 'delivered'
group by month
order by month

-- 1 order and customer count over months
-- 2 insight: right now my counts for both orders and customers is the same. which is weird. even if i delete distinct clause the result is the same.
-- 3 next question: why numbers are the same? find out what's wrong
select date_trunc('month' , order_purchase_timestamp::date)::date as month, count(distinct order_id) as order_count, count(distinct customer_id) as customer_count
from olist_orders_dataset ood 
where order_status = 'delivered'
group by month
order by month

-- 1 checking customer count hypothesis
-- 2 insight: the results in previous query where wrong because real customer ids are in the separate table. 
-- the numbers are not that far apart though. do people order only once mostly and never come back to our online store?
-- 3 next question: how low are retention rates? how many orders are there per customer?
select count(distinct customer_id), count(distinct customer_unique_id)
from olist_customers_dataset ocd 

-- 1 orders per customer
-- checking hypothesis about customers placing one order and never coming back
-- 2 insight: almost 97% place only one order. 
-- 3 next question: did users have enough time to order again?
with customer_order_count as 
(
select 
customer_unique_id,
case 
	when customer_order_count = 1 then '1'
	when customer_order_count = 2 then '2'
	else '2+'
end as order_count
from (
select customer_unique_id, count(order_id) as customer_order_count
from olist_customers_dataset ocd 
left join olist_orders_dataset ood using (customer_id)
group by customer_unique_id
) as t
)
select 
order_count, 
count(customer_unique_id), 
round(count(customer_unique_id)/ (sum(count(customer_unique_id)) over ()::decimal)*100, 2) as order_count_perc
from customer_order_count 
group by order_count
order by order_count_perc desc

-- 1 orders per customer grouped by first purchase month
-- checking hypothesis about customers placing one order and never coming back
-- 2 insight: across all cohorts, the majority of users make only one purchase. 
-- every month percentage of people that order once is over 90%. 
-- however, newer cohorts have had less time to return.  
-- 3 next question: let's check retention rates. are there any people that regularly order? if so, why is that? and why people don't come back?
select 
start_month, 
customer_order_count, 
count(customer_order_count), 
round((count(customer_order_count)::decimal/sum(count(customer_order_count)) over(partition by start_month))*100, 2)
from 
(
select customer_unique_id, count(order_id) as customer_order_count, date_trunc('month' , min(order_purchase_timestamp)::date)::date as start_month
from olist_customers_dataset ocd 
left join olist_orders_dataset ood using (customer_id)
group by customer_unique_id
order by start_month
) as t
group by start_month, customer_order_count
order by start_month, customer_order_count

-- 1 retention rate
-- 2 insight: retention is low (below 1% after first month in each cohort), which suggests mostly one-time purchase behavior. needs investigating
-- 3 next question: what are the sales dynamics over time? what makes the retention rate so low?  
with temp as 
(
select 
customer_unique_id,
date_trunc('month', order_purchase_timestamp::date) as order_month, 
date_trunc('month', start_date::date) as start_month

from
(
select 
distinct customer_unique_id, min(order_purchase_timestamp) over (partition by customer_unique_id) as start_date, order_purchase_timestamp
from olist_orders_dataset ood
left join olist_customers_dataset ocd using (customer_id)
order by customer_unique_id
) as t
)
select 
    start_month,
    order_month,
    (extract(year from order_month) - extract(year from start_month)) * 12 +
    (extract(month from order_month) - extract(month from start_month)) as month_number,
    count(distinct customer_unique_id) as users,
    round((count(distinct customer_unique_id)::decimal/ max(count(distinct customer_unique_id)) over(partition by start_month))*100, 3)
from temp
group by start_month, order_month
order by start_month, month_number

-- Key takeaways
-- order growth over years is misleading due to incomplete 2016 data
-- business shows steady growth with seasonal peaks (Nov–Jan)
-- majority of customers (~97%) make only one purchase
-- retention is low, indicating one-time purchase behavior
-- what are the sales dynamics over time? what makes the retention rate so low?
