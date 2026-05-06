-- 1 exploring data structure
select * from olist_order_items_dataset ooid 

select * from olist_orders_dataset ood 

-- 1 order prices
select order_id, sum(price)
from olist_order_items_dataset ooid 
group by order_id
order by order_id

-- 1 checking statuses
-- 2 insight: successful order has delivered status
select order_status from olist_orders_dataset ood group by order_status

-- 1 order prices for successful orders
select order_id, order_status, sum(price)
from olist_order_items_dataset ooid 
left join olist_orders_dataset ood using (order_id)
where order_status = 'delivered'
group by order_id, order_status
order by order_id

-- 1 average order price
select avg(order_price) from 
(
select order_id, order_status, sum(price) as order_price
from olist_order_items_dataset ooid 
left join olist_orders_dataset ood using (order_id)
where order_status = 'delivered'
group by order_id, order_status
order by order_id
)


-- 1 sales by month
-- 2 insight: sales are growing, weird totals for sept and dec 2016
-- 3 next question: how fast are sales growing? what are total sales?
select date_trunc('month', purchase_time)::date as purchase_month, sum(order_price)
from 
(
select order_id, order_purchase_timestamp::date as purchase_time, sum(price) as order_price
from olist_order_items_dataset ooid 
left join olist_orders_dataset ood using (order_id)
where order_status = 'delivered'
group by order_id, order_purchase_timestamp
order by order_id
) as t
group by purchase_month
order by purchase_month

