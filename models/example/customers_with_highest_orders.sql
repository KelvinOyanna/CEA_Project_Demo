{{ config(materialized = 'table') }}
with customers as (
select
    id as customer_id,
    first_name,
    last_name
    from {{ source('jaffle_shop', 'customers') }}
),

orders as (

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
    from {{ source('jaffle_shop', 'orders') }}

),

customer_orders as (
select concat(first_name, ' ', last_name) as full_name,
order_id
from customers
join orders 
using (customer_id)
),


final as (
select full_name,
count(distinct(order_id)) as total_orders
from customer_orders
group by 1
order by 2 desc
)

select * from final
limit 5