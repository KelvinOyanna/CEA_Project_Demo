with orders as (
    select *
    from {{ source('jaffle_shop', 'orders') }}
),

customers as (
    select *
    from {{ source('jaffle_shop', 'customers') }}
),

payments as (
    select 
        orderid,
        amount
    from {{ source('stripe', 'payment') }}
),

customer_orders as(
    select concat(first_name, ' ', last_name) as full_name,
    orders.id as orderid
    from customers
    join orders on 
    customers.id = orders.id
),

final as (
    
    select full_name,
    sum(amount) as total_purchase
    from customer_orders
    join payments using(orderid)
    group by 1
    order by 2 desc
)

select * from final