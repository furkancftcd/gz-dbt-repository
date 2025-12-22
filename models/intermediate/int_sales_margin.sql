with sales as (

    select
        orders_id,
        pdt_id,
        date_date,
        quantity,
        revenue
    from {{ ref('stg_raw__sales') }}

),

product as (

    select
        products_id,
        purchase_price
    from {{ ref('stg_raw__product') }}

),

joined as (

    select
        s.orders_id,
        s.pdt_id,
        s.date_date,
        s.quantity,
        s.revenue,
        p.purchase_price,
        s.quantity * p.purchase_price as purchase_cost,
        s.revenue - (s.quantity * p.purchase_price) as margin
    from sales s
    left join product p
        on s.pdt_id = p.products_id

)

select * from joined
