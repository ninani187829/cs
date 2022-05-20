-- The purpose of this query is to understand the latency between customer placing order and customer contacting agents, then we can forecast agent HC better
with first_contact as (
select * from (
    select
        incidentid,
        ticketnumber,
        customerid,
        stx_contactreasontype,
        stx_contactreasonsubtype,
        stx_contactreason,
        stx_ordernumber,
        createdon,
        caseorigin,
        row_number() over (partition by stx_ordernumber, customerid order by createdon) rnk
    -- One customer may contact agents on same ordernumber for multiple times, 
    -- we only count the 1st contact to calculate the latency between order createdon and customer contact createdon
    from customerservice.fact_case
    -- where trunc(createdon) >= '2022-05-01'
    ) as sub
where rnk = 1
),
latency as (
select
    incidentid,
    ticketnumber,
    customerid,
    stx_contactreasontype,
    stx_contactreasonsubtype,
    stx_contactreason,
    stx_ordernumber ordernumber,
    f.createdon case_createdon,
    o.created_at order_createdon,
    case when date_diff('day',o.created_at,f.createdon) < 1 then '< 1 Day'
         when date_diff('day',o.created_at,f.createdon) between 1 and 3 then '1 - 3 Days'
         when date_diff('day',o.created_at,f.createdon) between 4 and 7 then '4 - 7 Days'
         when date_diff('day',o.created_at,f.createdon) between 8 and 31 then '1 week - 1 Month'
         else '> 1 Month' end as days_of_latency
    -- Most latency is less than 1 month, maxium latency can be up to 
from first_contact f
join analytics_core.core_orders_ods o
on f.stx_ordernumber = o.order_id
where days_of_latency > 0 and trunc(o.created_at) >= '2022-05-01'
-- 1% of orders have order date later than contact date, this number is small and can be ignored
)
select
    stx_contactreasontype,
    stx_contactreasonsubtype,
    stx_contactreason,
    days_of_latency,
    count(ordernumber) cnt
from latency
group by 1,2,3,4
order by 1,2,3,4
-- Show the results in Tableau and filter by contact reasons