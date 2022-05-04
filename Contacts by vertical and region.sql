-- To get number of contacts, contact rate (total contacts / total trades) by financial_regions and verticals

-- This part is same as contact reason basic query
with fact_case_new as (
select
    incidentid,
    ticketnumber,
    --customerid,
    stx_ordernumber ordernumber,
    trunc(createdon) case_createdon,
    --stx_contactreasontype,
    --stx_contactreason,
    --stx_contactreasonsubtype,
    --cast(split_part(first_queuename,'-',2) as text) queuetype
from customerservice.fact_case f
where case_createdon between '2021-10-01' and '2021-12-31'
),
core_orders_new as (
select order_id,
       trunc(created_at) order_createdon,
       vertical_name
    from analytics_core.core_orders_ods
    where created_at is null or created_at > '2021-04-01'
    -- some orders don't have orderid and some orders are incorrectly mannual entered and pretty old with order createdon before 2021. 
    -- Because I am pulling data for Q4 2022, I assume 6 month earlier, which is '2021-04-01' can clean orders which happened a long time ago
),
cte as (
 SELECT
    --f.*,
    --o.*,
    iso2_financial_regions,
    coalesce(vertical_name, 'non_vertical') vertical,
    case_createdon,
    count(distinct ticketnumber) cnt_contacts
FROM
    fact_case_new f
join customerservice.dim_contact d on f.customerid = d.contactid
left join core_orders_new o on o.order_id = f.ordernumber
group by 1,2,3
),
-- Below table shows total trades by region, vertical and date.
total_trade as (
select
       financial_regions,
       vertical,
       trunc(created_at) order_createdon,
       count(distinct order_id) total_trade
from analytics_core.core_orders_ods c
join analytics_core.iso_2_mapping t on c.ask_user_shipping_country_id = t.alpha2
group by 1,2,3
)
-- Join total contacts and total trades to calculate contact rate (total contacts / total trades) in Tableau when filter changes, e.g. region, vertical, date range
select iso2_financial_regions,
       cte.vertical,
       case_createdon,
       cnt_contacts,
       total_trade
from cte left
join total_trade t on cte.vertical = t.vertical
and iso2_financial_regions = t.financial_regions
and cte.case_createdon = t.order_createdon
order by 1,2,3