-- To get number of contacts by date, contact reason, queuetype, channel, vertical, region, seller tier and visualize in Tableau.
-- I first aggreate the total contacts by each segmention, then sum in Tableau for specific dimension, e.g. channel.

-- All contacts / cases are from fact_case table, I normalized the channel using caseorigin column.
with fact_case_new as (
select
    incidentid,
    ticketnumber,
    customerid,
    stx_ordernumber ordernumber,
    trunc(createdon) case_createdon,
    stx_contactreasontype,
    --stx_contactreason,
    stx_contactreasonsubtype,
    cast(split_part(first_queuename,'-',2) as text) queuetype,
    case when caseorigin = 'Virtual Teammate' then 'Chat'
            when caseorigin in ('Email','Solvy','Web','Internal') then 'Email'
            when caseorigin ='Help Page' then 'Web'
            when caseorigin in ('Phone','Callback') then 'Phone'
            else 'Others' end as Channel
from customerservice.fact_case f
where case_createdon between '2021-10-01' and '2021-12-31'
),
-- Join core_orders_ods table with core_seller_program_tiers to get the seller tier for ordernumbers
core_orders_new as (
select order_id,
       ask_user_uuid,
       created_at,
       trunc(created_at) order_createdon,
       vertical_name,
       seller_program_tier,
       seller_uuid
    from analytics_core.core_orders_ods o
    join analytics_core.core_seller_program_tiers s on o.ask_user_uuid = s.seller_uuid
where o.created_at between tier_start_datetime and tier_end_datetime
),
-- Join above tables. Not all contacts have ordernumber, some are general asking for account or feedback. The vertical for those contacts are 'non_vertical'. 
-- For contacts w/ ordernumber, they may from buyer or seller, using the filter below to tell if the contact customer is a seller, then return their seller_tier.
cte as (
SELECT
    f.*,
    o.*,
    --fullname dim_contact_name,
    iso2_financial_regions,
    case when stx_stockxid = o.seller_uuid then seller_program_tier else Null end seller_tier,
    coalesce(vertical_name, 'non_vertical') vertical
FROM
    fact_case_new f
join customerservice.dim_contact d on f.customerid = d.contactid
left join core_orders_new o on o.order_id = f.ordernumber
)
-- Aggregate contatcs by different dimensions and import data to Tableau for further aggregation. Thus, the filter for each dimension in Tableau will work.
select case_createdon,
    stx_contactreasontype,
    stx_contactreasonsubtype,
    queuetype,
    Channel,
    vertical,
    iso2_financial_regions,
    seller_tier,
    count(distinct ticketnumber) contacts
from cte
group by 1,2,3,4,5,6,7,8
