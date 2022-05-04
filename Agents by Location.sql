-- Number of agents who took customer service cases in Q4 2021 by agent location
-- There are some entry errors when agents enter location and can't map to iso_2_mapping table. However, the problem is solve by using a case when function to catrgorize agent regions.
with cte as (
select
     agentid,
    case when address1_country in ('United States of America','USA', 'US')  then 'US'
     when address1_country = 'Remote' then 'Remote'
     when address1_country like '%Korea%' then 'APAC'
     when address1_country = 'UK' then 'EMEA'
     else financial_regions end as agent_financial_region
from customerservice.fact_case f
join customerservice.dim_agent a on f.owninguserid = a.agentid
left join analytics_core.iso_2_mapping i
on a.address1_country = i.name
where f.createdon between '2021-10-01' and '2021-12-31'
)
select
    agent_financial_region,
    count(distinct agentid) Agents
from cte
group by 1