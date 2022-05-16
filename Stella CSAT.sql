-- upload CSAT data
CREATE TABLE bi.CSAT3 (
    request_id  varchar(256) encode zstd,
    request_sent_at timestamp,
    request_delivery_status varchar(200),
    response_received_at timestamp,
    star_rating int,
    ext_interaction_id varchar(256) encode zstd,
    channel varchar(200),
    customer_name varchar(200),
    employee_email varchar(200),
    language varchar(200),
    country varchar(200)
)
    DISTKEY (request_id)
    SORTKEY (request_id);

COPY bi.CSAT3
FROM 's3://stockx-analytics-sftp/business-intelligence/upload_files/CSAT.csv' -- Your team's assigned folder path
IAM_ROLE 'arn:aws:iam::951752409922:role/stockx_analytics_sftp_redshift_role'
IGNOREHEADER 1  -- If the file has a header
CSV             -- If the file is a .csv

select * from bi.CSAT3 limit 10;


-- Compare CSAT by different time periods

with CSAT_Mar as (
SELECT
    stx_contactreason,
    round(sum(star_rating)*1.00/count(request_id),2) csat_mar,
    count(request_id) num_requests_mar
FROM
    bi.CSAT3 a
JOIN
    customerservice.fact_case f on a.ext_interaction_id = f.incidentid
where star_rating is not null and trunc(request_sent_at) < '2022-04-01'
GROUP BY 1
),
CSAT_Apr as (
select stx_contactreason,
round(sum(star_rating)*1.00/count(request_id),2) csat_apr,
count(request_id) num_requests_apr
FROM
    bi.CSAT3 b
JOIN
    customerservice.fact_case f on b.ext_interaction_id = f.incidentid
WHERE
    star_rating is not null and trunc(request_sent_at) >= '2022-04-01'
GROUP BY 1
)
select coalesce(CSAT_Mar.stx_contactreason, CSAT_Apr.stx_contactreason),
       csat_mar,
       num_requests_mar,
       csat_apr,
       num_requests_apr
    from CSAT_Mar full join CSAT_Apr
    on CSAT_Mar.stx_contactreason = CSAT_Apr.stx_contactreason
order by CSAT_Apr.stx_contactreason;