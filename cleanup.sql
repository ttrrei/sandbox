-- FUNCTION: public.cleanup()

-- DROP FUNCTION public.cleanup();

CREATE OR REPLACE FUNCTION public.cleanup(
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN
drop table if exists s1.web_finance;
create table s1.web_finance as select * from web_finance where sys_load_time < '2018-12-15';
drop table if exists s2.web_finance;
create table s2.web_finance as select * from web_finance where sys_load_time > '2019-02-17';
drop table if exists s2.web_consensus;
create table s2.web_consensus as select * from web_consensus where sys_load_time > '2019-02-17';

drop table if exists s1.idx_date;
create table s1.idx_date as 
select load_date, row_number() over (order by load_date) as date_index from (
select distinct date(sys_load_time) as load_date from s1.web_finance) temp;

drop table if exists s2.idx_date;
create table s2.idx_date as 
select load_date, row_number() over (order by load_date) as date_index from (
select distinct date(sys_load_time) as load_date from s2.web_finance) temp;

drop table if exists s1.web_finance_clean;
create table s1.web_finance_clean as 
select code, load_date, date_index
,	coalesce(
		case when market_open = '' then null else market_open end 
	,	case when regular_market_open = '' then null else regular_market_open end 
	)::numeric as market_open
,	coalesce(
		case when market_previous_close = '' then null else market_previous_close end 
	,	case when regular_market_previous_close = '' then null else regular_market_previous_close end 
	)::numeric as market_previous_close
,	coalesce(
		case when market_day_high = '' then null else market_day_high end 
	,	case when "regular_market_day_high" = '' then null else "regular_market_day_high" end 
	)::numeric as market_day_high
,	coalesce(
		case when market_day_low = '' then null else market_day_low end 
	,	case when "regular_market_day_low" = '' then null else "regular_market_day_low" end 
	)::numeric as market_day_low
,	coalesce(
		case when market_price = '' then null else market_price end 
	,	case when current_price = '' then null else current_price end 
	)::numeric as market_price
,	coalesce(
		case when market_volume = '' then null else market_volume end 
	,	case when "regualr_market_volume" = '' then null else "regualr_market_volume" end 
	)::numeric as market_volume
,	case when target_low_price = '' then null else target_low_price end ::numeric
,	case when target_high_price = '' then null else target_high_price end ::numeric
,	case when target_median_price = '' then null else target_median_price end ::numeric
,	case when target_mean_price = '' then null else target_mean_price end ::numeric
,	case when "number_of_analyst" = '' then null else "number_of_analyst" end ::numeric
,	case when "recommendation_mean" = '' then null else "recommendation_mean" end ::numeric
,	case when "tailing_pe" = '' or "tailing_pe" = 'Infinity' then null else "tailing_pe" end ::numeric
,	case when forward_pe = '' or forward_pe = 'Infinity' then null else forward_pe end ::numeric
,	case when average_volume = '' then null else average_volume end ::numeric
,	case when ask = '' then null else ask end ::numeric
,	case when ask_size = '' then null else ask_size end ::numeric
,	case when bid = '' then null else bid end ::numeric
,	case when bid_size = '' then null else bid_size end::numeric
,	case when enterprise_revenue = '' then null else enterprise_revenue end ::numeric
,	case when profit_margins = '' then null else profit_margins end ::numeric
,	case when enterprise_ebitda = '' then null else enterprise_ebitda end::numeric
,	case when "quarterly_earning_growth" = '' then null else "quarterly_earning_growth" end::numeric
from s1.web_finance inner join s1.idx_date
on cast(web_finance.sys_load_time as date)  = idx_date.load_date;	

drop table if exists s2.web_finance_clean;
create table s2.web_finance_clean as 
select code, load_date, date_index
,	coalesce(
		case when market_open = '' then null else market_open end 
	,	case when regular_market_open = '' then null else regular_market_open end 
	)::numeric as market_open
,	coalesce(
		case when market_previous_close = '' then null else market_previous_close end 
	,	case when regular_market_previous_close = '' then null else regular_market_previous_close end 
	)::numeric as market_previous_close
,	coalesce(
		case when market_day_high = '' then null else market_day_high end 
	,	case when "regular_market_day_high" = '' then null else "regular_market_day_high" end 
	)::numeric as market_day_high
,	coalesce(
		case when market_day_low = '' then null else market_day_low end 
	,	case when "regular_market_day_low" = '' then null else "regular_market_day_low" end 
	)::numeric as market_day_low
,	coalesce(
		case when market_price = '' then null else market_price end 
	,	case when current_price = '' then null else current_price end 
	)::numeric as market_price
,	coalesce(
		case when market_volume = '' then null else market_volume end 
	,	case when "regualr_market_volume" = '' then null else "regualr_market_volume" end 
	)::numeric as market_volume
,	case when target_low_price = '' then null else target_low_price end ::numeric
,	case when target_high_price = '' then null else target_high_price end ::numeric
,	case when target_median_price = '' then null else target_median_price end ::numeric
,	case when target_mean_price = '' then null else target_mean_price end ::numeric
,	case when "number_of_analyst" = '' then null else "number_of_analyst" end ::numeric
,	case when "recommendation_mean" = '' then null else "recommendation_mean" end ::numeric
,	case when "tailing_pe" = '' or "tailing_pe" = 'Infinity' then null else "tailing_pe" end ::numeric
,	case when forward_pe = '' or forward_pe = 'Infinity' then null else forward_pe end ::numeric
,	case when average_volume = '' then null else average_volume end ::numeric
,	case when ask = '' then null else ask end ::numeric
,	case when ask_size = '' then null else ask_size end ::numeric
,	case when bid = '' then null else bid end ::numeric
,	case when bid_size = '' then null else bid_size end::numeric
,	case when enterprise_revenue = '' then null else enterprise_revenue end ::numeric
,	case when profit_margins = '' then null else profit_margins end ::numeric
,	case when enterprise_ebitda = '' then null else enterprise_ebitda end::numeric
,	case when "quarterly_earning_growth" = '' then null else "quarterly_earning_growth" end::numeric
from s2.web_finance inner join s2.idx_date
on cast(web_finance.sys_load_time as date)  = idx_date.load_date;	

drop table if exists s2.web_consensus_clean;
create table s2.web_consensus_clean as 
 SELECT t.code,
    t.load_date,
        CASE
            WHEN (t.sb + t.nb + t.hd + t.ns + t.ss) <> 0::numeric THEN (t.sb * 1::numeric + t.nb * 2::numeric + t.hd * 3::numeric + t.ns * 4::numeric + t.ss * 5::numeric) / (t.sb + t.nb + t.hd + t.ns + t.ss)
            ELSE 5::numeric
        END::numeric(5,3) AS adv_ratio,
    t.sb + t.nb + t.hd + t.ns + t.ss AS num
   FROM ( SELECT web_consensus.code,
                CASE
                    WHEN web_consensus.buy::text = ''::text OR web_consensus.buy IS NULL THEN 0::numeric
                    ELSE web_consensus.buy::numeric
                END AS sb,
                CASE
                    WHEN web_consensus.overweight::text = ''::text OR web_consensus.overweight IS NULL THEN 0::numeric
                    ELSE web_consensus.overweight::numeric
                END AS nb,
                CASE
                    WHEN web_consensus.hold::text = ''::text OR web_consensus.hold IS NULL THEN 0::numeric
                    ELSE web_consensus.hold::numeric
                END AS hd,
                CASE
                    WHEN web_consensus.underweight::text = ''::text OR web_consensus.underweight IS NULL THEN 0::numeric
                    ELSE web_consensus.underweight::numeric
                END AS ns,
                CASE
                    WHEN web_consensus.sell::text = ''::text OR web_consensus.sell IS NULL THEN 0::numeric
                    ELSE web_consensus.sell::numeric
                END AS ss,
            web_consensus.sys_load_time::date AS load_date
           FROM s2.web_consensus) t;

drop table if exists s1.short_clean;
create table s1.short_clean as 
select concat(product_code, '.AX') as code, 
case when short_position = '' then null else cast(short_position as bigint) end as short_position,
case when total_in_issue = '' then null else cast(total_in_issue as bigint) end as total_in_issue, 
case when reported_position = '' then null else cast(reported_position as numeric) end as reported_position,
cast(sys_load as date) as load_date
from public.daily_short;

drop table if exists s2.short_clean;
create table s2.short_clean as 
select concat(product_code, '.AX') as code, 
case when short_position = '' then null else cast(short_position as bigint) end as short_position,
case when total_in_issue = '' then null else cast(total_in_issue as bigint) end as total_in_issue, 
case when reported_position = '' then null else cast(reported_position as numeric) end as reported_position,
cast(sys_load as date) as load_date
from public.daily_short;

drop table if exists s1.short_clean;
create table s1.short_clean as 
select concat(trim(product_code), '.AX') as code, 
case when short_position = '' then null else cast(short_position as bigint) end as short_position,
case when total_in_issue = '' then null else cast(total_in_issue as bigint) end as total_in_issue, 
case when reported_position = '' then null else cast(reported_position as numeric) end as reported_position,
cast(sys_load as date) as load_date
from public.daily_short;
drop table if exists s2.short_clean;
create table s2.short_clean as 
select concat(trim(product_code), '.AX') as code, 
case when short_position = '' then null else cast(short_position as bigint) end as short_position,
case when total_in_issue = '' then null else cast(total_in_issue as bigint) end as total_in_issue, 
case when reported_position = '' then null else cast(reported_position as numeric) end as reported_position,
cast(sys_load as date) as load_date
from public.daily_short;

drop table if exists s1.transactions;
create table s1.transactions as 
select f.*, s.short_position, s.total_in_issue, s.reported_position 
from s1.web_finance_clean f left outer join s1.short_clean s 
on f.code=s.code and f.load_date=s.load_date;

update s1.transactions  set "market_open" =  "market_open"*10 where code = 'EHL.AX' and "market_open" < 1.0;
update s1.transactions  set "market_previous_close" =  "market_previous_close"*10 where code = 'EHL.AX' and "market_previous_close" < 1.0;
update s1.transactions  set "market_day_high" =  "market_day_high"*10 where code = 'EHL.AX' and "market_day_high" < 1.0;
update s1.transactions  set "market_day_low" =  "market_day_low"*10 where code = 'EHL.AX' and "market_day_low" < 1.0;
update s1.transactions  set "market_price" =  "market_price"*10 where code = 'EHL.AX' and "market_price" < 1.0;
update s1.transactions  set "target_low_price" =  "target_low_price"*10 where code = 'EHL.AX' and "target_low_price" < 1.0;
update s1.transactions  set "target_high_price" =  "target_high_price"*10 where code = 'EHL.AX' and "target_high_price" < 1.0;
update s1.transactions  set "target_median_price" =  "target_median_price"*10 where code = 'EHL.AX' and "target_median_price" < 1.0;
update s1.transactions  set "target_mean_price" =  "target_mean_price"*10 where code = 'EHL.AX' and "target_mean_price" < 1.0;
update s1.transactions  set "ask" =  "ask"*10 where code = 'EHL.AX' and "ask" < 1.0;
update s1.transactions  set "bid" =  "bid"*10 where code = 'EHL.AX' and "bid" < 1.0;

END

$BODY$;

ALTER FUNCTION public.cleanup()
    OWNER TO postgres;
