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
create table s2.web_finance as select * from web_finance where sys_load_time > '2019-02-17' and sys_load_time < '2019-05-18';
drop table if exists s2.web_consensus;
create table s2.web_consensus as select * from web_consensus where sys_load_time > '2019-02-17' and sys_load_time < '2019-05-18';

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
 SELECT concat(t.code, '.AX') as code ,
    t.load_date,
        CASE
            WHEN (t.sb + t.nb + t.hd + t.ns + t.ss) <> 0::numeric THEN (t.sb * 1::numeric + t.nb * 2::numeric + t.hd * 3::numeric + t.ns * 4::numeric + t.ss * 5::numeric) / (t.sb + t.nb + t.hd + t.ns + t.ss)
            ELSE 5::numeric
        END::numeric(5,3) AS adv_ratio,
    t.sb + t.nb + t.hd + t.ns + t.ss AS num,
	high,median,low,average
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
		 	case when substring(high, 2,length(high)) = '' then null else cast(substring(high, 2,length(high)) as numeric) end as high
,	case when substring(median, 2,length(median)) = '' then null else cast(substring(median, 2,length(median)) as numeric) end as median		
,	case when substring(low, 2,length(low)) = '' then null else cast(substring(low, 2,length(low)) as numeric) end as low
,	case when substring(avearge, 2,length(avearge)) = '' then null else cast(substring(avearge, 2,length(avearge)) as numeric) end as average
,
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

drop table if exists temp_web_finance_clean;
create temp table temp_web_finance_clean as 
select f.code,f.load_date, f.date_index, f.market_open,f.market_previous_close,
f.market_day_high, f.market_day_low, f.market_price, f.market_volume, c.low as target_low_price, 
c.high as target_high_price, c.median as target_median_price,c.average as target_mean_price, 
c.num as number_of_analyst, c.adv_ratio as recommendation_mean, f.tailing_pe, f.forward_pe,
f.average_volume, f.ask, f.ask_size, f.bid, f.bid_size, f.enterprise_revenue, f.profit_margins, 
f.enterprise_ebitda, f.quarterly_earning_growth
from s2.web_finance_clean f left join s2.web_consensus_clean  c
on f.code = c.code and f.load_date = c.load_date;

drop table if exists s2.transactions;
create table s2.transactions as 
select f.*, s.short_position, s.total_in_issue, s.reported_position 
from temp_web_finance_clean f left outer join s2.short_clean s 
on f.code=s.code and f.load_date=s.load_date;

drop table if exists s1.performance;
create table s1.performance as 
select s.code, s.date_index, s.market_open as start_price,
e.market_price as end_price
from s1.transactions s inner join s1.transactions e
on s.code = e.code and s.date_index = e.date_index -28;

drop table if exists s2.performance;
create table s2.performance as 
select s.code, s.date_index, s.market_open as start_price,
e.market_price as end_price
from s2.transactions s inner join s2.transactions e
on s.code = e.code and s.date_index = e.date_index -28;


drop table if exists s1.increase_ratio;
create table s1.increase_ratio as 
select code, date_index, increase_ratio,
row_number() over(partition by date_index order by increase_ratio desc) as rank from (
select code, date_index,
case when cast(market_previous_close*100 as decimal(10,4))> 0 
then (cast(market_price as numeric) - cast(market_previous_close as numeric))*100/cast(market_previous_close as numeric)
else -100 end as increase_ratio
from s1.transactions ) t
order by 2,3 desc;

drop table if exists s2.increase_ratio;
create table s2.increase_ratio as 
select code, date_index, increase_ratio,
row_number() over(partition by date_index order by increase_ratio desc) as rank from (
select code, date_index,
case when cast(market_previous_close*100 as decimal(10,4))> 0 
then (cast(market_price as numeric) - cast(market_previous_close as numeric))*100/cast(market_previous_close as numeric)
else -100 end as increase_ratio
from s2.transactions ) t
order by 2,3 desc;

drop table if exists s1.volume_rank;
create table s1.volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from s1.transactions  t
order by 2,3  desc ;

drop table if exists s2.volume_rank;
create table s2.volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from s2.transactions  t
order by 2,3  desc ;

drop table if exists s1.pe_rank;
create table s1.pe_rank as 
select code, date_index, case when  forward_pe is null then -999 else forward_pe end as forward_pe,
row_number() over(partition by date_index order by case when forward_pe is null then -999 else forward_pe end desc) as rank
from s1.transactions t
order by 2 desc , 3 desc;

drop table if exists s2.pe_rank;
create table s2.pe_rank as 
select code, date_index, case when  forward_pe is null then -999 else forward_pe end as forward_pe,
row_number() over(partition by date_index order by case when forward_pe is null then -999 else forward_pe end desc) as rank
from s2.transactions t
order by 2 desc , 3 desc;


drop table if exists s1.market_price;
create table s1.market_price as
select code, date_index , json_build_array(
  (mp19 - mp19)/avgmp , (mp18 - mp19)/avgmp , (mp17 - mp19)/avgmp 
, (mp16 - mp19)/avgmp , (mp15 - mp19)/avgmp , (mp14 - mp19)/avgmp
, (mp13 - mp19)/avgmp , (mp12 - mp19)/avgmp , (mp11 - mp19)/avgmp
, (mp10 - mp19)/avgmp , (mp09 - mp19)/avgmp , (mp08 - mp19)/avgmp
, (mp07 - mp19)/avgmp , (mp06 - mp19)/avgmp , (mp05 - mp19)/avgmp
, (mp04 - mp19)/avgmp , (mp03 - mp19)/avgmp , (mp02 - mp19)/avgmp
, (mp01 - mp19)/avgmp , (mp00 - mp19)/avgmp ) as list20
, json_build_array(
  (mp09 - mp09)/avgmp , (mp08 - mp09)/avgmp , (mp07 - mp09)/avgmp
, (mp06 - mp09)/avgmp , (mp05 - mp09)/avgmp , (mp04 - mp09)/avgmp
, (mp03 - mp09)/avgmp , (mp02 - mp09)/avgmp , (mp01 - mp09)/avgmp
, (mp00 - mp09)/avgmp ) as list10
, json_build_array(
  (mp04 - mp04)/avgmp , (mp03 - mp04)/avgmp , (mp02 - mp04)/avgmp
, (mp01 - mp04)/avgmp , (mp00 - mp04)/avgmp ) as list05 from
(select case when mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19  = 0
	then null else (mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19)/20 end as avgmp
, * from ( select code, date_index, market_price as mp00
, lag(market_price, 01) over( partition by code order by load_date) as mp01
, lag(market_price, 02) over( partition by code order by load_date) as mp02
, lag(market_price, 03) over( partition by code order by load_date) as mp03
, lag(market_price, 04) over( partition by code order by load_date) as mp04
, lag(market_price, 05) over( partition by code order by load_date) as mp05
, lag(market_price, 06) over( partition by code order by load_date) as mp06
, lag(market_price, 07) over( partition by code order by load_date) as mp07
, lag(market_price, 08) over( partition by code order by load_date) as mp08
, lag(market_price, 09) over( partition by code order by load_date) as mp09
, lag(market_price, 10) over( partition by code order by load_date) as mp10
, lag(market_price, 11) over( partition by code order by load_date) as mp11
, lag(market_price, 12) over( partition by code order by load_date) as mp12
, lag(market_price, 13) over( partition by code order by load_date) as mp13
, lag(market_price, 14) over( partition by code order by load_date) as mp14
, lag(market_price, 15) over( partition by code order by load_date) as mp15
, lag(market_price, 16) over( partition by code order by load_date) as mp16
, lag(market_price, 17) over( partition by code order by load_date) as mp17
, lag(market_price, 18) over( partition by code order by load_date) as mp18
, lag(market_price, 19) over( partition by code order by load_date) as mp19
from s1.transactions ) t ) t  where date_index >=20;

drop table if exists s2.market_price;
create table s2.market_price as
select code, date_index , json_build_array(
  (mp19 - mp19)/avgmp , (mp18 - mp19)/avgmp , (mp17 - mp19)/avgmp 
, (mp16 - mp19)/avgmp , (mp15 - mp19)/avgmp , (mp14 - mp19)/avgmp
, (mp13 - mp19)/avgmp , (mp12 - mp19)/avgmp , (mp11 - mp19)/avgmp
, (mp10 - mp19)/avgmp , (mp09 - mp19)/avgmp , (mp08 - mp19)/avgmp
, (mp07 - mp19)/avgmp , (mp06 - mp19)/avgmp , (mp05 - mp19)/avgmp
, (mp04 - mp19)/avgmp , (mp03 - mp19)/avgmp , (mp02 - mp19)/avgmp
, (mp01 - mp19)/avgmp , (mp00 - mp19)/avgmp ) as list20
, json_build_array(
  (mp09 - mp09)/avgmp , (mp08 - mp09)/avgmp , (mp07 - mp09)/avgmp
, (mp06 - mp09)/avgmp , (mp05 - mp09)/avgmp , (mp04 - mp09)/avgmp
, (mp03 - mp09)/avgmp , (mp02 - mp09)/avgmp , (mp01 - mp09)/avgmp
, (mp00 - mp09)/avgmp ) as list10
, json_build_array(
  (mp04 - mp04)/avgmp , (mp03 - mp04)/avgmp , (mp02 - mp04)/avgmp
, (mp01 - mp04)/avgmp , (mp00 - mp04)/avgmp ) as list05 from
(select case when mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19  = 0
	then null else (mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19)/20 end as avgmp
, * from ( select code, date_index, market_price as mp00
, lag(market_price, 01) over( partition by code order by load_date) as mp01
, lag(market_price, 02) over( partition by code order by load_date) as mp02
, lag(market_price, 03) over( partition by code order by load_date) as mp03
, lag(market_price, 04) over( partition by code order by load_date) as mp04
, lag(market_price, 05) over( partition by code order by load_date) as mp05
, lag(market_price, 06) over( partition by code order by load_date) as mp06
, lag(market_price, 07) over( partition by code order by load_date) as mp07
, lag(market_price, 08) over( partition by code order by load_date) as mp08
, lag(market_price, 09) over( partition by code order by load_date) as mp09
, lag(market_price, 10) over( partition by code order by load_date) as mp10
, lag(market_price, 11) over( partition by code order by load_date) as mp11
, lag(market_price, 12) over( partition by code order by load_date) as mp12
, lag(market_price, 13) over( partition by code order by load_date) as mp13
, lag(market_price, 14) over( partition by code order by load_date) as mp14
, lag(market_price, 15) over( partition by code order by load_date) as mp15
, lag(market_price, 16) over( partition by code order by load_date) as mp16
, lag(market_price, 17) over( partition by code order by load_date) as mp17
, lag(market_price, 18) over( partition by code order by load_date) as mp18
, lag(market_price, 19) over( partition by code order by load_date) as mp19
from s2.transactions ) t ) t  where date_index >=20;

drop table if exists s1.volume_rank;
create table s1.volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from s1.transactions  t
order by 2,3  desc ;

drop table if exists s2.volume_rank;
create table s2.volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from s2.transactions  t
order by 2,3  desc ;

drop table if exists s1.temp_sub ;
create table s1.temp_sub as 
select code, date_index, average_volume,
market_volume * (market_price - market_previous_close)/market_previous_close as strength
from s1.transactions where market_previous_close <> 0 and  market_previous_close is not null;
create index idx on s1.temp_sub (code);

drop table if exists s2.temp_sub ;
create table s2.temp_sub as 
select code, date_index, average_volume,
market_volume * (market_price - market_previous_close)/market_previous_close as strength
from s2.transactions where market_previous_close <> 0 and  market_previous_close is not null;
create index idx on s2.temp_sub (code);

drop table if exists temp_sub ;
create temp table temp_sub as select * from s1.temp_sub;
create index idx on temp_sub (code);

drop table if exists s1.temp_strength;
create table s1.temp_strength as 
select t0.code, t0.date_index , t0.strength as current_strength
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5)/15/t0.average_volume as back05
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5
+  t06.strength*6 + t07.strength*7 + t08.strength*8 + t09.strength*9 + t10.strength*10)/55/t0.average_volume as back10
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5
+  t06.strength*6 + t07.strength*7 + t08.strength*8 + t09.strength*9 + t10.strength*10
+  t11.strength*11 + t12.strength*12 + t13.strength*13 + t14.strength*14 + t15.strength*15
+  t16.strength*16 + t17.strength*17 + t18.strength*18 + t19.strength*19 + t20.strength*20)/210/t0.average_volume as back20
from temp_sub t0
inner join temp_sub t01 on t0.date_index = t01.date_index +1  and t0.code = t01.code 
inner join temp_sub t02 on t0.date_index = t02.date_index +2  and t0.code = t02.code
inner join temp_sub t03 on t0.date_index = t03.date_index +3  and t0.code = t03.code
inner join temp_sub t04 on t0.date_index = t04.date_index +4  and t0.code = t04.code
inner join temp_sub t05 on t0.date_index = t05.date_index +5  and t0.code = t05.code
inner join temp_sub t06 on t0.date_index = t06.date_index +6  and t0.code = t06.code
inner join temp_sub t07 on t0.date_index = t07.date_index +7  and t0.code = t07.code
inner join temp_sub t08 on t0.date_index = t08.date_index +8  and t0.code = t08.code
inner join temp_sub t09 on t0.date_index = t09.date_index +9  and t0.code = t09.code
inner join temp_sub t10 on t0.date_index = t10.date_index +10 and t0.code = t10.code
inner join temp_sub t11 on t0.date_index = t11.date_index +11 and t0.code = t11.code
inner join temp_sub t12 on t0.date_index = t12.date_index +12 and t0.code = t12.code
inner join temp_sub t13 on t0.date_index = t13.date_index +13 and t0.code = t13.code
inner join temp_sub t14 on t0.date_index = t14.date_index +14 and t0.code = t14.code
inner join temp_sub t15 on t0.date_index = t15.date_index +15 and t0.code = t15.code
inner join temp_sub t16 on t0.date_index = t16.date_index +16 and t0.code = t16.code
inner join temp_sub t17 on t0.date_index = t17.date_index +17 and t0.code = t17.code
inner join temp_sub t18 on t0.date_index = t18.date_index +18 and t0.code = t18.code
inner join temp_sub t19 on t0.date_index = t19.date_index +19 and t0.code = t19.code
inner join temp_sub t20 on t0.date_index = t20.date_index +20 and t0.code = t20.code;


drop table if exists temp_sub ;
create temp table temp_sub as select * from s2.temp_sub;
create index idx on temp_sub (code);

drop table if exists s2.temp_strength;
create  table s2.temp_strength as 
select t0.code, t0.date_index , t0.strength as current_strength
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5)/15/t0.average_volume as back05
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5
+  t06.strength*6 + t07.strength*7 + t08.strength*8 + t09.strength*9 + t10.strength*10)/55/t0.average_volume as back10
, (t01.strength*1 + t02.strength*2 + t03.strength*3 + t04.strength*4 + t05.strength*5
+  t06.strength*6 + t07.strength*7 + t08.strength*8 + t09.strength*9 + t10.strength*10
+  t11.strength*11 + t12.strength*12 + t13.strength*13 + t14.strength*14 + t15.strength*15
+  t16.strength*16 + t17.strength*17 + t18.strength*18 + t19.strength*19 + t20.strength*20)/210/t0.average_volume as back20
from temp_sub t0
inner join temp_sub t01 on t0.date_index = t01.date_index +1  and t0.code = t01.code 
inner join temp_sub t02 on t0.date_index = t02.date_index +2  and t0.code = t02.code
inner join temp_sub t03 on t0.date_index = t03.date_index +3  and t0.code = t03.code
inner join temp_sub t04 on t0.date_index = t04.date_index +4  and t0.code = t04.code
inner join temp_sub t05 on t0.date_index = t05.date_index +5  and t0.code = t05.code
inner join temp_sub t06 on t0.date_index = t06.date_index +6  and t0.code = t06.code
inner join temp_sub t07 on t0.date_index = t07.date_index +7  and t0.code = t07.code
inner join temp_sub t08 on t0.date_index = t08.date_index +8  and t0.code = t08.code
inner join temp_sub t09 on t0.date_index = t09.date_index +9  and t0.code = t09.code
inner join temp_sub t10 on t0.date_index = t10.date_index +10 and t0.code = t10.code
inner join temp_sub t11 on t0.date_index = t11.date_index +11 and t0.code = t11.code
inner join temp_sub t12 on t0.date_index = t12.date_index +12 and t0.code = t12.code
inner join temp_sub t13 on t0.date_index = t13.date_index +13 and t0.code = t13.code
inner join temp_sub t14 on t0.date_index = t14.date_index +14 and t0.code = t14.code
inner join temp_sub t15 on t0.date_index = t15.date_index +15 and t0.code = t15.code
inner join temp_sub t16 on t0.date_index = t16.date_index +16 and t0.code = t16.code
inner join temp_sub t17 on t0.date_index = t17.date_index +17 and t0.code = t17.code
inner join temp_sub t18 on t0.date_index = t18.date_index +18 and t0.code = t18.code
inner join temp_sub t19 on t0.date_index = t19.date_index +19 and t0.code = t19.code
inner join temp_sub t20 on t0.date_index = t20.date_index +20 and t0.code = t20.code;


drop table if exists temp_rsv;
create temp table temp_rsv as 
select code, load_date, date_index, 
case when top = down then 0 else (recent - down)/(top - down) end as rsv from
( select code, load_date, date_index,  market_previous_close as recent,
greatest(
 lag(market_day_high, 1) over( partition by code order by load_date)
, lag(market_day_high, 2) over( partition by code order by load_date)
, lag(market_day_high, 3) over( partition by code order by load_date)
, lag(market_day_high, 4) over( partition by code order by load_date)
, lag(market_day_high, 5) over( partition by code order by load_date)
, lag(market_day_high, 6) over( partition by code order by load_date)
, lag(market_day_high, 7) over( partition by code order by load_date)
, lag(market_day_high, 8) over( partition by code order by load_date)
, lag(market_day_high, 9) over( partition by code order by load_date)
, lag(market_day_high, 10) over( partition by code order by load_date)
) as top, least(
 lag(market_day_low, 1) over( partition by code order by load_date)
, lag(market_day_low, 2) over( partition by code order by load_date)
, lag(market_day_low, 3) over( partition by code order by load_date)
, lag(market_day_low, 4) over( partition by code order by load_date)
, lag(market_day_low, 5) over( partition by code order by load_date)
, lag(market_day_low, 6) over( partition by code order by load_date)
, lag(market_day_low, 7) over( partition by code order by load_date)
, lag(market_day_low, 8) over( partition by code order by load_date)
, lag(market_day_low, 9) over( partition by code order by load_date)
, lag(market_day_low, 10) over( partition by code order by load_date)
) as down from s1.transactions ) temp where date_index >10;

drop table if exists s1.temp_kind;
create table s1.temp_kind as 
select code, load_date, date_index, 
0.5*(2^10/3^10) + (2^9/3^10)*lag_10 + (2^8/3^9)*lag_9 + 
(2^7/3^8)*lag_8 + (2^6/3^7)*lag_7 + (2^5/3^6)*lag_6 + 
(2^4/3^5)*lag_5 + (2^3/3^4)*lag_4 + (2^2/3^3)*lag_3 + 
(2^1/3^2)*lag_2 + (2^0/3^1)*lag_1 as kind from (
select code, load_date, date_index,
lag(rsv, 1) over (partition by code order by load_date) as lag_1,
lag(rsv, 2) over (partition by code order by load_date) as lag_2,
lag(rsv, 3) over (partition by code order by load_date) as lag_3,
lag(rsv, 4) over (partition by code order by load_date) as lag_4,
lag(rsv, 5) over (partition by code order by load_date) as lag_5,
lag(rsv, 6) over (partition by code order by load_date) as lag_6,
lag(rsv, 7) over (partition by code order by load_date) as lag_7,
lag(rsv, 8) over (partition by code order by load_date) as lag_8,
lag(rsv, 9) over (partition by code order by load_date) as lag_9,
lag(rsv, 10) over (partition by code order by load_date) as lag_10
from temp_rsv) t 
where 0.5*(2^10/3^10) + (2^9/3^10)*lag_10 + (2^8/3^9)*lag_9 + 
(2^7/3^8)*lag_8 + (2^6/3^7)*lag_7 + (2^5/3^6)*lag_6 + 
(2^4/3^5)*lag_5 + (2^3/3^4)*lag_4 + (2^2/3^3)*lag_3 + 
(2^1/3^2)*lag_2 + (2^0/3^1)*lag_1 is not null;


drop table if exists temp_rsv;
create temp table temp_rsv as 
select code, load_date, date_index, 
case when top = down then 0 else (recent - down)/(top - down) end as rsv from
( select code, load_date, date_index,  market_previous_close as recent,
greatest(
 lag(market_day_high, 1) over( partition by code order by load_date)
, lag(market_day_high, 2) over( partition by code order by load_date)
, lag(market_day_high, 3) over( partition by code order by load_date)
, lag(market_day_high, 4) over( partition by code order by load_date)
, lag(market_day_high, 5) over( partition by code order by load_date)
, lag(market_day_high, 6) over( partition by code order by load_date)
, lag(market_day_high, 7) over( partition by code order by load_date)
, lag(market_day_high, 8) over( partition by code order by load_date)
, lag(market_day_high, 9) over( partition by code order by load_date)
, lag(market_day_high, 10) over( partition by code order by load_date)
) as top, least(
 lag(market_day_low, 1) over( partition by code order by load_date)
, lag(market_day_low, 2) over( partition by code order by load_date)
, lag(market_day_low, 3) over( partition by code order by load_date)
, lag(market_day_low, 4) over( partition by code order by load_date)
, lag(market_day_low, 5) over( partition by code order by load_date)
, lag(market_day_low, 6) over( partition by code order by load_date)
, lag(market_day_low, 7) over( partition by code order by load_date)
, lag(market_day_low, 8) over( partition by code order by load_date)
, lag(market_day_low, 9) over( partition by code order by load_date)
, lag(market_day_low, 10) over( partition by code order by load_date)
) as down from s2.transactions ) temp where date_index >10;


drop table if exists s2.temp_kind;
create table s2.temp_kind as 
select code, load_date, date_index, 
0.5*(2^10/3^10) + (2^9/3^10)*lag_10 + (2^8/3^9)*lag_9 + 
(2^7/3^8)*lag_8 + (2^6/3^7)*lag_7 + (2^5/3^6)*lag_6 + 
(2^4/3^5)*lag_5 + (2^3/3^4)*lag_4 + (2^2/3^3)*lag_3 + 
(2^1/3^2)*lag_2 + (2^0/3^1)*lag_1 as kind from (
select code, load_date, date_index,
lag(rsv, 1) over (partition by code order by load_date) as lag_1,
lag(rsv, 2) over (partition by code order by load_date) as lag_2,
lag(rsv, 3) over (partition by code order by load_date) as lag_3,
lag(rsv, 4) over (partition by code order by load_date) as lag_4,
lag(rsv, 5) over (partition by code order by load_date) as lag_5,
lag(rsv, 6) over (partition by code order by load_date) as lag_6,
lag(rsv, 7) over (partition by code order by load_date) as lag_7,
lag(rsv, 8) over (partition by code order by load_date) as lag_8,
lag(rsv, 9) over (partition by code order by load_date) as lag_9,
lag(rsv, 10) over (partition by code order by load_date) as lag_10
from temp_rsv) t 
where 0.5*(2^10/3^10) + (2^9/3^10)*lag_10 + (2^8/3^9)*lag_9 + 
(2^7/3^8)*lag_8 + (2^6/3^7)*lag_7 + (2^5/3^6)*lag_6 + 
(2^4/3^5)*lag_5 + (2^3/3^4)*lag_4 + (2^2/3^3)*lag_3 + 
(2^1/3^2)*lag_2 + (2^0/3^1)*lag_1 is not null;


drop table if exists s1.short_strength;
create table s1.short_strength as select code, load_date, date_index,
cast((1/(1+EXP(0-r*0.1))-0.5)*2 as decimal(5,2)) as short_strength from (
select cast(short_position*1.00/average_volume as decimal(5,2)) as r, 
reported_position, * from s1.transactions ) t ;
	
drop table if exists s2.short_strength;
create table s2.short_strength as select code, load_date, date_index,
cast((1/(1+EXP(0-r*0.1))-0.5)*2 as decimal(5,2)) as short_strength from (
select cast(short_position*1.00/average_volume as decimal(5,2)) as r, 
reported_position, * from s2.transactions ) t ;


drop table if exists s1.ask_bid;
create table s1.ask_bid as select code, load_date, date_index, 
case when split_ratio < 0 then 0.00 else split_ratio end as split_ratio,
ask_strength from (select 
 case 	when market_price <>0 then cast((ask-bid)/market_price*1.00 as decimal(10,5)) 
 		when ask-bid <> 0 then cast((ask-bid)*1.00/(ask+bid)/2 as decimal(10,5))
		else 0 end split_ratio , 
 case 	when ask < market_price or ask < bid then 0.00 
  		when bid-ask <> 0 then cast((ask-market_price)*1.00/(ask-bid)*1.00 as decimal(10,5)) 
		else null end ask_strength,
 * from s1.transactions ) t ;
 
 
drop table if exists s2.ask_bid;
create table s2.ask_bid as select code, load_date, date_index, 
case when split_ratio < 0 then 0.00 else split_ratio end as split_ratio,
ask_strength from (select 
 case 	when market_price <>0 then cast((ask-bid)/market_price*1.00 as decimal(10,5)) 
 		when ask-bid <> 0 then cast((ask-bid)*1.00/(ask+bid)/2 as decimal(10,5))
		else 0 end split_ratio , 
 case 	when ask < market_price or ask < bid then 0.00 
  		when bid-ask <> 0 then cast((ask-market_price)*1.00/(ask-bid)*1.00 as decimal(10,5)) 
		else null end ask_strength,
 * from s2.transactions ) t ;
 
drop table if exists s1.price_ratio;
create table s1.price_ratio as select code, load_date, date_index, 
case when price_ratio > 1 then 1 when price_ratio < 0 then 0 else price_ratio end as price_ratio,
row_number() over(partition by date_index order by price_ratio desc) as rank
from (select code, load_date, date_index,
case when market_day_high<> market_day_low then 
(market_price- market_day_low)/(market_day_high-market_day_low) 
else 0 end as price_ratio from s1.transactions ) t ;

drop table if exists s2.price_ratio;
create table s2.price_ratio as select code, load_date, date_index, 
case when price_ratio > 1 then 1 when price_ratio < 0 then 0 else price_ratio end as price_ratio,
row_number() over(partition by date_index order by price_ratio desc) as rank
from (select code, load_date, date_index,
case when market_day_high<> market_day_low then 
(market_price- market_day_low)/(market_day_high-market_day_low) 
else 0 end as price_ratio from s2.transactions ) t ;


END

$BODY$;

ALTER FUNCTION public.cleanup()
    OWNER TO postgres;


select * from public.cleanup();

/*
select load_date, count(*) from s1.transactions group by 1 order by 1;
select load_date, count(*) from s2.transactions group by 1 order by 1;

select cast(sys_load_time as date) 
,sum(case when buy = '' then 0 else 1 end )as val
, count(*)
from web_consensus group by 1 order by 1;
drop table if exists s1.trends;
create table s1.trends as 
select code, 'market_price_list20' as attr, list20 as val, date_index from s1.market_price ;
insert into s1.trends 
select code, 'market_price_list10' as attr, list10 as val, date_index from s1.market_price ;
insert into s1.trends 
select code, 'market_price_list05' as attr, list05 as val, date_index from s1.market_price ;

drop table if exists s2.trends;
create table s2.trends as 
select code, 'market_price_list20' as attr, list20 as val, date_index from s2.market_price ;
insert into s1.trends 
select code, 'market_price_list10' as attr, list10 as val, date_index from s2.market_price ;
insert into s1.trends 
select code, 'market_price_list05' as attr, list05 as val, date_index from s2.market_price ;



drop table if exists s1.eavt;
create table s1.eavt (
entity varchar,
attrib varchar,
valu   varchar,
dindex varchar);

drop table if exists s2.eavt;
create table s2.eavt (
entity varchar,
attrib varchar,
valu   varchar,
dindex varchar);


update s1.eavt set valu = replace(valu, ':-Infinity', ':"-Infinity"');
update s1.eavt set valu = replace(valu, ':Infinity', ':"Infinity"');
update s1.eavt set valu = replace(valu, ':NaN', ':"NaN"');

drop table if exists s1.eavt_basic;
create table s1.eavt_basic as select entity, dindex as date_index, attrib
,case when valu::json->>'average'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'average'  end::numeric as average
,case when valu::json->>'variance' in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'variance' end::numeric as variance
,case when valu::json->>'minimum'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'minimum'  end::numeric as minimum
,case when valu::json->>'pop_var'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'pop_var'  end::numeric as pop_var
,case when valu::json->>'maximum'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'maximum'  end::numeric as maximum
,case when valu::json->>'sum_log'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'sum_log'  end::numeric as sum_log
,case when valu::json->>'sum_sqr'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'sum_sqr'  end::numeric as sum_sqr
,case when valu::json->>'geo_avg'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'geo_avg'  end::numeric as geo_avg
,case when valu::json->>'fst_qrt'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'fst_qrt'  end::numeric as fst_qrt
,case when valu::json->>'lst_qrt'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'lst_qrt'  end::numeric as lst_qrt
,case when valu::json->>'lst_pct'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'lst_pct'  end::numeric as lst_pct
,case when valu::json->>'fst_pct'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'fst_pct'  end::numeric as fst_pct
from s1.eavt where attrib like '%_basic';


drop table if exists s1.eavt_curve;
create table s1.eavt_curve as select entity, dindex as date_index, attrib
,case when valu::json->>0 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>0 end::numeric as zero
,case when valu::json->>1 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>1 end::numeric as first
,case when valu::json->>2 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>2 end::numeric as second
from s1.eavt where attrib like '%_linear' or attrib like '%_quadratic';


update s2.eavt set valu = replace(valu, ':-Infinity', ':"-Infinity"');
update s2.eavt set valu = replace(valu, ':Infinity', ':"Infinity"');
update s2.eavt set valu = replace(valu, ':NaN', ':"NaN"');

drop table if exists s2.eavt_basic;
create table s2.eavt_basic as select entity, dindex as date_index, attrib
,case when valu::json->>'average'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'average'  end::numeric as average
,case when valu::json->>'variance' in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'variance' end::numeric as variance
,case when valu::json->>'minimum'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'minimum'  end::numeric as minimum
,case when valu::json->>'pop_var'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'pop_var'  end::numeric as pop_var
,case when valu::json->>'maximum'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'maximum'  end::numeric as maximum
,case when valu::json->>'sum_log'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'sum_log'  end::numeric as sum_log
,case when valu::json->>'sum_sqr'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'sum_sqr'  end::numeric as sum_sqr
,case when valu::json->>'geo_avg'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'geo_avg'  end::numeric as geo_avg
,case when valu::json->>'fst_qrt'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'fst_qrt'  end::numeric as fst_qrt
,case when valu::json->>'lst_qrt'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'lst_qrt'  end::numeric as lst_qrt
,case when valu::json->>'lst_pct'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'lst_pct'  end::numeric as lst_pct
,case when valu::json->>'fst_pct'  in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>'fst_pct'  end::numeric as fst_pct
from s2.eavt where attrib like '%_basic';


drop table if exists s2.eavt_curve;
create table s2.eavt_curve as select entity, dindex as date_index, attrib
,case when valu::json->>0 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>0 end::numeric as zero
,case when valu::json->>1 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>1 end::numeric as first
,case when valu::json->>2 in('NaN', 'Infinity', '-Infinity' ) then null else valu::json->>2 end::numeric as second
from s2.eavt where attrib like '%_linear' or attrib like '%_quadratic';






select market_volume, * from s1.transactions limit 100;


drop table if exists market_volume;
create temp table market_volume as
select code, date_index , json_build_array(
  (mp19 - mp19)/avgmp , (mp18 - mp19)/avgmp , (mp17 - mp19)/avgmp 
, (mp16 - mp19)/avgmp , (mp15 - mp19)/avgmp , (mp14 - mp19)/avgmp
, (mp13 - mp19)/avgmp , (mp12 - mp19)/avgmp , (mp11 - mp19)/avgmp
, (mp10 - mp19)/avgmp , (mp09 - mp19)/avgmp , (mp08 - mp19)/avgmp
, (mp07 - mp19)/avgmp , (mp06 - mp19)/avgmp , (mp05 - mp19)/avgmp
, (mp04 - mp19)/avgmp , (mp03 - mp19)/avgmp , (mp02 - mp19)/avgmp
, (mp01 - mp19)/avgmp , (mp00 - mp19)/avgmp ) as list20
, json_build_array(
  (mp09 - mp09)/avgmp , (mp08 - mp09)/avgmp , (mp07 - mp09)/avgmp
, (mp06 - mp09)/avgmp , (mp05 - mp09)/avgmp , (mp04 - mp09)/avgmp
, (mp03 - mp09)/avgmp , (mp02 - mp09)/avgmp , (mp01 - mp09)/avgmp
, (mp00 - mp09)/avgmp ) as list10
, json_build_array(
  (mp04 - mp04)/avgmp , (mp03 - mp04)/avgmp , (mp02 - mp04)/avgmp
, (mp01 - mp04)/avgmp , (mp00 - mp04)/avgmp ) as list05 from
(select case when mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19  = 0
	then null else (mp00 + mp01 + mp02 + mp03 + mp04 + mp05 + mp06 + mp07 + mp08 + mp09
	+ mp10 + mp11 + mp12 + mp13 + mp14 + mp15 + mp16 + mp17 + mp18 + mp19)/20 end as avgmp
, * from ( select code, date_index, market_volume as mp00
, lag(market_volume, 01) over( partition by code order by load_date) as mp01
, lag(market_volume, 02) over( partition by code order by load_date) as mp02
, lag(market_volume, 03) over( partition by code order by load_date) as mp03
, lag(market_volume, 04) over( partition by code order by load_date) as mp04
, lag(market_volume, 05) over( partition by code order by load_date) as mp05
, lag(market_volume, 06) over( partition by code order by load_date) as mp06
, lag(market_volume, 07) over( partition by code order by load_date) as mp07
, lag(market_volume, 08) over( partition by code order by load_date) as mp08
, lag(market_volume, 09) over( partition by code order by load_date) as mp09
, lag(market_volume, 10) over( partition by code order by load_date) as mp10
, lag(market_volume, 11) over( partition by code order by load_date) as mp11
, lag(market_volume, 12) over( partition by code order by load_date) as mp12
, lag(market_volume, 13) over( partition by code order by load_date) as mp13
, lag(market_volume, 14) over( partition by code order by load_date) as mp14
, lag(market_volume, 15) over( partition by code order by load_date) as mp15
, lag(market_volume, 16) over( partition by code order by load_date) as mp16
, lag(market_volume, 17) over( partition by code order by load_date) as mp17
, lag(market_volume, 18) over( partition by code order by load_date) as mp18
, lag(market_volume, 19) over( partition by code order by load_date) as mp19
from s2.transactions ) t ) t  where date_index >=20;


*/
