-- FUNCTION: public.org_cleanup()

-- DROP FUNCTION public.org_cleanup();

CREATE OR REPLACE FUNCTION public.org_cleanup(
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN

/*
drop table if exists output.web_sentiment_clean;
create table output.web_sentiment_clean as 
select code, publish_title, publish_date, cast(null as int) as sentiment from (
	select code, publish_title, cast(sys_load_time as date) as load_date, 
	cast(to_timestamp(cast(substring(publish_time, 1,10) as bigint)) as date) as publish_date,	
	row_number() over( partition by code, publish_title order by sys_load_time) as qrank 
	from web_sentiment
 ) t where qrank = 1 and load_date = publish_date order by code, publish_date, load_date ;
*/
drop table if exists idx_date;
create temp table idx_date as 
select load_date, row_number() over (order by load_date) as date_index from (
select distinct date(sys_load_time) as load_date from web_finance) temp;

drop table if exists output.web_finance_clean;
create table output.web_finance_clean as 
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
from web_finance inner join idx_date
on cast(web_finance.sys_load_time as date)  = idx_date.load_date;	

drop table if exists sentiment_agg;
create table sentiment_agg as 
select code, publish_date as load_date, avg(sentiment) as sentiment, count(*) as weight
from output.web_sentiment_clean group by 1,2 order by 1,2;

drop table if exists output.transactions;
create table output.transactions as 
select f.*, s.short_position, s.total_in_issue, s.reported_position,
case when s1.sentiment is null then 0 else s1.sentiment end as sentiment ,
case when s1.weight is null then 0 else s1.weight end as weight  
from output.web_finance_clean f
left outer join output.short_clean s
on f.code = s.code and f.load_date = s.load_date
left outer join sentiment_agg s1
on f.code = s1.code and f.load_date = s1.load_date;

update output.transactions  set "market_open" =  "market_open"*10 where code = 'EHL.AX' and "market_open" < 1.0;
update output.transactions  set "market_previous_close" =  "market_previous_close"*10 where code = 'EHL.AX' and "market_previous_close" < 1.0;
update output.transactions  set "market_day_high" =  "market_day_high"*10 where code = 'EHL.AX' and "market_day_high" < 1.0;
update output.transactions  set "market_day_low" =  "market_day_low"*10 where code = 'EHL.AX' and "market_day_low" < 1.0;
update output.transactions  set "market_price" =  "market_price"*10 where code = 'EHL.AX' and "market_price" < 1.0;
update output.transactions  set "target_low_price" =  "target_low_price"*10 where code = 'EHL.AX' and "target_low_price" < 1.0;
update output.transactions  set "target_high_price" =  "target_high_price"*10 where code = 'EHL.AX' and "target_high_price" < 1.0;
update output.transactions  set "target_median_price" =  "target_median_price"*10 where code = 'EHL.AX' and "target_median_price" < 1.0;
update output.transactions  set "target_mean_price" =  "target_mean_price"*10 where code = 'EHL.AX' and "target_mean_price" < 1.0;
update output.transactions  set "ask" =  "ask"*10 where code = 'EHL.AX' and "ask" < 1.0;
update output.transactions  set "bid" =  "bid"*10 where code = 'EHL.AX' and "bid" < 1.0;

drop table if exists performance;
create temp table performance as 
select s.code, s.date_index, s.market_open as start_price,
e.market_price as end_price
from output.transactions s inner join output.transactions e
on s.code = e.code and s.date_index = e.date_index -14;

drop table if exists increase_ratio;
create temp table increase_ratio as 
select code, date_index, increase_ratio,
row_number() over(partition by date_index order by increase_ratio desc) as rank from (
select code, date_index,
case when cast(market_previous_close*100 as decimal(10,4))> 0 
then (cast(market_price as numeric) - cast(market_previous_close as numeric))*100/cast(market_previous_close as numeric)
else -100 end as increase_ratio
from output.transactions ) t
order by 2,3 desc;

drop table if exists volume_rank;
create temp table volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from output.transactions  t
order by 2,3  desc ;

drop table if exists pe_rank;
create temp table pe_rank as 
select code, date_index, case when  forward_pe is null then -999 else forward_pe end as forward_pe,
row_number() over(partition by date_index order by case when forward_pe is null then -999 else forward_pe end desc) as rank
from output.transactions t
order by 2 desc , 3 desc;

drop table if exists market_price;
create temp table market_price as
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
from output.transactions ) t ) t  where date_index >=20;

drop table if exists volume_rank;
create temp table volume_rank as 
select code, date_index, market_volume,
row_number() over(partition by date_index order by market_volume desc) as rank
from output.transactions  t
order by 2,3  desc ;

drop table if exists temp_sub ;
create temp table temp_sub as 
select code, date_index, average_volume,
market_volume * (market_price - market_previous_close)/market_previous_close as strength
from output.transactions where market_previous_close <> 0 and  market_previous_close is not null;
create index idx on temp_sub (code);

drop table if exists temp_strength;
create temp table temp_strength as 
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
) as down from output.transactions ) temp where date_index >10;

drop table if exists temp_kind;
create temp table temp_kind as 
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

drop table if exists short_strength;
create temp table short_strength as select code, load_date, date_index,
cast((1/(1+EXP(0-r*0.1))-0.5)*2 as decimal(5,2)) as short_strength from (
select cast(short_position*1.00/average_volume as decimal(5,2)) as r, 
reported_position, * from output.transactions ) t ;
	
	
	
drop table if exists ask_bid;
create temp table ask_bid as select code, load_date, date_index, 
case when split_ratio < 0 then 0.00 else split_ratio end as split_ratio,
ask_strength from (select 
 case 	when market_price <>0 then cast((ask-bid)/market_price*1.00 as decimal(10,5)) 
 		when ask-bid <> 0 then cast((ask-bid)*1.00/(ask+bid)/2 as decimal(10,5))
		else 0 end split_ratio , 
 case 	when ask < market_price or ask < bid then 0.00 
  		when bid-ask <> 0 then cast((ask-market_price)*1.00/(ask-bid)*1.00 as decimal(10,5)) 
		else null end ask_strength,
 * from output.transactions ) t ;
	
drop table if exists price_ratio;
create temp table price_ratio as select code, load_date, date_index, 
case when price_ratio > 1 then 1 when price_ratio < 0 then 0 else price_ratio end as price_ratio,
row_number() over(partition by date_index order by price_ratio desc) as rank
from (select code, load_date, date_index,
case when market_day_high<> market_day_low then 
(market_price- market_day_low)/(market_day_high-market_day_low) 
else 0 end as price_ratio from output.transactions ) t ;

drop  table if exists training;
create temp table training as 
select p.code, p.date_index as mapping_index, (p.end_price-p.start_price)/p.start_price as performance
, s.pe_rank, f.avg_rank as avg_pe_rank,
strength05 as strength_ratio_05, strength10 as strength_ratio_10, strength20 as strength_ratio_20, 
rstrength05 as strength_rank_05, rstrength10 as strength_rank_10, rstrength20 as strength_rank_20,
i.increase_ratio, i.increase_rank, ifi.increase_filter
from performance p
left join 
(select code, date_index, cast(cast(rank as decimal(10,3))/300 as decimal(5,3)) as pe_rank  
 from pe_rank where code is not null and date_index is not null) as s
on p.code = s.code and p.date_index-1 = s. date_index
left join 
(select code, date_index, cast((rank*1.000 
+ lag(rank*1.000, 01) over (partition by code order by date_index) 
+ lag(rank*1.000, 02) over (partition by code order by date_index) 
+ lag(rank*1.000, 03) over (partition by code order by date_index) 
+ lag(rank*1.000, 04) over (partition by code order by date_index) 
+ lag(rank*1.000, 05) over (partition by code order by date_index))/1500 as decimal(5,3)) as avg_rank
from pe_rank ) as  f
on p.code = f.code and p.date_index-1 = f.date_index
left join 
(select code, date_index,
cast(1/(1+EXP(0-back05)) as decimal(5,3)) as strength05, 
cast(1/(1+EXP(0-back10)) as decimal(5,3)) as strength10, 
cast(1/(1+EXP(0-back20)) as decimal(5,3)) as strength20, 
cast((300.0- b05)/300 as decimal(5,3)) as rstrength05, 
cast((300.0- b10)/300 as decimal(5,3)) as rstrength10, 
cast((300.0- b20)/300 as decimal(5,3)) as rstrength20 from
(select code, date_index, back05, back10, back20,
row_number() over(partition by date_index order by back05 desc) b05,
row_number() over(partition by date_index order by back10 desc) b10,
row_number() over(partition by date_index order by back20 desc) b20
from temp_strength) t )t
on p.code = t.code and p.date_index-1 = t.date_index
left join 
(select code, date_index,
cast(1/(1+EXP(0-increase_ratio)) as decimal(5,3)) as increase_ratio,
cast(cast(rank as decimal(10,3))/300 as decimal(5,3)) as increase_rank
from increase_ratio ) i
on p.code = i.code and p.date_index-1 = i.date_index
left join 
(select date_index, 
cast(cast(max(rank) as decimal(10,3))/300 as decimal(5,3)) as increase_filter
from increase_ratio where increase_ratio > 0 group by 1 ) ifi
on p.date_index-1 = ifi.date_index
where p.start_price <> 0 and p.start_price  is not null;

END

$BODY$;

ALTER FUNCTION public.org_cleanup()
    OWNER TO postgres;
