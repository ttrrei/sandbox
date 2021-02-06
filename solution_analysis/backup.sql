--
-- PostgreSQL database dump
--

-- Dumped from database version 11.8
-- Dumped by pg_dump version 11.2

-- Started on 2021-02-06 13:12:42

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


--
-- TOC entry 257 (class 1255 OID 19949)
-- Name: _final_median(numeric[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public._final_median(numeric[]) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    AS $_$
   SELECT AVG(val)
   FROM (
     SELECT val
     FROM unnest($1) val
     ORDER BY 1
     LIMIT  2 - MOD(array_upper($1, 1), 2)
     OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
   ) sub;
$_$;


--
-- TOC entry 255 (class 1255 OID 16476)
-- Name: code_list_generator(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.code_list_generator() RETURNS void
    LANGUAGE plpgsql
    AS $$

BEGIN
drop table if exists temp_code_list;                          
create temp table temp_code_list as select * from code_list where  
record_dts::date=(select max(record_dts::date) from code_list);

drop table if exists code_list_alpha;
create table code_list_alpha as select * from temp_code_list;

drop table if exists code_list_finance;
create table code_list_finance as select * from temp_code_list;

drop table if exists code_list_consensus;
create table code_list_consensus as select * from temp_code_list;

drop table if exists temp_code_list;
END; 
$$;


--
-- TOC entry 256 (class 1255 OID 16477)
-- Name: tick_cleaner(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.tick_cleaner() RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN
drop table if exists tick_current;
create temp table tick_current as select distinct code, (record_time ::timestamp  at time zone 'America/New_York') at time zone 'AEST' as record_time, open, high, low, close, volume from tick_snapshot where sys_load_time::date = current_date;
drop table if exists tick_clean;
create temp table tick_clean as select c.* from tick_current c left outer join tick_merge m on c.code = m.code and c.record_time = m.record_time where m.code is null;
insert into tick_merge select * from tick_clean;
END; $$;


--
-- TOC entry 272 (class 1255 OID 22520)
-- Name: view_consensus_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.view_consensus_update() RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN
insert into view_consensus
 WITH temp_consensus AS (
         SELECT concat(btrim(web_consensus.code::text), '.AX') AS code,
            web_consensus.buy::numeric(5,3) AS sb,
            web_consensus.overweight::numeric(5,3) AS nb,
            web_consensus.hold::numeric(5,3) AS hd,
            web_consensus.underweight::numeric(5,3) AS ns,
            web_consensus.sell::numeric(5,3) AS ss,
            web_consensus.sys_load_time::date AS load_date,
            row_number() OVER (PARTITION BY web_consensus.code, (web_consensus.sys_load_time::date) ORDER BY (web_consensus.sys_load_time::time without time zone) DESC) AS rnum
           FROM web_consensus
          WHERE web_consensus.buy IS NOT NULL AND web_consensus.buy::text <> ''::text AND web_consensus.overweight IS NOT NULL AND web_consensus.overweight::text <> ''::text AND web_consensus.hold IS NOT NULL AND web_consensus.hold::text <> ''::text AND web_consensus.underweight IS NOT NULL AND web_consensus.underweight::text <> ''::text AND web_consensus.sell IS NOT NULL AND web_consensus.sell::text <> ''::text
        ), sub_consensus AS (
         SELECT temp_consensus.code,
            temp_consensus.load_date,
            (temp_consensus.sb + temp_consensus.nb + temp_consensus.hd + temp_consensus.ns + temp_consensus.ss)::integer AS analysis,
            ((temp_consensus.sb * 1::numeric + temp_consensus.nb * 2::numeric + temp_consensus.hd * 3::numeric + temp_consensus.ns * 4::numeric + temp_consensus.ss * 5::numeric) / (temp_consensus.sb + temp_consensus.nb + temp_consensus.hd + temp_consensus.ns + temp_consensus.ss))::numeric(5,3) AS weight
           FROM temp_consensus
          WHERE (temp_consensus.sb + temp_consensus.nb + temp_consensus.hd + temp_consensus.ns + temp_consensus.ss) > 3::numeric AND temp_consensus.rnum = 1
        )
 SELECT sub_consensus.code::character varying AS code,
    sub_consensus.load_date,
    sub_consensus.analysis,
    sub_consensus.weight
   FROM sub_consensus
   where sub_consensus.load_date = ( select max(load_date) from sub_consensus)
ON CONFLICT (code, load_date) do nothing;

END; $$;


--
-- TOC entry 273 (class 1255 OID 22489)
-- Name: view_finance_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.view_finance_update() RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN
insert into view_finance  
WITH temp_finance AS (
         SELECT row_number() OVER (PARTITION BY web_finance.code, (web_finance.sys_load_time::date) ORDER BY (web_finance.sys_load_time::time without time zone) DESC) AS rnum,
            web_finance.code,
            web_finance.sys_load_time::date AS load_date,
            COALESCE(
                CASE
                    WHEN web_finance.market_open::text = ''::text THEN NULL::character varying
                    ELSE web_finance.market_open
                END,
                CASE
                    WHEN web_finance.regular_market_open::text = ''::text THEN NULL::character varying
                    ELSE web_finance.regular_market_open
                END)::numeric(6,3) AS open,
            COALESCE(
                CASE
                    WHEN web_finance.market_previous_close::text = ''::text THEN NULL::character varying
                    ELSE web_finance.market_previous_close
                END,
                CASE
                    WHEN web_finance.regular_market_previous_close::text = ''::text THEN NULL::character varying
                    ELSE web_finance.regular_market_previous_close
                END)::numeric(6,3) AS previous_close,
            COALESCE(
                CASE
                    WHEN web_finance.market_price::text = ''::text THEN NULL::character varying
                    ELSE web_finance.market_price
                END,
                CASE
                    WHEN web_finance.current_price::text = ''::text THEN NULL::character varying
                    ELSE web_finance.current_price
                END)::numeric(6,3) AS close,
            COALESCE(
                CASE
                    WHEN web_finance.market_volume::text = ''::text THEN NULL::character varying
                    ELSE web_finance.market_volume
                END,
                CASE
                    WHEN web_finance.regualr_market_volume::text = ''::text THEN NULL::character varying
                    ELSE web_finance.regualr_market_volume
                END)::bigint AS volume,
                CASE
                    WHEN web_finance.target_low_price::text = ''::text THEN NULL::character varying
                    ELSE web_finance.target_low_price
                END::numeric(6,3) AS tlow,
                CASE
                    WHEN web_finance.target_high_price::text = ''::text THEN NULL::character varying
                    ELSE web_finance.target_high_price
                END::numeric(6,3) AS thigh,
                CASE
                    WHEN web_finance.target_median_price::text = ''::text THEN NULL::character varying
                    ELSE web_finance.target_median_price
                END::numeric(6,3) AS tmedian,
                CASE
                    WHEN web_finance.forward_pe::text = ''::text OR web_finance.forward_pe::text = 'Infinity'::text THEN NULL::character varying
                    ELSE web_finance.forward_pe
                END::numeric(6,1) AS forward_pe
           FROM web_finance
        ), temp AS (
         SELECT temp_finance.code,
            temp_finance.load_date,
            temp_finance.open,
            temp_finance.previous_close,
            temp_finance.close,
            temp_finance.volume,
            COALESCE(temp_finance.forward_pe, lag(temp_finance.forward_pe, 1, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 2, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 3, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 4, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date)) AS forward_pe,
                CASE
                    WHEN temp_finance.thigh IS NOT NULL AND temp_finance.tlow IS NOT NULL AND temp_finance.thigh <> temp_finance.tlow THEN (temp_finance.tmedian - temp_finance.tlow) / (temp_finance.thigh - temp_finance.tlow)
                    ELSE 0::numeric
                END::numeric(5,3) AS target_ratio
           FROM temp_finance
          WHERE temp_finance.rnum = 1
        ), sub_finance AS (
         SELECT temp.code,
            temp.load_date,
            temp.open,
            temp.previous_close,
            temp.close,
            temp.volume,
            temp.forward_pe,
            temp.target_ratio
			-- ,rank() OVER (PARTITION BY temp.load_date ORDER BY temp.forward_pe DESC) AS pe_rank
           FROM temp
          WHERE temp.forward_pe IS NOT NULL -- AND temp.target_ratio > 0.5
        )
 SELECT sub_finance.code,
    sub_finance.load_date,
    sub_finance.open,
    sub_finance.previous_close,
    sub_finance.close,
    sub_finance.volume,
    sub_finance.forward_pe,
    sub_finance.target_ratio
	-- ,sub_finance.pe_rank
   FROM sub_finance
   where sub_finance.load_date = ( select max(load_date) from sub_finance)
ON CONFLICT (code, load_date) do nothing;
END; $$;


--
-- TOC entry 271 (class 1255 OID 22511)
-- Name: view_short_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.view_short_update() RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN
insert into view_short
 WITH sub_short AS (
         SELECT concat(btrim(daily_short.product_code::text), '.AX') AS code,
            daily_short.sys_load::date AS load_date,
                CASE
                    WHEN daily_short.short_position::text = ''::text THEN NULL::bigint
                    ELSE daily_short.short_position::bigint
                END AS short_position,
                CASE
                    WHEN daily_short.total_in_issue::text = ''::text THEN NULL::bigint
                    ELSE daily_short.total_in_issue::bigint
                END AS total_in_issue,
                CASE
                    WHEN daily_short.reported_position::text = ''::text THEN NULL::numeric
                    ELSE daily_short.reported_position::numeric(31,8)
                END AS reported_position,
            row_number() OVER (PARTITION BY daily_short.product_code, (daily_short.sys_load::date) ORDER BY (daily_short.sys_load::time without time zone) DESC) AS rnum
           FROM daily_short
        )
 SELECT sub_short.code::character varying AS code,
    sub_short.load_date,
    sub_short.short_position,
    sub_short.total_in_issue,
    sub_short.reported_position
   FROM sub_short
  WHERE sub_short.rnum = 1
   and sub_short.load_date = ( select max(load_date) from sub_short)
ON CONFLICT (code, load_date) do nothing;





END; $$;


--
-- TOC entry 258 (class 1255 OID 22756)
-- Name: views_cleanup(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.views_cleanup() RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN

delete from view_consensus
where load_date in (
select load_date from idx
where rnum >30 );

delete from view_finance
where load_date in (
select load_date from idx
where rnum >30 );

delete from view_short
where load_date in (
select load_date from idx
where rnum >30 );

END; $$;


--
-- TOC entry 819 (class 1255 OID 19950)
-- Name: median(numeric); Type: AGGREGATE; Schema: public; Owner: -
--

CREATE AGGREGATE public.median(numeric) (
    SFUNC = array_append,
    STYPE = numeric[],
    INITCOND = '{}',
    FINALFUNC = public._final_median
);


SET default_with_oids = false;

--
-- TOC entry 204 (class 1259 OID 16579)
-- Name: code_list; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.code_list (
    code character varying,
    record_dts timestamp with time zone DEFAULT now()
);


--
-- TOC entry 252 (class 1259 OID 23355)
-- Name: code_list_alpha; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.code_list_alpha (
    code character varying,
    record_dts timestamp with time zone
);


--
-- TOC entry 254 (class 1259 OID 23367)
-- Name: code_list_consensus; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.code_list_consensus (
    code character varying,
    record_dts timestamp with time zone
);


--
-- TOC entry 253 (class 1259 OID 23361)
-- Name: code_list_finance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.code_list_finance (
    code character varying,
    record_dts timestamp with time zone
);


--
-- TOC entry 230 (class 1259 OID 22678)
-- Name: view_consensus; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.view_consensus (
    code character varying,
    load_date date,
    analysis integer,
    weight numeric(5,3)
);


--
-- TOC entry 231 (class 1259 OID 22684)
-- Name: view_finance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.view_finance (
    code character varying,
    load_date date,
    open numeric(6,3),
    previous_close numeric(6,3),
    close numeric(6,3),
    volume bigint,
    forward_pe numeric,
    target_ratio numeric(5,3)
);


--
-- TOC entry 232 (class 1259 OID 22690)
-- Name: view_short; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.view_short (
    code character varying,
    load_date date,
    short_position bigint,
    total_in_issue bigint,
    reported_position numeric
);


--
-- TOC entry 233 (class 1259 OID 22705)
-- Name: idx; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.idx AS
 WITH date_list AS (
         SELECT view_consensus.load_date
           FROM public.view_consensus
          GROUP BY view_consensus.load_date
        UNION
         SELECT view_finance.load_date
           FROM public.view_finance
          GROUP BY view_finance.load_date
        UNION
         SELECT view_short.load_date
           FROM public.view_short
          GROUP BY view_short.load_date
        ), idx AS (
         SELECT date_list.load_date,
            (date_part('dow'::text, date_list.load_date))::integer AS dow,
            row_number() OVER (ORDER BY date_list.load_date DESC) AS rnum
           FROM date_list
          GROUP BY date_list.load_date
        )
 SELECT idx.load_date,
    idx.dow,
    idx.rnum
   FROM idx;


--
-- TOC entry 234 (class 1259 OID 22710)
-- Name: current_list; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.current_list AS
 WITH temp_finance AS (
         SELECT view_finance.code,
            view_finance.load_date,
            view_finance.open,
            view_finance.previous_close,
            view_finance.close,
            view_finance.volume,
            view_finance.forward_pe,
            view_finance.target_ratio,
            rank() OVER (PARTITION BY view_finance.load_date ORDER BY view_finance.forward_pe DESC) AS pe_rank
           FROM public.view_finance
          WHERE (view_finance.target_ratio > 0.5)
        ), output AS (
         SELECT i.load_date,
            i.dow,
            i.rnum,
            c.analysis,
            c.weight,
            s.short_position,
            f.code,
            f.open,
            f.close,
            f.volume,
            f.target_ratio,
            f.pe_rank,
            (((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0)))::numeric(5,3) AS total_weight,
            rank() OVER (PARTITION BY i.load_date ORDER BY ((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0))) AS total_rank
           FROM (((public.idx i
             JOIN temp_finance f ON ((i.load_date = f.load_date)))
             JOIN public.view_short s ON (((i.load_date = s.load_date) AND ((f.code)::text = (s.code)::text))))
             JOIN public.view_consensus c ON (((i.load_date = c.load_date) AND ((f.code)::text = (c.code)::text))))
          WHERE ((i.rnum <= 20) AND ((s.code)::text = (c.code)::text))
        )
 SELECT output.load_date,
    output.dow,
    output.rnum,
    output.analysis,
    output.weight,
    output.short_position,
    output.code,
    output.open,
    output.close,
    output.volume,
    output.target_ratio,
    output.pe_rank,
    output.total_weight,
    output.total_rank
   FROM output
  ORDER BY output.load_date DESC, output.total_weight;


--
-- TOC entry 239 (class 1259 OID 22884)
-- Name: current_list_full; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.current_list_full AS
 WITH temp_finance AS (
         SELECT view_finance.code,
            view_finance.load_date,
            view_finance.open,
            view_finance.previous_close,
            view_finance.close,
            view_finance.volume,
            view_finance.forward_pe,
            view_finance.target_ratio,
            rank() OVER (PARTITION BY view_finance.load_date ORDER BY view_finance.forward_pe DESC) AS pe_rank
           FROM public.view_finance
          WHERE (view_finance.target_ratio > 0.5)
        ), output AS (
         SELECT i.load_date,
            i.dow,
            i.rnum,
            c.analysis,
            c.weight,
            s.short_position,
            f.code,
            f.open,
            f.close,
            f.volume,
            f.target_ratio,
            f.pe_rank,
            (((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0)))::numeric(5,3) AS total_weight,
            rank() OVER (PARTITION BY i.load_date ORDER BY ((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0))) AS total_rank
           FROM (((public.idx i
             JOIN temp_finance f ON ((i.load_date = f.load_date)))
             JOIN public.view_short s ON (((i.load_date = s.load_date) AND ((f.code)::text = (s.code)::text))))
             JOIN public.view_consensus c ON (((i.load_date = c.load_date) AND ((f.code)::text = (c.code)::text))))
          WHERE ((s.code)::text = (c.code)::text)
        )
 SELECT output.load_date,
    output.dow,
    output.rnum,
    output.analysis,
    output.weight,
    output.short_position,
    output.code,
    output.open,
    output.close,
    output.volume,
    output.target_ratio,
    output.pe_rank,
    output.total_weight,
    output.total_rank
   FROM output
  ORDER BY output.load_date DESC, output.total_weight;


--
-- TOC entry 205 (class 1259 OID 16604)
-- Name: daily_short; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_short (
    company_full_name character varying,
    product_code character varying,
    short_position character varying,
    total_in_issue character varying,
    reported_position character varying,
    sys_load timestamp with time zone DEFAULT now()
);


--
-- TOC entry 208 (class 1259 OID 16634)
-- Name: web_finance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.web_finance (
    code character varying,
    market_open character varying,
    market_day_high character varying,
    market_previous_close character varying,
    market_day_low character varying,
    market_price character varying,
    market_volume character varying,
    current_price character varying,
    target_low_price character varying,
    target_median_price character varying,
    number_of_analyst character varying,
    target_mean_price character varying,
    target_high_price character varying,
    recommendation_mean character varying,
    regular_market_open character varying,
    regular_market_day_high character varying,
    regular_market_previous_close character varying,
    regular_market_day_low character varying,
    tailing_pe character varying,
    regualr_market_volume character varying,
    average_volume character varying,
    ask character varying,
    ask_size character varying,
    forward_pe character varying,
    bid character varying,
    bid_size character varying,
    enterprise_revenue character varying,
    profit_margins character varying,
    enterprise_ebitda character varying,
    book_value character varying,
    tailing_eps character varying,
    price_to_book character varying,
    beta character varying,
    quarterly_earning_growth character varying,
    sys_load_time timestamp with time zone DEFAULT now()
);


--
-- TOC entry 212 (class 1259 OID 16779)
-- Name: v_idx_date; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_idx_date AS
 SELECT temp.load_date,
    row_number() OVER (ORDER BY temp.load_date) AS date_index
   FROM ( SELECT DISTINCT (web_finance.sys_load_time)::date AS load_date
           FROM public.web_finance) temp;


--
-- TOC entry 210 (class 1259 OID 16769)
-- Name: v_instance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_instance AS
 SELECT temp.code,
    temp.load_date,
    temp.market_price,
    temp.average_volume,
    temp.pe_rank,
    temp.target_ratio,
    temp.recommend,
    row_number() OVER (PARTITION BY temp.load_date ORDER BY (((1.0)::double precision + temp.pe_rank) + ((temp.recommend * 0.5))::double precision) DESC) AS ranking
   FROM ( SELECT temp_1.code,
            temp_1.load_date,
            temp_1.market_price,
            temp_1.average_volume,
                CASE
                    WHEN (((1)::double precision - ((temp_1.perank)::double precision / (100)::double precision)) > (0)::double precision) THEN ((1)::double precision - ((temp_1.perank)::double precision / (100)::double precision))
                    ELSE (0)::double precision
                END AS pe_rank,
                CASE
                    WHEN ((temp_1.target_high_price IS NOT NULL) AND (temp_1.target_low_price IS NOT NULL) AND (temp_1.target_high_price <> temp_1.target_low_price)) THEN ((temp_1.target_median_price - temp_1.target_low_price) / (temp_1.target_high_price - temp_1.target_low_price))
                    ELSE (0)::numeric
                END AS target_ratio,
                CASE
                    WHEN ((temp_1.number_of_analyst IS NOT NULL) AND (temp_1.number_of_analyst >= (3)::numeric)) THEN (((5)::numeric - temp_1.recommendation_mean) / (5)::numeric)
                    ELSE (0)::numeric
                END AS recommend
           FROM ( SELECT web_finance.code,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_open)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_open
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_open)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_open
                        END))::numeric AS market_open,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_previous_close)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_previous_close
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_previous_close)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_previous_close
                        END))::numeric AS market_previous_close,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_day_high)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_day_high
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_day_high)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_day_high
                        END))::numeric AS market_day_high,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_day_low)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_day_low
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_day_low)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_day_low
                        END))::numeric AS market_day_low,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_price
                        END,
                        CASE
                            WHEN ((web_finance.current_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.current_price
                        END))::numeric AS market_price,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_volume
                        END,
                        CASE
                            WHEN ((web_finance.regualr_market_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regualr_market_volume
                        END))::numeric AS market_volume,
                    (
                        CASE
                            WHEN ((web_finance.target_low_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_low_price
                        END)::numeric AS target_low_price,
                    (
                        CASE
                            WHEN ((web_finance.target_high_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_high_price
                        END)::numeric AS target_high_price,
                    (
                        CASE
                            WHEN ((web_finance.target_median_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_median_price
                        END)::numeric AS target_median_price,
                    (
                        CASE
                            WHEN ((web_finance.target_mean_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_mean_price
                        END)::numeric AS target_mean_price,
                    (
                        CASE
                            WHEN ((web_finance.number_of_analyst)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.number_of_analyst
                        END)::numeric AS number_of_analyst,
                    (
                        CASE
                            WHEN ((web_finance.recommendation_mean)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.recommendation_mean
                        END)::numeric AS recommendation_mean,
                    (
                        CASE
                            WHEN ((web_finance.average_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.average_volume
                        END)::numeric AS average_volume,
                    (
                        CASE
                            WHEN ((web_finance.ask)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.ask
                        END)::numeric AS ask,
                    (
                        CASE
                            WHEN ((web_finance.ask_size)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.ask_size
                        END)::numeric AS ask_size,
                    (
                        CASE
                            WHEN ((web_finance.bid)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.bid
                        END)::numeric AS bid,
                    (
                        CASE
                            WHEN ((web_finance.bid_size)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.bid_size
                        END)::numeric AS bid_size,
                    (
                        CASE
                            WHEN ((web_finance.profit_margins)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.profit_margins
                        END)::numeric AS profit_margins,
                    (web_finance.sys_load_time)::date AS load_date,
                    row_number() OVER (PARTITION BY ((web_finance.sys_load_time)::date) ORDER BY (web_finance.forward_pe)::numeric DESC) AS perank
                   FROM public.web_finance
                  WHERE (((web_finance.forward_pe)::text <> ''::text) AND ((web_finance.forward_pe)::text <> 'Infinity'::text) AND ((web_finance.average_volume)::text <> ''::text) AND ((web_finance.average_volume)::numeric > (50000)::numeric))) temp_1) temp
  WHERE (temp.target_ratio > 0.5);


--
-- TOC entry 219 (class 1259 OID 16811)
-- Name: list0; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.list0 AS
 SELECT i.code,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE (d.date_index = ( SELECT (max(v_idx_date.date_index) - 0)
           FROM public.v_idx_date));


--
-- TOC entry 220 (class 1259 OID 16815)
-- Name: list1; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.list1 AS
 SELECT i.code,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE (d.date_index = ( SELECT (max(v_idx_date.date_index) - 1)
           FROM public.v_idx_date));


--
-- TOC entry 221 (class 1259 OID 16819)
-- Name: list2; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.list2 AS
 SELECT i.code,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE (d.date_index = ( SELECT (max(v_idx_date.date_index) - 2)
           FROM public.v_idx_date));


--
-- TOC entry 222 (class 1259 OID 16823)
-- Name: list3; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.list3 AS
 SELECT i.code,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE (d.date_index = ( SELECT (max(v_idx_date.date_index) - 3)
           FROM public.v_idx_date));


--
-- TOC entry 223 (class 1259 OID 16827)
-- Name: list4; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.list4 AS
 SELECT i.code,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE (d.date_index = ( SELECT (max(v_idx_date.date_index) - 4)
           FROM public.v_idx_date));


--
-- TOC entry 235 (class 1259 OID 22720)
-- Name: ranking_list; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.ranking_list AS
 WITH temp_pool AS (
         SELECT current_list.code AS code0,
            lead(current_list.code, 1, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code1,
            lead(current_list.code, 2, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code2,
            lead(current_list.code, 3, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code3,
            lead(current_list.code, 4, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code4,
            current_list.load_date,
            current_list.rnum,
            current_list.total_rank
           FROM public.current_list
        ), recon AS (
         SELECT p1.load_date,
            p1.rnum,
            json_array_elements_text(json_build_array(p1.code0, p1.code1, p1.code2, p1.code3, p1.code4, p2.code0, p2.code1, p2.code2, p2.code3, p2.code4, p3.code0, p3.code1, p3.code2, p3.code3, p3.code4, p4.code0, p4.code1, p4.code2, p4.code3, p4.code4, p5.code0, p5.code1, p5.code2, p5.code3, p5.code4)) AS code,
            json_array_elements_text(json_build_array(1, 0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7, 0.6, 0.5, 0.8, 0.7, 0.6, 0.5, 0.4, 0.7, 0.6, 0.5, 0.4, 0.3, 0.6, 0.5, 0.4, 0.3, 0.2)) AS weight
           FROM ((((temp_pool p1
             JOIN temp_pool p2 ON (((p1.rnum = p2.rnum) AND (p1.total_rank = 1) AND (p2.total_rank = 2))))
             JOIN temp_pool p3 ON (((p1.rnum = p3.rnum) AND (p1.total_rank = 1) AND (p3.total_rank = 3))))
             JOIN temp_pool p4 ON (((p1.rnum = p4.rnum) AND (p1.total_rank = 1) AND (p4.total_rank = 4))))
             JOIN temp_pool p5 ON (((p1.rnum = p5.rnum) AND (p1.total_rank = 1) AND (p5.total_rank = 5))))
        )
 SELECT recon.load_date,
    recon.code,
    recon.rnum,
    sum((recon.weight)::numeric) AS weight,
    row_number() OVER (PARTITION BY recon.load_date ORDER BY (sum((recon.weight)::numeric)) DESC) AS wnum
   FROM recon
  WHERE (recon.rnum <= 6)
  GROUP BY recon.load_date, recon.code, recon.rnum
  ORDER BY recon.load_date DESC, (sum((recon.weight)::numeric)) DESC;


--
-- TOC entry 229 (class 1259 OID 22651)
-- Name: sub_finance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.sub_finance AS
 WITH temp_finance AS (
         SELECT row_number() OVER (PARTITION BY web_finance.code, ((web_finance.sys_load_time)::date) ORDER BY ((web_finance.sys_load_time)::time without time zone) DESC) AS rnum,
            web_finance.code,
            (web_finance.sys_load_time)::date AS load_date,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_open)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_open
                END,
                CASE
                    WHEN ((web_finance.regular_market_open)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regular_market_open
                END))::numeric(6,3) AS open,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_previous_close)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_previous_close
                END,
                CASE
                    WHEN ((web_finance.regular_market_previous_close)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regular_market_previous_close
                END))::numeric(6,3) AS previous_close,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_price
                END,
                CASE
                    WHEN ((web_finance.current_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.current_price
                END))::numeric(6,3) AS close,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_volume)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_volume
                END,
                CASE
                    WHEN ((web_finance.regualr_market_volume)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regualr_market_volume
                END))::bigint AS volume,
            (
                CASE
                    WHEN ((web_finance.target_low_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_low_price
                END)::numeric(6,3) AS tlow,
            (
                CASE
                    WHEN ((web_finance.target_high_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_high_price
                END)::numeric(6,3) AS thigh,
            (
                CASE
                    WHEN ((web_finance.target_median_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_median_price
                END)::numeric(6,3) AS tmedian,
            (
                CASE
                    WHEN (((web_finance.forward_pe)::text = ''::text) OR ((web_finance.forward_pe)::text = 'Infinity'::text)) THEN NULL::character varying
                    ELSE web_finance.forward_pe
                END)::numeric(6,1) AS forward_pe
           FROM public.web_finance
        ), temp AS (
         SELECT temp_finance.code,
            temp_finance.load_date,
            temp_finance.open,
            temp_finance.previous_close,
            temp_finance.close,
            temp_finance.volume,
            COALESCE(temp_finance.forward_pe, lag(temp_finance.forward_pe, 1, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 2, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 3, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 4, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date)) AS forward_pe,
            (
                CASE
                    WHEN ((temp_finance.thigh IS NOT NULL) AND (temp_finance.tlow IS NOT NULL) AND (temp_finance.thigh <> temp_finance.tlow)) THEN ((temp_finance.tmedian - temp_finance.tlow) / (temp_finance.thigh - temp_finance.tlow))
                    ELSE (0)::numeric
                END)::numeric(5,3) AS target_ratio
           FROM temp_finance
          WHERE (temp_finance.rnum = 1)
        ), sub_finance AS (
         SELECT temp.code,
            temp.load_date,
            temp.open,
            temp.previous_close,
            temp.close,
            temp.volume,
            temp.forward_pe,
            temp.target_ratio
           FROM temp
          WHERE (temp.forward_pe IS NOT NULL)
        )
 SELECT sub_finance.code,
    sub_finance.load_date,
    sub_finance.open,
    sub_finance.previous_close,
    sub_finance.close,
    sub_finance.volume,
    sub_finance.forward_pe,
    sub_finance.target_ratio
   FROM sub_finance;


--
-- TOC entry 238 (class 1259 OID 22751)
-- Name: output_performance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.output_performance AS
 WITH temp_list AS (
         SELECT f.code,
            f.open,
            f.close,
            f.load_date,
            r.wnum
           FROM (public.sub_finance f
             JOIN public.ranking_list r ON (((f.code)::text = r.code)))
          WHERE ((r.rnum = 1) AND (r.wnum <= 5))
        ), rlist AS (
         SELECT temp_list.code,
            temp_list.open,
            temp_list.close,
            temp_list.load_date,
            temp_list.wnum,
            ((11 - row_number() OVER (PARTITION BY temp_list.code ORDER BY temp_list.load_date DESC)))::integer AS offset_count
           FROM temp_list
        ), performance AS (
         SELECT rlist.code,
            rlist.close,
            rlist.load_date,
            rlist.wnum,
            lag(rlist.load_date, rlist.offset_count, NULL::date) OVER (PARTITION BY rlist.code ORDER BY rlist.load_date) AS offset_date,
            lag(rlist.close, rlist.offset_count, NULL::numeric) OVER (PARTITION BY rlist.code ORDER BY rlist.load_date) AS offset_close
           FROM rlist
          WHERE (rlist.offset_count >= 0)
        )
 SELECT p1.load_date,
    ((p1.close / p1.offset_close))::numeric(8,3) AS ratio1,
    ((p2.close / p2.offset_close))::numeric(8,3) AS ratio2,
    ((p3.close / p3.offset_close))::numeric(8,3) AS ratio3,
    ((p4.close / p4.offset_close))::numeric(8,3) AS ratio4,
    ((p5.close / p5.offset_close))::numeric(8,3) AS ratio5
   FROM ((((performance p1
     JOIN performance p2 ON ((p1.load_date = p2.load_date)))
     JOIN performance p3 ON ((p1.load_date = p3.load_date)))
     JOIN performance p4 ON ((p1.load_date = p4.load_date)))
     JOIN performance p5 ON ((p1.load_date = p5.load_date)))
  WHERE ((p1.wnum = 1) AND (p2.wnum = 2) AND (p3.wnum = 3) AND (p4.wnum = 4) AND (p5.wnum = 5))
  ORDER BY p1.load_date;


--
-- TOC entry 236 (class 1259 OID 22730)
-- Name: output_ranking; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.output_ranking AS
 WITH temp AS (
         SELECT ranking_list.weight,
            ranking_list.code AS code0,
            lead(ranking_list.code, 1, NULL::text) OVER (PARTITION BY ranking_list.wnum ORDER BY ranking_list.load_date DESC) AS code1,
            lead(ranking_list.code, 2, NULL::text) OVER (PARTITION BY ranking_list.wnum ORDER BY ranking_list.load_date DESC) AS code2,
            lead(ranking_list.code, 3, NULL::text) OVER (PARTITION BY ranking_list.wnum ORDER BY ranking_list.load_date DESC) AS code3,
            lead(ranking_list.code, 4, NULL::text) OVER (PARTITION BY ranking_list.wnum ORDER BY ranking_list.load_date DESC) AS code4,
            ranking_list.rnum
           FROM public.ranking_list
          WHERE (ranking_list.wnum <= 5)
          ORDER BY ranking_list.load_date DESC, ranking_list.wnum
        )
 SELECT row_number() OVER (ORDER BY temp.weight DESC) AS rank,
    temp.code0,
    temp.code1,
    temp.code2,
    temp.code3,
    temp.code4
   FROM temp
  WHERE (temp.rnum = 1);


--
-- TOC entry 228 (class 1259 OID 22415)
-- Name: sub_short; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.sub_short AS
 WITH sub_short AS (
         SELECT concat(btrim((daily_short.product_code)::text), '.AX') AS code,
            (daily_short.sys_load)::date AS load_date,
                CASE
                    WHEN ((daily_short.short_position)::text = ''::text) THEN NULL::bigint
                    ELSE (daily_short.short_position)::bigint
                END AS short_position,
                CASE
                    WHEN ((daily_short.total_in_issue)::text = ''::text) THEN NULL::bigint
                    ELSE (daily_short.total_in_issue)::bigint
                END AS total_in_issue,
                CASE
                    WHEN ((daily_short.reported_position)::text = ''::text) THEN NULL::numeric
                    ELSE (daily_short.reported_position)::numeric(31,8)
                END AS reported_position,
            row_number() OVER (PARTITION BY daily_short.product_code, ((daily_short.sys_load)::date) ORDER BY ((daily_short.sys_load)::time without time zone) DESC) AS rnum
           FROM public.daily_short
        )
 SELECT (sub_short.code)::character varying AS code,
    sub_short.load_date,
    sub_short.short_position,
    sub_short.total_in_issue,
    sub_short.reported_position
   FROM sub_short
  WHERE (sub_short.rnum = 1);


--
-- TOC entry 237 (class 1259 OID 22741)
-- Name: output_short; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.output_short AS
 WITH temp_finance AS (
         SELECT view_finance.code,
            view_finance.load_date,
            view_finance.open,
            view_finance.previous_close,
            view_finance.close,
            view_finance.volume,
            view_finance.forward_pe,
            view_finance.target_ratio,
            rank() OVER (PARTITION BY view_finance.load_date ORDER BY view_finance.forward_pe DESC) AS pe_rank
           FROM public.view_finance
          WHERE (1 = 1)
        ), short AS (
         SELECT i.load_date,
            sum(
                CASE
                    WHEN (f.pe_rank > 60) THEN (0)::numeric
                    ELSE (f.close * (s.short_position)::numeric)
                END) AS s60,
            sum(
                CASE
                    WHEN (f.pe_rank > 50) THEN (0)::numeric
                    ELSE (f.close * (s.short_position)::numeric)
                END) AS s50,
            sum(
                CASE
                    WHEN (f.pe_rank > 40) THEN (0)::numeric
                    ELSE (f.close * (s.short_position)::numeric)
                END) AS s40,
            sum(
                CASE
                    WHEN (f.pe_rank > 30) THEN (0)::numeric
                    ELSE (f.close * (s.short_position)::numeric)
                END) AS s30
           FROM ((public.idx i
             JOIN temp_finance f ON ((i.load_date = f.load_date)))
             JOIN public.sub_short s ON (((i.load_date = s.load_date) AND ((f.code)::text = (s.code)::text))))
          GROUP BY i.load_date
        ), output AS (
         SELECT short.load_date,
            row_number() OVER (ORDER BY short.load_date DESC) AS rnum,
            short.s50 AS org_s50,
            short.s40 AS org_s40,
            (((1)::numeric - (1.0 / ((1)::numeric + exp(((- (short.s60 - 6000000000.0)) / 1500000000.0))))))::numeric(6,4) AS s60,
            (((1)::numeric - (1.0 / ((1)::numeric + exp(((- (short.s50 - 5500000000.0)) / 1375000000.0))))))::numeric(6,4) AS s50,
            (((1)::numeric - (1.0 / ((1)::numeric + exp(((- (short.s40 - 4400000000.0)) / 1100000000.0))))))::numeric(6,4) AS s40,
            (((1)::numeric - (1.0 / ((1)::numeric + exp(((- (short.s30 - 4000000000.0)) / 1000000000.0))))))::numeric(6,4) AS s30,
                CASE
                    WHEN ((short.s50 > ('5500000000'::bigint)::numeric) AND (short.s40 > ('4400000000'::bigint)::numeric)) THEN 0.0
                    ELSE 1.0
                END AS flag
           FROM short
        )
 SELECT output.load_date,
    output.s30,
    output.s40,
    output.s50,
    output.s60,
    output.flag
   FROM output
  WHERE (output.rnum <= 10)
  ORDER BY output.rnum DESC;


--
-- TOC entry 240 (class 1259 OID 22889)
-- Name: ranking_list_full; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.ranking_list_full AS
 WITH temp_pool AS (
         SELECT current_list.code AS code0,
            lead(current_list.code, 1, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code1,
            lead(current_list.code, 2, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code2,
            lead(current_list.code, 3, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code3,
            lead(current_list.code, 4, NULL::character varying) OVER (PARTITION BY current_list.total_rank ORDER BY current_list.rnum) AS code4,
            current_list.load_date,
            current_list.rnum,
            current_list.total_rank
           FROM public.current_list_full current_list
        ), recon AS (
         SELECT p1.load_date,
            p1.rnum,
            json_array_elements_text(json_build_array(p1.code0, p1.code1, p1.code2, p1.code3, p1.code4, p2.code0, p2.code1, p2.code2, p2.code3, p2.code4, p3.code0, p3.code1, p3.code2, p3.code3, p3.code4, p4.code0, p4.code1, p4.code2, p4.code3, p4.code4, p5.code0, p5.code1, p5.code2, p5.code3, p5.code4)) AS code,
            json_array_elements_text(json_build_array(1, 0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7, 0.6, 0.5, 0.8, 0.7, 0.6, 0.5, 0.4, 0.7, 0.6, 0.5, 0.4, 0.3, 0.6, 0.5, 0.4, 0.3, 0.2)) AS weight
           FROM ((((temp_pool p1
             JOIN temp_pool p2 ON (((p1.rnum = p2.rnum) AND (p1.total_rank = 1) AND (p2.total_rank = 2))))
             JOIN temp_pool p3 ON (((p1.rnum = p3.rnum) AND (p1.total_rank = 1) AND (p3.total_rank = 3))))
             JOIN temp_pool p4 ON (((p1.rnum = p4.rnum) AND (p1.total_rank = 1) AND (p4.total_rank = 4))))
             JOIN temp_pool p5 ON (((p1.rnum = p5.rnum) AND (p1.total_rank = 1) AND (p5.total_rank = 5))))
        )
 SELECT recon.load_date,
    recon.code,
    recon.rnum,
    sum((recon.weight)::numeric) AS weight,
    row_number() OVER (PARTITION BY recon.load_date ORDER BY (sum((recon.weight)::numeric)) DESC) AS wnum
   FROM recon
  GROUP BY recon.load_date, recon.code, recon.rnum
  ORDER BY recon.load_date DESC, (sum((recon.weight)::numeric)) DESC;


--
-- TOC entry 224 (class 1259 OID 16831)
-- Name: ranking_table; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.ranking_table AS
 SELECT l0.code,
    l0.ranking AS m0,
    l1.ranking AS m1,
    l2.ranking AS m2,
    l3.ranking AS m3,
    l4.ranking AS m4
   FROM ((((public.list0 l0
     LEFT JOIN public.list1 l1 ON (((l0.code)::text = (l1.code)::text)))
     LEFT JOIN public.list2 l2 ON (((l0.code)::text = (l2.code)::text)))
     LEFT JOIN public.list3 l3 ON (((l0.code)::text = (l3.code)::text)))
     LEFT JOIN public.list4 l4 ON (((l0.code)::text = (l4.code)::text)))
  WHERE (l0.ranking < 11);


--
-- TOC entry 209 (class 1259 OID 16689)
-- Name: web_consensus; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.web_consensus (
    code character varying,
    buy character varying,
    overweight character varying,
    hold character varying,
    underweight character varying,
    sell character varying,
    high character varying,
    median character varying,
    low character varying,
    avearge character varying,
    current character varying,
    sys_load_time timestamp with time zone DEFAULT now()
);


--
-- TOC entry 227 (class 1259 OID 22405)
-- Name: sub_consensus; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.sub_consensus AS
 WITH temp_consensus AS (
         SELECT concat(btrim((web_consensus.code)::text), '.AX') AS code,
            (web_consensus.buy)::numeric(5,3) AS sb,
            (web_consensus.overweight)::numeric(5,3) AS nb,
            (web_consensus.hold)::numeric(5,3) AS hd,
            (web_consensus.underweight)::numeric(5,3) AS ns,
            (web_consensus.sell)::numeric(5,3) AS ss,
            (web_consensus.sys_load_time)::date AS load_date,
            row_number() OVER (PARTITION BY web_consensus.code, ((web_consensus.sys_load_time)::date) ORDER BY ((web_consensus.sys_load_time)::time without time zone) DESC) AS rnum
           FROM public.web_consensus
          WHERE ((web_consensus.buy IS NOT NULL) AND ((web_consensus.buy)::text <> ''::text) AND (web_consensus.overweight IS NOT NULL) AND ((web_consensus.overweight)::text <> ''::text) AND (web_consensus.hold IS NOT NULL) AND ((web_consensus.hold)::text <> ''::text) AND (web_consensus.underweight IS NOT NULL) AND ((web_consensus.underweight)::text <> ''::text) AND (web_consensus.sell IS NOT NULL) AND ((web_consensus.sell)::text <> ''::text))
        ), sub_consensus AS (
         SELECT temp_consensus.code,
            temp_consensus.load_date,
            (((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss))::integer AS analysis,
            (((((((temp_consensus.sb * (1)::numeric) + (temp_consensus.nb * (2)::numeric)) + (temp_consensus.hd * (3)::numeric)) + (temp_consensus.ns * (4)::numeric)) + (temp_consensus.ss * (5)::numeric)) / ((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss)))::numeric(5,3) AS weight
           FROM temp_consensus
          WHERE ((((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss) > (3)::numeric) AND (temp_consensus.rnum = 1))
        )
 SELECT (sub_consensus.code)::character varying AS code,
    sub_consensus.load_date,
    sub_consensus.analysis,
    sub_consensus.weight
   FROM sub_consensus;


--
-- TOC entry 206 (class 1259 OID 16619)
-- Name: tick_merge; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tick_merge (
    code character varying,
    record_time timestamp with time zone,
    open character varying,
    high character varying,
    low character varying,
    close character varying,
    volume character varying
);


--
-- TOC entry 203 (class 1259 OID 16447)
-- Name: tick_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tick_snapshot (
    code character varying,
    record_time character varying,
    open character varying,
    high character varying,
    low character varying,
    close character varying,
    volume character varying,
    sys_load_time timestamp without time zone DEFAULT now()
);


--
-- TOC entry 225 (class 1259 OID 18756)
-- Name: v_current_list; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_current_list AS
 WITH date_list AS (
         SELECT (web_consensus.sys_load_time)::date AS load_date
           FROM public.web_consensus
          GROUP BY ((web_consensus.sys_load_time)::date)
        UNION
         SELECT (web_finance.sys_load_time)::date AS load_date
           FROM public.web_finance
          GROUP BY ((web_finance.sys_load_time)::date)
        UNION
         SELECT (daily_short.sys_load)::date AS load_date
           FROM public.daily_short
          GROUP BY ((daily_short.sys_load)::date)
        ), idx AS (
         SELECT date_list.load_date,
            (date_part('dow'::text, date_list.load_date))::integer AS dow,
            row_number() OVER (ORDER BY date_list.load_date DESC) AS rnum
           FROM date_list
          GROUP BY date_list.load_date
        ), temp_finance AS (
         SELECT row_number() OVER (PARTITION BY web_finance.code, ((web_finance.sys_load_time)::date) ORDER BY ((web_finance.sys_load_time)::time without time zone) DESC) AS rnum,
            web_finance.code,
            (web_finance.sys_load_time)::date AS load_date,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_open)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_open
                END,
                CASE
                    WHEN ((web_finance.regular_market_open)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regular_market_open
                END))::numeric(6,3) AS open,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_previous_close)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_previous_close
                END,
                CASE
                    WHEN ((web_finance.regular_market_previous_close)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regular_market_previous_close
                END))::numeric(6,3) AS previous_close,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_price
                END,
                CASE
                    WHEN ((web_finance.current_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.current_price
                END))::numeric(6,3) AS close,
            (COALESCE(
                CASE
                    WHEN ((web_finance.market_volume)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.market_volume
                END,
                CASE
                    WHEN ((web_finance.regualr_market_volume)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.regualr_market_volume
                END))::bigint AS volume,
            (
                CASE
                    WHEN ((web_finance.target_low_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_low_price
                END)::numeric(6,3) AS tlow,
            (
                CASE
                    WHEN ((web_finance.target_high_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_high_price
                END)::numeric(6,3) AS thigh,
            (
                CASE
                    WHEN ((web_finance.target_median_price)::text = ''::text) THEN NULL::character varying
                    ELSE web_finance.target_median_price
                END)::numeric(6,3) AS tmedian,
            (
                CASE
                    WHEN (((web_finance.forward_pe)::text = ''::text) OR ((web_finance.forward_pe)::text = 'Infinity'::text)) THEN NULL::character varying
                    ELSE web_finance.forward_pe
                END)::numeric(6,1) AS forward_pe
           FROM public.web_finance
        ), temp AS (
         SELECT temp_finance.code,
            temp_finance.load_date,
            temp_finance.open,
            temp_finance.previous_close,
            temp_finance.close,
            temp_finance.volume,
            COALESCE(temp_finance.forward_pe, lag(temp_finance.forward_pe, 1, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 2, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 3, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date), lag(temp_finance.forward_pe, 4, NULL::numeric) OVER (PARTITION BY temp_finance.code ORDER BY temp_finance.load_date)) AS forward_pe,
            (
                CASE
                    WHEN ((temp_finance.thigh IS NOT NULL) AND (temp_finance.tlow IS NOT NULL) AND (temp_finance.thigh <> temp_finance.tlow)) THEN ((temp_finance.tmedian - temp_finance.tlow) / (temp_finance.thigh - temp_finance.tlow))
                    ELSE (0)::numeric
                END)::numeric(5,3) AS target_ratio
           FROM temp_finance
          WHERE (temp_finance.rnum = 1)
        ), sub_finance AS (
         SELECT temp.code,
            temp.load_date,
            temp.open,
            temp.previous_close,
            temp.close,
            temp.volume,
            temp.forward_pe,
            temp.target_ratio,
            rank() OVER (PARTITION BY temp.load_date ORDER BY temp.forward_pe DESC) AS pe_rank
           FROM temp
          WHERE ((temp.forward_pe IS NOT NULL) AND (temp.target_ratio > 0.5))
        ), temp_consensus AS (
         SELECT concat(btrim((web_consensus.code)::text), '.AX') AS code,
            (web_consensus.buy)::numeric(5,3) AS sb,
            (web_consensus.overweight)::numeric(5,3) AS nb,
            (web_consensus.hold)::numeric(5,3) AS hd,
            (web_consensus.underweight)::numeric(5,3) AS ns,
            (web_consensus.sell)::numeric(5,3) AS ss,
            (web_consensus.sys_load_time)::date AS load_date,
            row_number() OVER (PARTITION BY web_consensus.code, ((web_consensus.sys_load_time)::date) ORDER BY ((web_consensus.sys_load_time)::time without time zone) DESC) AS rnum
           FROM public.web_consensus
          WHERE ((web_consensus.buy IS NOT NULL) AND ((web_consensus.buy)::text <> ''::text) AND (web_consensus.overweight IS NOT NULL) AND ((web_consensus.overweight)::text <> ''::text) AND (web_consensus.hold IS NOT NULL) AND ((web_consensus.hold)::text <> ''::text) AND (web_consensus.underweight IS NOT NULL) AND ((web_consensus.underweight)::text <> ''::text) AND (web_consensus.sell IS NOT NULL) AND ((web_consensus.sell)::text <> ''::text))
        ), sub_consensus AS (
         SELECT temp_consensus.code,
            temp_consensus.load_date,
            (((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss))::integer AS analysis,
            (((((((temp_consensus.sb * (1)::numeric) + (temp_consensus.nb * (2)::numeric)) + (temp_consensus.hd * (3)::numeric)) + (temp_consensus.ns * (4)::numeric)) + (temp_consensus.ss * (5)::numeric)) / ((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss)))::numeric(5,3) AS weight
           FROM temp_consensus
          WHERE ((((((temp_consensus.sb + temp_consensus.nb) + temp_consensus.hd) + temp_consensus.ns) + temp_consensus.ss) > (3)::numeric) AND (temp_consensus.rnum = 1))
        ), sub_short AS (
         SELECT concat(btrim((daily_short.product_code)::text), '.AX') AS code,
            (daily_short.sys_load)::date AS load_date,
                CASE
                    WHEN ((daily_short.short_position)::text = ''::text) THEN NULL::bigint
                    ELSE (daily_short.short_position)::bigint
                END AS short_position,
                CASE
                    WHEN ((daily_short.total_in_issue)::text = ''::text) THEN NULL::bigint
                    ELSE (daily_short.total_in_issue)::bigint
                END AS total_in_issue,
                CASE
                    WHEN ((daily_short.reported_position)::text = ''::text) THEN NULL::numeric
                    ELSE (daily_short.reported_position)::numeric(31,8)
                END AS reported_position,
            row_number() OVER (PARTITION BY daily_short.product_code, ((daily_short.sys_load)::date) ORDER BY ((daily_short.sys_load)::time without time zone) DESC) AS rnum
           FROM public.daily_short
        ), output AS (
         SELECT i.load_date,
            i.dow,
            i.rnum,
            c.analysis,
            c.weight,
            s.short_position,
            f.code,
            f.open,
            f.close,
            f.volume,
            f.target_ratio,
            f.pe_rank,
            (((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0)))::numeric(5,3) AS total_weight,
            rank() OVER (PARTITION BY i.load_date ORDER BY ((c.weight * 0.5) + ((f.pe_rank)::numeric / 100.0)), ((f.pe_rank)::numeric)) AS total_rank
           FROM (((idx i
             JOIN sub_finance f ON ((i.load_date = f.load_date)))
             JOIN sub_short s ON (((i.load_date = s.load_date) AND ((f.code)::text = s.code))))
             JOIN sub_consensus c ON (((i.load_date = c.load_date) AND ((f.code)::text = c.code))))
          WHERE ((s.rnum = 1) AND (i.rnum <= 20) AND (s.code = c.code))
        )
 SELECT output.load_date,
    output.dow,
    output.rnum,
    output.analysis,
    output.weight,
    output.short_position,
    output.code,
    output.open,
    output.close,
    output.volume,
    output.target_ratio,
    output.pe_rank,
    output.total_weight,
    output.total_rank
   FROM output
  ORDER BY output.load_date DESC, output.total_weight;


--
-- TOC entry 211 (class 1259 OID 16774)
-- Name: v_instance_full; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_instance_full AS
 SELECT temp.code,
    temp.load_date,
    temp.market_price,
    temp.average_volume,
    temp.pe_rank,
    temp.target_ratio,
    temp.recommend,
    row_number() OVER (PARTITION BY temp.load_date ORDER BY (((1.0)::double precision + temp.pe_rank) + ((temp.recommend * 0.5))::double precision) DESC) AS ranking
   FROM ( SELECT temp_1.code,
            temp_1.load_date,
            temp_1.market_price,
            temp_1.average_volume,
                CASE
                    WHEN (((1)::double precision - ((temp_1.perank)::double precision / (100)::double precision)) > (0)::double precision) THEN ((1)::double precision - ((temp_1.perank)::double precision / (100)::double precision))
                    ELSE (0)::double precision
                END AS pe_rank,
                CASE
                    WHEN ((temp_1.target_high_price IS NOT NULL) AND (temp_1.target_low_price IS NOT NULL) AND (temp_1.target_high_price <> temp_1.target_low_price)) THEN ((temp_1.target_median_price - temp_1.target_low_price) / (temp_1.target_high_price - temp_1.target_low_price))
                    ELSE (0)::numeric
                END AS target_ratio,
                CASE
                    WHEN ((temp_1.number_of_analyst IS NOT NULL) AND (temp_1.number_of_analyst >= (3)::numeric)) THEN (((5)::numeric - temp_1.recommendation_mean) / (5)::numeric)
                    ELSE (0)::numeric
                END AS recommend
           FROM ( SELECT web_finance.code,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_open)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_open
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_open)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_open
                        END))::numeric AS market_open,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_previous_close)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_previous_close
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_previous_close)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_previous_close
                        END))::numeric AS market_previous_close,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_day_high)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_day_high
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_day_high)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_day_high
                        END))::numeric AS market_day_high,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_day_low)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_day_low
                        END,
                        CASE
                            WHEN ((web_finance.regular_market_day_low)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regular_market_day_low
                        END))::numeric AS market_day_low,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_price
                        END,
                        CASE
                            WHEN ((web_finance.current_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.current_price
                        END))::numeric AS market_price,
                    (COALESCE(
                        CASE
                            WHEN ((web_finance.market_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.market_volume
                        END,
                        CASE
                            WHEN ((web_finance.regualr_market_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.regualr_market_volume
                        END))::numeric AS market_volume,
                    (
                        CASE
                            WHEN ((web_finance.target_low_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_low_price
                        END)::numeric AS target_low_price,
                    (
                        CASE
                            WHEN ((web_finance.target_high_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_high_price
                        END)::numeric AS target_high_price,
                    (
                        CASE
                            WHEN ((web_finance.target_median_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_median_price
                        END)::numeric AS target_median_price,
                    (
                        CASE
                            WHEN ((web_finance.target_mean_price)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.target_mean_price
                        END)::numeric AS target_mean_price,
                    (
                        CASE
                            WHEN ((web_finance.number_of_analyst)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.number_of_analyst
                        END)::numeric AS number_of_analyst,
                    (
                        CASE
                            WHEN ((web_finance.recommendation_mean)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.recommendation_mean
                        END)::numeric AS recommendation_mean,
                    (
                        CASE
                            WHEN ((web_finance.average_volume)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.average_volume
                        END)::numeric AS average_volume,
                    (
                        CASE
                            WHEN ((web_finance.ask)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.ask
                        END)::numeric AS ask,
                    (
                        CASE
                            WHEN ((web_finance.ask_size)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.ask_size
                        END)::numeric AS ask_size,
                    (
                        CASE
                            WHEN ((web_finance.bid)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.bid
                        END)::numeric AS bid,
                    (
                        CASE
                            WHEN ((web_finance.bid_size)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.bid_size
                        END)::numeric AS bid_size,
                    (
                        CASE
                            WHEN ((web_finance.profit_margins)::text = ''::text) THEN NULL::character varying
                            ELSE web_finance.profit_margins
                        END)::numeric AS profit_margins,
                    (web_finance.sys_load_time)::date AS load_date,
                    row_number() OVER (PARTITION BY ((web_finance.sys_load_time)::date) ORDER BY (web_finance.forward_pe)::numeric DESC) AS perank
                   FROM public.web_finance
                  WHERE (((web_finance.forward_pe)::text <> ''::text) AND ((web_finance.forward_pe)::text <> 'Infinity'::text) AND ((web_finance.average_volume)::text <> ''::text))) temp_1) temp;


--
-- TOC entry 214 (class 1259 OID 16788)
-- Name: v_performance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_performance AS
 SELECT vc.code,
    vc.ranking,
    vl.load_date,
    ((vl.market_price - vc.market_price) / vc.market_price) AS ratio
   FROM (((public.v_instance vc
     JOIN public.v_idx_date idc ON ((vc.load_date = idc.load_date)))
     JOIN public.v_idx_date idl ON ((idc.date_index = (idl.date_index - 1))))
     JOIN public.v_instance_full vl ON ((idl.load_date = vl.load_date)))
  WHERE (((vc.code)::text = (vl.code)::text) AND (vc.ranking < 6) AND (idc.date_index >= ( SELECT (max(v_idx_date.date_index) - 10)
           FROM public.v_idx_date)));


--
-- TOC entry 215 (class 1259 OID 16793)
-- Name: v_daily_performance_top; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_daily_performance_top AS
 SELECT t1.load_date,
    t1.top1,
    t2.top2,
    t3.top3,
    t4.top4,
    t5.top5
   FROM ((((( SELECT v_performance.load_date,
            (avg(v_performance.ratio) + (1)::numeric) AS top1
           FROM public.v_performance
          WHERE (v_performance.ranking < 2)
          GROUP BY v_performance.load_date) t1
     JOIN ( SELECT v_performance.load_date,
            (avg(v_performance.ratio) + (1)::numeric) AS top2
           FROM public.v_performance
          WHERE (v_performance.ranking < 3)
          GROUP BY v_performance.load_date) t2 ON ((t1.load_date = t2.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (avg(v_performance.ratio) + (1)::numeric) AS top3
           FROM public.v_performance
          WHERE (v_performance.ranking < 4)
          GROUP BY v_performance.load_date) t3 ON ((t2.load_date = t3.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (avg(v_performance.ratio) + (1)::numeric) AS top4
           FROM public.v_performance
          WHERE (v_performance.ranking < 5)
          GROUP BY v_performance.load_date) t4 ON ((t3.load_date = t4.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (avg(v_performance.ratio) + (1)::numeric) AS top5
           FROM public.v_performance
          WHERE (v_performance.ranking < 6)
          GROUP BY v_performance.load_date) t5 ON ((t4.load_date = t5.load_date)));


--
-- TOC entry 216 (class 1259 OID 16798)
-- Name: v_daily_performance_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_daily_performance_view AS
 SELECT v1.load_date,
    v1.view1,
    v2.view2,
    v3.view3,
    v4.view4,
    v5.view5
   FROM ((((( SELECT v_performance.load_date,
            (v_performance.ratio + (1)::numeric) AS view1
           FROM public.v_performance
          WHERE (v_performance.ranking = 1)) v1
     JOIN ( SELECT v_performance.load_date,
            (v_performance.ratio + (1)::numeric) AS view2
           FROM public.v_performance
          WHERE (v_performance.ranking = 2)) v2 ON ((v1.load_date = v2.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (v_performance.ratio + (1)::numeric) AS view3
           FROM public.v_performance
          WHERE (v_performance.ranking = 3)) v3 ON ((v2.load_date = v3.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (v_performance.ratio + (1)::numeric) AS view4
           FROM public.v_performance
          WHERE (v_performance.ranking = 4)) v4 ON ((v3.load_date = v4.load_date)))
     JOIN ( SELECT v_performance.load_date,
            (v_performance.ratio + (1)::numeric) AS view5
           FROM public.v_performance
          WHERE (v_performance.ranking = 5)) v5 ON ((v4.load_date = v5.load_date)));


--
-- TOC entry 217 (class 1259 OID 16803)
-- Name: v_incre_top; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_incre_top AS
 SELECT t2.load_date,
    ((10)::numeric ^ sum(log(t1.top1))) AS top1,
    ((10)::numeric ^ sum(log(t1.top2))) AS top2,
    ((10)::numeric ^ sum(log(t1.top3))) AS top3,
    ((10)::numeric ^ sum(log(t1.top4))) AS top4,
    ((10)::numeric ^ sum(log(t1.top5))) AS top5
   FROM (public.v_daily_performance_top t1
     CROSS JOIN public.v_daily_performance_top t2)
  WHERE (t1.load_date <= t2.load_date)
  GROUP BY t2.load_date
  ORDER BY t2.load_date;


--
-- TOC entry 218 (class 1259 OID 16807)
-- Name: v_incre_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_incre_view AS
 SELECT t2.load_date,
    ((10)::numeric ^ sum(log(t1.view1))) AS view1,
    ((10)::numeric ^ sum(log(t1.view2))) AS view2,
    ((10)::numeric ^ sum(log(t1.view3))) AS view3,
    ((10)::numeric ^ sum(log(t1.view4))) AS view4,
    ((10)::numeric ^ sum(log(t1.view5))) AS view5
   FROM (public.v_daily_performance_view t1
     CROSS JOIN public.v_daily_performance_view t2)
  WHERE (t1.load_date <= t2.load_date)
  GROUP BY t2.load_date
  ORDER BY t2.load_date;


--
-- TOC entry 213 (class 1259 OID 16783)
-- Name: v_list; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_list AS
 SELECT i.code,
    i.load_date,
    i.ranking
   FROM (public.v_instance i
     JOIN public.v_idx_date d ON ((i.load_date = d.load_date)))
  WHERE ((i.ranking < 6) AND (d.date_index > ( SELECT (max(v_idx_date.date_index) - 10)
           FROM public.v_idx_date)))
  ORDER BY i.load_date DESC, i.ranking;


--
-- TOC entry 226 (class 1259 OID 18876)
-- Name: v_ranking_list; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_ranking_list AS
 WITH temp_pool AS (
         SELECT v_current_list.code AS code0,
            lead(v_current_list.code, 1, NULL::character varying) OVER (PARTITION BY v_current_list.total_rank ORDER BY v_current_list.rnum) AS code1,
            lead(v_current_list.code, 2, NULL::character varying) OVER (PARTITION BY v_current_list.total_rank ORDER BY v_current_list.rnum) AS code2,
            lead(v_current_list.code, 3, NULL::character varying) OVER (PARTITION BY v_current_list.total_rank ORDER BY v_current_list.rnum) AS code3,
            lead(v_current_list.code, 4, NULL::character varying) OVER (PARTITION BY v_current_list.total_rank ORDER BY v_current_list.rnum) AS code4,
            v_current_list.load_date,
            v_current_list.rnum,
            v_current_list.total_rank
           FROM public.v_current_list
        ), recon AS (
         SELECT p1.load_date,
            p1.rnum,
            json_array_elements_text(json_build_array(p1.code0, p1.code1, p1.code2, p1.code3, p1.code4, p2.code0, p2.code1, p2.code2, p2.code3, p2.code4, p3.code0, p3.code1, p3.code2, p3.code3, p3.code4, p4.code0, p4.code1, p4.code2, p4.code3, p4.code4, p5.code0, p5.code1, p5.code2, p5.code3, p5.code4)) AS code,
            json_array_elements_text(json_build_array(1, 0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7, 0.6, 0.5, 0.8, 0.7, 0.6, 0.5, 0.4, 0.7, 0.6, 0.5, 0.4, 0.3, 0.6, 0.5, 0.4, 0.3, 0.2)) AS weight
           FROM ((((temp_pool p1
             JOIN temp_pool p2 ON (((p1.rnum = p2.rnum) AND (p1.total_rank = 1) AND (p2.total_rank = 2))))
             JOIN temp_pool p3 ON (((p1.rnum = p3.rnum) AND (p1.total_rank = 1) AND (p3.total_rank = 3))))
             JOIN temp_pool p4 ON (((p1.rnum = p4.rnum) AND (p1.total_rank = 1) AND (p4.total_rank = 4))))
             JOIN temp_pool p5 ON (((p1.rnum = p5.rnum) AND (p1.total_rank = 1) AND (p5.total_rank = 5))))
        )
 SELECT recon.load_date,
    recon.code,
    sum((recon.weight)::numeric) AS weight
   FROM recon
  WHERE (recon.rnum <= 5)
  GROUP BY recon.load_date, recon.code
  ORDER BY recon.load_date DESC, (sum((recon.weight)::numeric)) DESC;


--
-- TOC entry 207 (class 1259 OID 16626)
-- Name: web_sentiment; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.web_sentiment (
    code character varying,
    publish_title character varying,
    publish_time character varying,
    sys_load_time timestamp with time zone DEFAULT now()
);


--
-- TOC entry 3915 (class 1259 OID 22135)
-- Name: idx_c_cd; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_c_cd ON public.web_consensus USING btree (code);


--
-- TOC entry 3914 (class 1259 OID 22136)
-- Name: idx_f_cd; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_f_cd ON public.web_finance USING btree (code);


--
-- TOC entry 3912 (class 1259 OID 22134)
-- Name: idx_s_cd; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_s_cd ON public.daily_short USING btree (product_code);


--
-- TOC entry 3916 (class 1259 OID 22697)
-- Name: idx_vc; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_vc ON public.view_consensus USING btree (code, load_date);


--
-- TOC entry 3917 (class 1259 OID 22696)
-- Name: idx_vf; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_vf ON public.view_finance USING btree (code, load_date);


--
-- TOC entry 3918 (class 1259 OID 22698)
-- Name: idx_vs; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_vs ON public.view_short USING btree (code, load_date);


--
-- TOC entry 3913 (class 1259 OID 16625)
-- Name: tm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX tm_idx ON public.tick_merge USING btree (code, record_time);


-- Completed on 2021-02-06 13:12:44

--
-- PostgreSQL database dump complete
--

