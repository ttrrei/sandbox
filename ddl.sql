-- Table: public.transactions

-- DROP TABLE public.transactions;

CREATE TABLE public.transactions
(
    code character varying COLLATE pg_catalog."default",
    load_date date,
    date_index bigint,
    market_open numeric,
    market_previous_close numeric,
    market_day_high numeric,
    market_day_low numeric,
    market_price numeric,
    market_volume numeric,
    target_low_price numeric,
    target_high_price numeric,
    target_median_price numeric,
    target_mean_price numeric,
    number_of_analyst numeric,
    recommendation_mean numeric,
    tailing_pe numeric,
    forward_pe numeric,
    average_volume numeric,
    ask numeric,
    ask_size numeric,
    bid numeric,
    bid_size numeric,
    enterprise_revenue numeric,
    profit_margins numeric,
    enterprise_ebitda numeric,
    quarterly_earning_growth numeric,
    short_position bigint,
    total_in_issue bigint,
    reported_position numeric
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.transactions
    OWNER to postgres;