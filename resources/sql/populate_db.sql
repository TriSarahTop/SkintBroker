--
-- SQL Commands for verifying/populating a SkintBroker database
--

--
-- Intraday data table
--

CREATE TABLE IF NOT EXISTS {ticker}_intraday (
    "timestamp" timestamp PRIMARY KEY,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint
);

--
-- Daily data table
--

CREATE TABLE IF NOT EXISTS {ticker}_daily (
    "timestamp" timestamp PRIMARY KEY,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint
);

--
-- Weekly data table
--

CREATE TABLE IF NOT EXISTS {ticker}_weekly (
    "timestamp" timestamp PRIMARY KEY,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision
);

--
-- Monthly data table
--

CREATE TABLE IF NOT EXISTS {ticker}_monthly (
    "timestamp" timestamp PRIMARY KEY,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision
);


ALTER TABLE {ticker}_intraday OWNER TO {username};
ALTER TABLE {ticker}_daily OWNER TO {username};
ALTER TABLE {ticker}_weekly OWNER TO {username};
ALTER TABLE {ticker}_monthly OWNER TO {username};
