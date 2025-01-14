"""
Market Data Provider.

This module contains implementations of the DataProvider abstract class, which
defines methods by which market information can be requested and presented.
"""

from abc import abstractmethod
from io import StringIO
import json
import os
import pathlib
import time
from typing import Any, Dict

import pandas as pd
import psycopg2
import requests


class DataCacheHandler:
    """
    An abstract class for handling data caching operations.
    """

    def __init__(self, ticker: str, *, memory_cache_size: int = 10):
        """
        Init function.
        +ticker+ is the ticker.
        +memory_cache_size+ is the number of entries to be stored in
        memory, to avoid duplicate accesses to persistent storage.
        """
        self.ticker = ticker
        self.memory_cache_size = memory_cache_size

        self._memory_cache = {}
        self._memory_cache_history = []

    def _check_memory_cache(self, key: str):
        """
        Checks for data associated with a given +key+ in the memory cache.
        If found, return it, else return None.
        """
        if key in self._memory_cache:
            cache_entry = self._memory_cache[key]
            if len(self._memory_cache) == self.memory_cache_size:
                self._memory_cache_history.remove(key)
                self._memory_cache_history.append(key)
            return cache_entry
        return None

    def _add_memory_cache(self, key: str, frame: pd.DataFrame):
        """
        Adds a +frame+ associated with a given +key+ to the memory cache.
        If the cache is full, pops off the least recently accessed entry.
        """
        # First, ensure we're not adding a duplicate
        if self._check_memory_cache(key) is not None:
            return

        # If necessary, purge the oldest item from the cache
        if len(self._memory_cache) == self.memory_cache_size:
            old_name = self._memory_cache_history.pop(0)
            del self._memory_cache[old_name]
        self._memory_cache[key] = frame
        self._memory_cache_history.append(key)

    def _get_key_by_timestamp(self, time: pd.Timestamp, interval: str):
        """
        Gets the key used to index the local cache for a given +time+ for
        data associated with a given +interval+, such as "day".

        Note that when 3.10+ is supported, the interval can become an enum.
        """
        simple_keys = {
            "weekly": "per_week",
            "monthly": "per_month"
            }
        if interval == "daily":
            return f"{time.year}_per_day"
        if interval == "intraday":
            return f"{time.day}_{time.month}_{time.year}_per_minute"
        elif interval in simple_keys:
            return simple_keys[interval]

        raise RuntimeError("Interval '{interval}' not supported!")            

    def retrieve(self, time: pd.Timestamp, interval: str):
        """
        Gets any locally cached data for a given +time+ for data associated
        with a given +interval+, such as "day".  There are two layers of
        caching - one is a direct memory cache of the relevant dataframe,
        the other is persistent.

        Note that when 3.10+ is supported, +interval+ can become an enum.
        """

        # First check the memory cache
        key = self._get_key_by_timestamp(time, interval)
        data = self._check_memory_cache(key)
        if data is not None:
            return data

        # Next, check the persistent cache
        return self._check_persistent_cache(time, interval)

    def store(self, time: pd.Timestamp, interval: str, data):
        """
        Stores +data+ for a given +time+ and +interval+ into the local cache
        heirarchy.  There are two layers of caching - one is a direct memory
        cache of the relevant dataframe, the other is persistent.

        Note that when 3.10+ is supported, +interval+ can become an enum.
        """
        # Add to both memory and persistent cache
        key = self._get_key_by_timestamp(time, interval)
        self._add_memory_cache(key, data)
        self._store_persistent_cache(time, interval, data)

    def _check_persistent_cache(self, time: pd.Timestamp, interval: str):
        """
        Gets any data cached in persistent space for a given +time+ for
        data associated with a given +interval+, such as "day".
        """

    def _store_persistent_cache(self, time: pd.Timestamp, interval: str, data):
        """
        Stores dataframe +data+ for a given +time+ for associated with a given
        +interval+ (such as "day") to a persistent space.
        """


class DataProvider:
    """
    Abstract class defining the DataProvider API.
    """

    def __init__(self, cache_handler: DataCacheHandler):
        """
        Init function.
        +cache_handler+ is an object which handles local caching operations.
        """
        self.cache_handler = cache_handler

    @abstractmethod
    def intraday(self, day: pd.Timestamp):
        """
        Gets the intraday data for a given day.
        """

    @abstractmethod
    def daily(self, year: pd.Timestamp):
        """
        Gets the yearly data for a given +year+.
        """

    @abstractmethod
    def weekly(self):
        """
        Returns a frame containing all weekly data+.
        """

    @abstractmethod
    def monthly(self):
        """
        Returns a frame containing all monthly data.
        """

    @abstractmethod
    def first(self) -> pd.Timestamp:
        """
        Returns the earliest date for which all types of data are available.
        """

    @abstractmethod
    def latest(self) -> pd.Timestamp:
        """
        Returns the latest date for which all types of data are available.
        """

    def access_all(self):
        """
        Simulates accesses of all kinds.  Designed to allow caching
        implementations to perform all of their caching up front.
        """


class PostgresDataCacheHandler(DataCacheHandler):
    """
    A class handling data caching operations performed via a heirarchy of CSV
    files.
    """

    def __init__(self, ticker: str, *,
                 postgres_server: str = "localhost",
                 postgres_username: str = "",
                 postgres_database: str = "skintbroker",
                 **kwargs: Dict[str, Any]):
        """
        Init function.

        +reqs_per_minute+ is the number of requests allowed per minute.
        +ticker+ provides the ticker symbol for the underlying FD.
        +memory_cache_size+ is the total number of entries to keep on-hand to
        speed up repeated accesses.
        +postgres_server+ is the URL of the postgres server used for persistent
        caching
        +postgres_username+ is the username for the postgres server
        +postgres_database+ is the database on the postgres server

        NOTE: This object assumes it is the only user of the API key at any
        given time, and will attempt the maximum number of accesses possible.
        """
        super().__init__(ticker, **kwargs)
        self.server = postgres_server
        self.username = postgres_username
        self.database = postgres_database

        self.columns = ['open', 'high', 'low', 'close', 'volume']

        # Get Postgres database password
        self.password = os.environ.get("SKINTBROKER_AV_POSTGRES_PASS")
        if not self.password:
            raise RuntimeError("No Postgres database password - please set "
                               "SKINTBROKER_AV_POSTGRES_PASS")

        # Make server connection
        self.connection = self.__connect_to_db()
        self.cursor = self.connection.cursor()

        # Verify or create all tables.  In the future, we could also create
        # the database, but we'd like to ensure that the user is placing
        # information in the right place.  Thus, error if the database is
        # not present.
        self.__verify_tables()

    def _check_persistent_cache(self, time: pd.Timestamp, interval: str):
        """
        Gets any data cached in persistent space for a given +time+ for
        data associated with a given +interval+, such as "day".  For
        this implementation, this includes searching a file hierarchy.
        """

        # First, query the database to and try to get the requisite values.
        # Start by generating the query, using conditions based on the
        # interval type
        query = "SELECT * FROM {ticker}_{interval}"
        if interval in ["intraday", "daily"]:
            query += f" WHERE EXTRACT(year from \"timestamp\") = {time.year}"
            query += f" AND EXTRACT(month from \"timestamp\") = {time.month}"
            if interval == "intraday":
                query += f" AND EXTRACT(day from \"timestamp\") = {time.day}"
        elif interval in ["weekly", "monthly"]:
            # Data is small enough that no special condition is needed
            pass
        else:
            raise RuntimeError(f"Unknown interval: {interval}")
        query_vars = {'ticker': self.ticker.lower(),
                      'interval': interval}

        # Next, perform the query and generate a dataframe
        self.cursor.execute(query.format(**query_vars))
        data = self.cursor.fetchall()
        frame = pd.DataFrame(data, columns=["timestamp", *self.columns])

        # Format the dataframe correctly
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])
        frame.set_index("timestamp", drop=True, inplace=True)

        # Determine if this data is complete or should be ignored
        update = True
        if not frame.empty:

            # If the data isn't sufficiently recent, update anyway.  As
            # the conditions are rather involved, they're broken up here
            # to make them easy to understand
            now = _now()
            if interval == "intraday":
                update = False

            if interval == "daily" and \
               (time.year != now.year or \
               frame.index[0].dayofyear != now.dayofyear):
                update = False

            now = _now()
            if interval == "weekly" and frame.index[0].week == now.week:
                update = False

            # If the data isn't recent, update
            if interval == "monthly" and frame.index[0].month == now.month:
                update = False

        if not update:
            # No update needed, just return the data
            key = self._get_key_by_timestamp(time, interval)
            self._add_memory_cache(key, frame)
            return frame

        return None

    def _store_persistent_cache(self, time: pd.Timestamp, interval: str, data):
        """
        Stores dataframe +data+ for a given +time+ for associated with a given
        +interval+ (such as "day") to a persistent space.  For this
        implementation, store it to a postgres database.
        """
        # The original implementation of this function used the standard approach
        # of converting the data into an in-memory CSV format, then converting it
        # to an SQL request via psycopg2's built-in copy.  This method was the
        # overall fastest, according to a comparative study, but broke down when
        # attempting update-on-conflict inserts (upserts).  The new method allows
        # upserts, but at the expense of generating a brutally large mega-query,
        # the speed of which is at the mercy of psycopg2's internals.

        # First, make sure there's some data to work with
        if data.empty:
            return

        # First, genreate the mega-query
        query = ""
        for index, row in data.iterrows():
            query += (f"INSERT INTO {self.ticker.lower()}_{interval} "
                      "(timestamp, open, high, low, close, volume) VALUES("
                      f"'{index}', {row['open']}, {row['high']}, "
                      f"{row['low']}, {row['close']}, {row['volume']}"
                      ") ON CONFLICT (timestamp) DO UPDATE SET "
                      "(open, high, low, close, volume) = "
                      "(EXCLUDED.open, EXCLUDED.high, EXCLUDED.low,"
                      " EXCLUDED.close, EXCLUDED.volume);\n")

        # Then execute the mega-query
        self.cursor.execute(query)
        self.connection.commit()

    def __connect_to_db(self):
        """
        Establish and return a connection to the postgres database.
        """
        # Don't catch any exceptions - they're sufficiently verbose and it's
        # best if they just tank the attempt.
        return psycopg2.connect(host=self.server,
                                database=self.database,
                                user=self.username,
                                password=self.password)

    def __verify_tables(self):
        """
        Populates the database, ensuring that all relevant tables are
        present.  If they're already there, leave them alone.
        """
        # First, load the query template
        sql_path = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))/"resources"/"sql"
        populate_db_path = sql_path/"populate_db.sql"
        query = populate_db_path.read_text()

        # Next, set up the query variables
        query_vars = {'ticker': self.ticker.lower(),
                      'username': self.username}

        # Execute
        self.cursor.execute(query.format(**query_vars))
        self.connection.commit()


class CSVDataCacheHandler(DataCacheHandler):
    """
    A class handling data caching operations performed via a heirarchy of CSV
    files.
    """

    def __init__(self, ticker: str, *, cache: str = "cache",
                 **kwargs: Dict[str, Any]):
        """
        Init function.

        +cache+ provides a directory which the DataProvider can use to
        speed up repeated accesses.
        """
        super().__init__(ticker, **kwargs)
        self.cache = pathlib.Path(cache)

        # Ensure the cache is suitable
        if self.cache.exists() and not self.cache.is_dir():
            raise RuntimeError("Cache must be a directory")
        self.cache.mkdir(exist_ok=True, parents=True)

    def __get_csv_path(self, time: pd.Timestamp, interval: str):
        """
        Gets the CSV associated with a given +time+ and +interval+.
        """
        cache_dir = self.cache/self.ticker
        key = self._get_key_by_timestamp(time, interval)
        if interval == "intraday":
            return cache_dir/str(time.year)/str(time.month)/f"{key}.csv"
        if interval == "daily":
            return cache_dir/str(time.year)/f"{key}.csv"
        if interval in ["weekly", "monthly"]:
            return cache_dir/f"{key}.csv"
        else:
            raise RuntimeError("Interval '{interval}' not supported!")            

    def _check_persistent_cache(self, time: pd.Timestamp, interval: str):
        """
        Gets any data cached in persistent space for a given +time+ for
        data associated with a given +interval+, such as "day".  For
        this implementation, this includes searching a file hierarchy.
        """
        key = self._get_key_by_timestamp(time, interval)
        csv = self.__get_csv_path(time, interval)

        update = True
        if csv.exists():
            frame = pd.read_csv(csv, parse_dates=[0],
                                infer_datetime_format=True,
                                index_col='time')

            # If the data isn't sufficiently recent, update anyway.  As
            # the conditions are rather involved, they're broken up here
            # to make them easy to understand
            now = _now()
            if interval == "intraday":
                update = False

            if interval == "daily" and \
               (time.year != now.year or \
               frame.index[0].dayofyear != now.dayofyear):
                update = False

            now = _now()
            if interval == "weekly" and frame.index[0].week == now.week:
                update = False

            # If the data isn't recent, update
            if interval == "monthly" and frame.index[0].month == now.month:
                update = False

        if not update:
            # No update needed, just return the data
            self._add_memory_cache(key, frame)
            return frame

        return None

    def _store_persistent_cache(self, time: pd.Timestamp, interval: str, data):
        """
        Stores dataframe +data+ for a given +time+ for associated with a given
        +interval+ (such as "day") to a persistent space.  For this
        implementation, this involves storing a CSV in a file hierarchy.
        """
        csv = self.__get_csv_path(time, interval)
        csv_dir = csv.parent
        csv_dir.mkdir(exist_ok=True, parents=True)
        data.to_csv(csv, index_label='time')



class AVDataProvider(DataProvider):
    """
    A subclass of DataProvider which uses the AlphaVantage API.
    """

    def __init__(self, ticker: str, *,
                 reqs_per_minute: int = 5,
                 **kwargs: Dict[str, Any]):
        """
        Init function.

        +reqs_per_minute+ is the number of requests allowed per minute.
        +ticker+ provides the ticker symbol for the underlying FD.
        +memory_cache_size+ is the total number of entries to keep on-hand to
        speed up repeated accesses.

        NOTE: This object assumes it is the only user of the API key at any
        given time, and will attempt the maximum number of accesses possible.
        """
        super().__init__(**kwargs)
        self.ticker = ticker
        self.reqs_per_minute = reqs_per_minute
        self._calls = []

        # Get AlphaVantage API key
        self.api_key = os.environ.get("SKINTBROKER_AV_API_KEY")
        if not self.api_key:
            raise RuntimeError("No AlphaVantage API key detected - please set "
                               "SKINTBROKER_AV_API_KEY")


    def intraday(self, day: pd.Timestamp):
        """
        Gets the intraday data for a given day.
        """
        # TODO handle today data

        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(day, "intraday")
        if frame is not None:
            return frame

        # Otherwise, download it.  Intraday data is divided into 30-day
        # segments, so first determine just how far back to look.
        days = (_now().floor('d') - day.floor('d')).days
        month = (days // 30) % 12 + 1
        year = (days // 360) + 1
        params = {"function": "TIME_SERIES_INTRADAY_EXTENDED",
                  "interval": "1min",
                  "symbol": self.ticker,
                  "slice": f"year{year}month{month}"}
        request_frame = self._api_request(**params)
        if request_frame.empty:
            return None

        # Cache all downloaded data - no point in wasting queries!
        grouper = pd.Grouper(freq='D')
        for date, group in request_frame.groupby(grouper):
            self.cache_handler.store(date, "intraday", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(day, "intraday")
        return frame

    def daily(self, year: pd.Timestamp):
        """
        Gets the yearly data for a given +year+.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(year, "daily")
        if frame is not None:
            return frame

        # Update from remote
        params = {"function": "TIME_SERIES_DAILY",
                  "symbol": self.ticker,
                  "outputsize": "full"}
        request_frame = self._api_request(**params)

        # Cache all returned data
        grouper = pd.Grouper(freq='Y')
        for date, group in request_frame.groupby(grouper):
            self.cache_handler.store(date, "daily", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(year, "daily")
        return frame

    def weekly(self):
        """
        Returns a frame containing all weekly data.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(_now(), "weekly")
        if frame is not None:
            return frame

        # Update from remote
        # Set up call parameters
        params = {"function": "TIME_SERIES_WEEKLY_ADJUSTED",
                  "symbol": self.ticker}
        request_frame = self._api_request(**params)

        # Cache returned data.
        self.cache_handler.store(_now(), "weekly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(_now(), "weekly")
        return frame

    def monthly(self):
        """
        Returns a frame containing all monthly data.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(_now(), "monthly")
        if frame is not None:
            return frame

        # Update from remote
        # Set up call parameters
        params = {"function": "TIME_SERIES_MONTHLY_ADJUSTED",
                  "symbol": self.ticker}
        request_frame = self._api_request(**params)

        # Cache returned data.
        self.cache_handler.store(_now(), "monthly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(_now(), "monthly")
        return frame

    def _api_request(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        """
        Performs an API request using the passed parameters.  Returns a
        DataFrame or None.
        """

        # Assemble the query
        site = "https://www.alphavantage.co/query?"
        params = [f"{key}={val}" for key, val in \
                {**kwargs, "apikey": self.api_key, "datatype": "csv"}.items()]
        query = "&".join(params)

        # Perform call limit bookkeeping, and delay if needed.
        if len(self._calls) >= self.reqs_per_minute:
            oldest_call = self._calls.pop(0)
            to_wait = 60 - (_now() - oldest_call).seconds
            if to_wait >= 0:
                time.sleep(to_wait + 1)

        # Call the API and generate the dataframe
        print("Querying: " + site + query)
        response = requests.get(site + query)
        response.encoding = 'utf-8'
        index_label = 'time' if "INTRADAY" in kwargs["function"] \
                      else 'timestamp'

        frame = pd.read_csv(StringIO(response.text), parse_dates=[0],
                            infer_datetime_format=True,
                            index_col=index_label)

        # Record this call for future checks
        self._calls.append(_now())

        return frame

    def first(self) -> pd.Timestamp:
        """
        Returns the earliest date for which all types of data are available.
        """
        # Based on the AlphaVantage system, it's reasonable to assume data
        # exists for two years back from today.  Note that it's entirely
        # possible that cached data exists from even earlier, so a future
        # extension should search for it.
        return _now() - pd.Timedelta(720 - 1, unit='d')

    def latest(self) -> pd.Timestamp:
        """
        Returns the latest date for which all types of data are available.
        """
        # Yesterday is fine
        return _now() - pd.Timedelta(1, unit='d')

    def access_all(self) -> None:
        """
        Simulates accesses of all kinds.  Designed to allow caching
        implementations to perform all of their caching up front.
        """
        # First, handle daily, weekly, and monthly entries for the last 20
        # years.  As this comes in one immense blob, just access that.
        now = _now()
        self.monthly()
        self.weekly()
        self.daily(now)

        # Then handle intraday for the last 2 years.
        days = pd.date_range(end=now, freq='D', periods=360 * 2 - 1)
        for day in days:
            if day.weekday() <= 4:
                self.intraday(day)


class FTXDataProvider(DataProvider):
    """
    A subclass of DataProvider which uses the FTX Crypto Exchange API.
    """

    def __init__(self, ticker: str, *,
                 reqs_per_minute: int = 60,
                 **kwargs: Dict[str, Any]):
        """
        Init function.

        +reqs_per_minute+ is the number of requests allowed per minute.
        +ticker+ provides the ticker symbol for the underlying FD.
        +memory_cache_size+ is the total number of entries to keep on-hand to
        speed up repeated accesses.
        """
        super().__init__(**kwargs)
        self.ticker = ticker
        self.reqs_per_minute = reqs_per_minute
        self._calls = []

    def intraday(self, day: pd.Timestamp):
        """
        Gets the intraday data for a given day.
        """
        # TODO handle today data

        # First, make sure there is a timezone associated with the
        # data.  If not, assume the day starts ate 00:00:00 EST.
        if not day.tz:
            day = day.tz_localize('EST', nonexistent='shift_backward')
        else:
            day = day.astimezone('EST')

        # Next, check if the data is already cached
        frame = self.cache_handler.retrieve(day, "intraday")
        if frame is not None:
            return frame

        # Otherwise, download it. Generate midnight start and end times,
        # ensuring that all stay exactly within the bounds of the day.
        start = day.floor('d')
        end = day + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)

        # Perform request.  We want a 60 second window length.
        query = self._gen_query(start, end, 60)
        request_frame = self._api_request(query)
        if request_frame.empty:
            return None

        # Cache all downloaded data - no point in wasting queries!
        grouper = pd.Grouper(freq='D')
        for date, group in request_frame.groupby(grouper):
            self.cache_handler.store(date, "intraday", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(day, "intraday")
        return frame

    def daily(self, year: pd.Timestamp):
        """
        Gets the yearly data for a given +year+.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(year, "daily")
        if frame is not None:
            return frame

        # Perform request.  We want a one day window length.
        query = self._gen_query(self.first(), self.latest(), 86400)
        request_frame = self._api_request(query)
        if request_frame.empty:
            return None

        # Cache all returned data
        grouper = pd.Grouper(freq='Y')
        for date, group in request_frame.groupby(grouper):
            self.cache_handler.store(date, "daily", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(year, "daily")
        return frame

    def weekly(self):
        """
        Returns a frame containing all weekly data.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(_now(), "weekly")
        if frame is not None:
            return frame

        # Perform request.  We want a seven day window length.
        query = self._gen_query(self.first(), self.latest(), 86400 * 7)
        request_frame = self._api_request(query)
        if request_frame.empty:
            return None

        # Cache returned data.
        self.cache_handler.store(_now(), "weekly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(_now(), "weekly")
        return frame

    def monthly(self):
        """
        Returns a frame containing all monthly data.
        """
        # First, check if the data is already cached
        frame = self.cache_handler.retrieve(_now(), "monthly")
        if frame is not None:
            return frame

        # Perform request.  We want a 30 day window length, per an
        # archaic standard set by the AlphaVantage provider.
        query = self._gen_query(self.first(), self.latest(), 86400 * 30)
        request_frame = self._api_request(query)
        if request_frame.empty:
            return None

        # Cache returned data.
        self.cache_handler.store(_now(), "monthly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self.cache_handler.retrieve(_now(), "monthly")
        return frame

    def _gen_query(self, start: pd.Timestamp, end: pd.Timestamp,
                   interval: int) -> str:
        """
        Generates a query string for historic data starting at +start+ and
        ending at +end+, with an +interval+ second interval between
        entries

        Note that any timestamps /must/ be timezone aware.
        """
        # Set up parameters.  Note that timestamps are in epoch time.
        params = {
            "start_time": start.timestamp(),
            "end_time": end.timestamp(),
            "resolution": interval
        }

        # Generate the query
        site = f"https://ftx.com/api/markets/{self.ticker}/USD/candles?"
        params = [f"{key}={val}" for key, val in params.items()]
        return site + "&".join(params)

    def _api_request(self, query: str) -> pd.DataFrame:
        """
        Performs a passed API +query+, converting the returned JSON into a
        DataFrame.
        """

        # Perform call limit bookkeeping, and delay if needed.
        if len(self._calls) >= self.reqs_per_minute:
            oldest_call = self._calls.pop(0)
            to_wait = 60 - (_now() - oldest_call).seconds
            if to_wait >= 0:
                time.sleep(to_wait + 1)

        # Call the API and generate the dataframe
        print("Querying: " + query)
        response = requests.get(query)
        response.encoding = 'utf-8'

        # Response is JSON
        frame = pd.json_normalize(json.load(StringIO(response.text)), 'result',
                                  errors='ignore')
        # Convert to the expected frame format
        frame.drop(columns=['time'], inplace=True)
        frame.rename(columns={'startTime': 'timestamp'}, inplace=True)
        frame['timestamp'] = pd.to_datetime(frame['timestamp']).dt.tz_convert('EST')
        frame.set_index("timestamp", drop=True, inplace=True)

        # Record this call for future checks
        self._calls.append(_now())

        return frame

    def first(self) -> pd.Timestamp:
        """
        Returns the earliest date for which all types of data are available.
        """
        # Shoot for two years ago, as it starts getting spotty before that.
        return _now() - pd.Timedelta(360 * 2 - 1, unit='d')

    def latest(self) -> pd.Timestamp:
        """
        Returns the latest date for which all types of data are available.
        """
        # Yesterday is fine for now
        return _now() - pd.Timedelta(1, unit='d')

    def access_all(self) -> None:
        """
        Simulates accesses of all kinds.  Designed to allow caching
        implementations to perform all of their caching up front.
        """
        # First, handle daily, weekly, and monthly entries for the last 20
        # years.  As this comes in one immense blob, just access that.
        now = _now()
        self.monthly()
        self.weekly()
        self.daily(now)

        # Then handle intraday for the last 2 years.
        days = pd.date_range(end=now.floor('d'), freq='D', periods=360 * 2 - 1)
        for day in days:
            self.intraday(day)


def _now() -> pd.Timestamp:
    """
    Returns the current DateTime.
    """
    return pd.Timestamp.now(tz="EST")
