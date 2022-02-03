"""
Market Data Provider.

This module contains implementations of the DataProvider abstract class, which
defines methods by which market information can be requested and presented.
"""

from abc import abstractmethod
from io import StringIO
import os
import pathlib
import time
from typing import Any, Dict

import pandas as pd
import requests


class DataProvider:
    """
    Abstract class defining the DataProvider API.
    """

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


class AVDataProvider(DataProvider):
    """
    An implementation of DataProvider which uses the AlphaVantage API.
    """

    def __init__(self, ticker: str, *,
                 reqs_per_minute: int = 5, cache: str = "cache",
                 memory_cache_size: int = 10,
                 **kwargs: Dict[str, Any]):
        """
        Init function.

        +reqs_per_minute+ is the number of requests allowed per minute.
        +ticker+ provides the ticker symbol for the underlying FD.
        +cache+ provides a directory which the DataProvider can use to
        organize data.
        +memory_cache_size+ is the total number of entries to keep on-hand to
        speed up repeated accesses.

        NOTE: This object assumes it is the only user of the API key at any
        given time, and will attempt the maximum number of accesses possible.
        """
        self.ticker = ticker
        self.reqs_per_minute = reqs_per_minute
        self.cache = pathlib.Path(cache)
        self.memory_cache_size = memory_cache_size

        self._calls = []
        self._memory_cache = {}
        self._memory_cache_history = []

        # Ensure the cache is suitable
        if self.cache.exists() and not self.cache.is_dir():
            raise RuntimeError("Cache must be a directory")
        self.cache.mkdir(exist_ok=True, parents=True)

        # Get AlphaVantage API key
        self.api_key = os.environ.get("SKINTBROKER_AV_API_KEY")
        if not self.api_key:
            raise RuntimeError("No AlphaVantage API key detected - please set "
                               "SKINTBROKER_AV_API_KEY")

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

    def _get_cached_data(self, time: pd.Timestamp, interval: str):
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

    def intraday(self, day: pd.Timestamp):
        """
        Gets the intraday data for a given day.
        """
        # TODO handle today data

        # First, check if the data is already cached
        frame = self._get_cached_data(day, "intraday")
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
            self._store_persistent_cache(date, "intraday", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self._get_cached_data(day, "intraday")
        return frame

    def daily(self, year: pd.Timestamp):
        """
        Gets the yearly data for a given +year+.
        """
        # First, check if the data is already cached
        frame = self._get_cached_data(year, "daily")
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
            self._store_persistent_cache(date, "daily", group)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self._get_cached_data(year, "daily")
        return frame

    def weekly(self):
        """
        Returns a frame containing all weekly data.
        """
        # First, check if the data is already cached
        frame = self._get_cached_data(_now(), "weekly")
        if frame is not None:
            return frame

        # Update from remote
        # Set up call parameters
        params = {"function": "TIME_SERIES_WEEKLY_ADJUSTED",
                  "symbol": self.ticker}
        request_frame = self._api_request(**params)

        # Cache returned data.
        self._store_persistent_cache(_now(), "weekly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self._get_cached_data(_now(), "weekly")
        return frame

    def monthly(self):
        """
        Returns a frame containing all monthly data.
        """
        # First, check if the data is already cached
        frame = self._get_cached_data(_now(), "monthly")
        if frame is not None:
            return frame

        # Update from remote
        # Set up call parameters
        params = {"function": "TIME_SERIES_MONTHLY_ADJUSTED",
                  "symbol": self.ticker}
        request_frame = self._api_request(**params)

        # Cache returned data.
        self._store_persistent_cache(_now(), "monthly", request_frame)

        # Try again.  If there's still no data, there probably isn't any.
        frame = self._get_cached_data(_now(), "monthly")
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


def _now() -> pd.Timestamp:
    """
    Returns the current DateTime.
    """
    return pd.to_datetime("now")
