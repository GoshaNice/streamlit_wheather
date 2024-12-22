import asyncio
import datetime
import multiprocessing as mp
from functools import partial
from typing import List, Dict, Tuple, Union

import requests
from sklearn.linear_model import LinearRegression
import pandas as pd

from contants import MONTH_TO_SEASON, CityInfo, TemperatureStats


async def get_current_temperature(
    city: str, api_key: str
) -> Tuple[str, Union[str, float]]:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = await asyncio.to_thread(requests.get, url)
    if response.ok:
        data = response.json()
        return response.ok, data["main"]["temp"]
    else:
        return response.ok, response.text


def get_current_season() -> str:
    current_month = datetime.datetime.now().month
    return MONTH_TO_SEASON[current_month]


def detect_anomaly(seasonal_stats: pd.DataFrame, city: str, temperature: float) -> bool:
    season = get_current_season()
    mean, std = (
        seasonal_stats.loc[season, "mean"],
        seasonal_stats.loc[season, "std"],
    )
    return temperature > mean + 2 * std or temperature < mean - 2 * std


def get_cities(df: pd.DataFrame) -> List[str]:
    return df["city"].unique().tolist()


def get_temperature_stats(temperature: pd.Series) -> TemperatureStats:
    return TemperatureStats(
        mean=temperature.mean(), min=temperature.min(), max=temperature.max()
    )


def get_temperature_trend(df: pd.DataFrame) -> float:
    model = LinearRegression()
    model.fit(df[["timestamp"]], df["temperature"])
    return model.coef_[0]


def get_city_seasonal_stats(city: str, df: pd.DataFrame) -> CityInfo:
    df = df[df["city"] == city].drop(columns=["city"])
    df["rolling_mean"] = df["temperature"].rolling(window=30, min_periods=1).mean()
    seasonal_stats = df.groupby("season")["rolling_mean"].agg(["mean", "std"])
    df = df.merge(seasonal_stats.reset_index(), on=("season"), how="left")
    df["anomaly"] = (df["temperature"] < (df["rolling_mean"] - 2 * df["std"])) | (
        df["temperature"] > (df["rolling_mean"] + 2 * df["std"])
    )
    temperature_trend = get_temperature_trend(df)
    return CityInfo(
        anomalies=df[df["anomaly"]][["timestamp", "temperature"]],
        seasonal_stats=seasonal_stats,
        temperature_stats=get_temperature_stats(df["temperature"]),
        temperature_trend=temperature_trend,
    )


def get_seasonal_stats(df: pd.DataFrame) -> Dict[str, CityInfo]:
    cities = get_cities(df)
    with mp.Pool(processes=8) as pool:
        results = pool.map(partial(get_city_seasonal_stats, df=df), cities)
    return dict(zip(cities, results))
