import asyncio
import datetime
import multiprocessing as mp
from functools import partial

import requests
from sklearn.linear_model import LinearRegression

from contants import (GREEN_COLOR, MONTH_TO_SEASON, RED_COLOR, CityInfo,
                      TemperatureStats)


async def get_current_temperature(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = await asyncio.to_thread(requests.get, url)
    data = response.json()
    return data["main"]["temp"]


def get_current_season():
    current_month = datetime.datetime.now().month
    return MONTH_TO_SEASON[current_month]


def detect_anomaly(seasonal_stats, city, temperature):
    season = get_current_season()
    mean, std = (
        seasonal_stats.loc[season, "mean"],
        seasonal_stats.loc[season, "std"],
    )
    return temperature > mean + 2 * std or temperature < mean - 2 * std


def get_cities(df):
    return df["city"].unique().tolist()


def merge_seasonal_stats(df, seasonal_stats):
    df = df.merge(seasonal_stats.reset_index(), on=("season"), how="left")
    return df


def get_colors_from_anomalies(df):
    return df["anomaly"].apply(lambda x: RED_COLOR if x else GREEN_COLOR)


def get_temperature_stats(temperature):
    return TemperatureStats(
        mean=temperature.mean(), min=temperature.min(), max=temperature.max()
    )


def get_temperature_trend(df):
    model = LinearRegression()
    model.fit(df[["timestamp"]], df["temperature"])
    return model.coef_[0]


def get_city_seasonal_stats(city, df):
    df = df[df["city"] == city].drop(columns=["city"])
    df["rolling_mean"] = df["temperature"].rolling(window=30, min_periods=1).mean()
    seasonal_stats = df.groupby("season")["rolling_mean"].agg(["mean", "std"])
    df = merge_seasonal_stats(df=df, seasonal_stats=seasonal_stats)
    df["anomaly"] = (df["temperature"] < (df["rolling_mean"] - 2 * df["std"])) | (
        df["temperature"] > (df["rolling_mean"] + 2 * df["std"])
    )
    df["color"] = get_colors_from_anomalies(df)
    temperature_trend = get_temperature_trend(df)
    return CityInfo(
        total_info=df,
        seasonal_stats=seasonal_stats,
        temperature_stats=get_temperature_stats(df["temperature"]),
        temperature_trend=temperature_trend,
    )


def get_seasonal_stats(df):
    cities = get_cities(df)
    with mp.Pool(processes=8) as pool:
        results = pool.map(partial(get_city_seasonal_stats, df=df), cities)
    return dict(zip(cities, results))


# def get_seasonal_stats(df):
#     cities = get_cities(df)
#     df['rolling_mean'] = df.groupby('city')['temperature'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)
#     seasonal_stats = df.groupby(['city', 'season'])["rolling_mean"].agg(['mean', 'std'])
#     df = merge_seasonal_stats(df=df, seasonal_stats=seasonal_stats)
#     df['anomaly'] = (df['temperature'] < (df['rolling_mean'] - 2 * df['std'])) | (df['temperature'] > (df['rolling_mean'] + 2 * df['std']))
#     df['color'] = get_colors_from_anomalies(df)
#     return df, seasonal_stats
