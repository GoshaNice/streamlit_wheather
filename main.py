import asyncio
import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from utils import (
    detect_anomaly,
    get_cities,
    get_current_season,
    get_current_temperature,
    get_seasonal_stats,
)

st.title("Информация о температуре в городах")
cities = None

uploaded_file = st.file_uploader(
    "Загрузите файл с историческими данными по температуре в городах"
)
if uploaded_file is not None:
    df: pd.DataFrame = pd.read_csv(uploaded_file, parse_dates=["timestamp"])
    cities_info = get_seasonal_stats(df)
    cities = get_cities(df)

if cities:
    city = st.selectbox("Выберите город", cities)
    api_key = st.text_input("OpenWeather Api key")
    if city and api_key:
        correct_response, response = asyncio.run(get_current_temperature(city, api_key))
        if correct_response:
            temperature = response
            city_info = cities_info[city]

            st.write(
                f"Средняя температура по историческим данным: {city_info.temperature_stats.mean:.2f}°C"
            )
            st.write(
                f"Минимальная температура по историческим данным: {city_info.temperature_stats.min:.2f}°C"
            )
            st.write(
                f"Максимальная температура по историческим данным: {city_info.temperature_stats.max:.2f}°C"
            )

            st.write(f"Сезонный профиль:")
            st.write(city_info.seasonal_stats)

            anomaly = detect_anomaly(
                seasonal_stats=city_info.seasonal_stats,
                city=city,
                temperature=temperature,
            )
            st.write(f"Температура в {city} прямо сейчас: {temperature}°C")
            if anomaly:
                st.write(f"Температура в городе {city} аномальна для данного сезона")
            else:
                st.write(
                    f"Температура в городе {city} является нормальной для данного сезона"
                )
            temp_trend = (
                "положительный" if city_info.temperature_trend > 0 else "отрицательный"
            )
            st.write(
                f"Температурный тренд {temp_trend} : {city_info.temperature_trend}"
            )

            st.text("График дат с аномальными температурами в исторических данных")
            st.scatter_chart(
                city_info.anomalies,
                x="timestamp",
                y="temperature",
                color="#ff0000",
            )
        else:
            st.write(response)
