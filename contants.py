from typing import Any

import pandas as pd
from pydantic import BaseModel

MONTH_TO_SEASON = {
    12: "winter",
    1: "winter",
    2: "winter",
    3: "spring",
    4: "spring",
    5: "spring",
    6: "summer",
    7: "summer",
    8: "summer",
    9: "autumn",
    10: "autumn",
    11: "autumn",
}

GREEN_COLOR = "#00ff00"
RED_COLOR = "#ff0000"


class TemperatureStats(BaseModel):
    mean: float
    min: float
    max: float


class CityInfo(BaseModel):
    total_info: Any  # pd.DataFrame
    seasonal_stats: Any  # pd.DataFrame
    temperature_stats: TemperatureStats
    temperature_trend: float
