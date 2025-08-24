#!/usr/bin/env python3
import os
from urllib.parse import quote_plus
import pandas as pd
import requests
import xorq as xo

from xorq.caching import SourceStorage

API = os.getenv("WEATHER_API_URL", "https://wttr.in")

schema_in  = xo.schema({"city": "string"})
schema_out = xo.schema({
    "city": "string",
    "timestamp": "string",
    "temp_c": "double",
    "weather": "string",
})

def _batch(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for city in df["city"].values:
        r = requests.get(f"{API}/{quote_plus(str(city))}", params={"format": "j1"}, timeout=10)
        r.raise_for_status()
        j = r.json()
        rows.append({
            "city": city,
            "timestamp": pd.Timestamp.utcnow().isoformat(),
            "temp_c": float(j["current_condition"][0]["temp_C"]),
            "weather": j["current_condition"][0]["weatherDesc"][0]["value"],
        })
    return pd.DataFrame(rows).reindex(schema_out.to_pyarrow().names, axis=1)

weather_udxf = xo.expr.relations.flight_udxf(
    process_df=_batch,
    maybe_schema_in=schema_in,
    maybe_schema_out=schema_out,
    name="do_weather",
)

cities_list = [c.strip() for c in os.getenv(
    "WEATHER_CITIES", "Norfolk,US|New York,US|Chicago,US"
).split("|") if c.strip()]

cities = xo.memtable([(c,) for c in cities_list], schema=schema_in, name="cities")
w = cities.pipe(weather_udxf)
expr = w.aggregate(
    by=[w.city],
    avg_temp_c=w.temp_c.mean(),
    readings=w.count(),
).cache(storage=SourceStorage(source=xo.duckdb.connect("weather.duckdb")))

out = xo.execute(expr)
print(out.to_string(index=False))
