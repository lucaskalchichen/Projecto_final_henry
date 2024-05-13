import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://air-quality-api.open-meteo.com/v1/air-quality"
params = {
	"latitude": 40.7143,
	"longitude": -74.006,
	"hourly": ["european_aqi", "european_aqi_pm2_5", "european_aqi_pm10", "european_aqi_nitrogen_dioxide", "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10", "us_aqi_nitrogen_dioxide", "us_aqi_carbon_monoxide"],
	"start_date": "2022-07-29",
	"end_date": "2024-05-14"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_european_aqi = hourly.Variables(0).ValuesAsNumpy()
hourly_european_aqi_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
hourly_european_aqi_pm10 = hourly.Variables(2).ValuesAsNumpy()
hourly_european_aqi_nitrogen_dioxide = hourly.Variables(3).ValuesAsNumpy()
hourly_us_aqi = hourly.Variables(4).ValuesAsNumpy()
hourly_us_aqi_pm2_5 = hourly.Variables(5).ValuesAsNumpy()
hourly_us_aqi_pm10 = hourly.Variables(6).ValuesAsNumpy()
hourly_us_aqi_nitrogen_dioxide = hourly.Variables(7).ValuesAsNumpy()
hourly_us_aqi_carbon_monoxide = hourly.Variables(8).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["european_aqi"] = hourly_european_aqi
hourly_data["european_aqi_pm2_5"] = hourly_european_aqi_pm2_5
hourly_data["european_aqi_pm10"] = hourly_european_aqi_pm10
hourly_data["european_aqi_nitrogen_dioxide"] = hourly_european_aqi_nitrogen_dioxide
hourly_data["us_aqi"] = hourly_us_aqi
hourly_data["us_aqi_pm2_5"] = hourly_us_aqi_pm2_5
hourly_data["us_aqi_pm10"] = hourly_us_aqi_pm10
hourly_data["us_aqi_nitrogen_dioxide"] = hourly_us_aqi_nitrogen_dioxide
hourly_data["us_aqi_carbon_monoxide"] = hourly_us_aqi_carbon_monoxide

hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe)


hourly_dataframe.to_csv('hourly_data_air_polution.csv', index=False)
hourly_dataframe.to_parquet('hourly_data_air_polution.parquet')