import requests
import json
from datetime import datetime
from geopy.geocoders import Nominatim
import pandas as pd
import time
from geopy.extra.rate_limiter import RateLimiter
import geopandas as gpd
from shapely.geometry import Point


url_data = "https://api.data.gov.sg/v1/transport/taxi-availability"
headers = {
        'Accept': 'application/vnd.geo+json',
        'Content-Type': 'application/json',
    }
today = datetime.today()
date_time = '2023-06-07T18:08:50+08:00'

# response_API = requests.get(url=url_data, headers=headers, verify=False).json()

# result = json.dumps(response_API,indent=2)

# print(result)
# print(response_API)

# for field in response_API:
#     print(field)
#     print(response_API[field])
start_date = datetime.today()
print(start_date)
print(start_date.strftime("%Y-%m-%dT00:00:00"))
start_time = datetime.strptime(start_date.strftime("%Y-%m-%dT00:00:00"), "%Y-%m-%dT%H:%M:%S")
print(start_time)
response_API = requests.get(url=url_data+f'?date_time={start_time.strftime("%Y-%m-%dT%H:%M:%S")}', headers=headers, verify=False).json()
print("=============================================================")
print(response_API["features"])
coor_list = response_API["features"][0]['geometry']['coordinates']
temp_df = pd.DataFrame(coor_list, columns = ['longitude', 'latitude'])
print(temp_df)

# Initialize Nominatim API
geolocator = Nominatim(user_agent="MyApp", timeout=10)
# rgeocode = RateLimiter(geolocator.reverse, min_delay_seconds=0.001)
# print(f"{temp_df['longitude'][0]},{temp_df['latitude'][0]}")
# area = geolocator.reverse(f"{temp_df['latitude'][0]},{temp_df['longitude'][0]}")
# print(area)
# city = area.raw['address'].get('county','')
# print(city)

temp_df['points'] = temp_df.apply(lambda x : Point(x['longitude'],x['latitude']), axis =1 )
print(temp_df)
gdf = gpd.read_file('./lib/singapore-week1-homework.geojson')
print(gdf)
dfp = gpd.GeoDataFrame({'geometry': temp_df['points']}, crs=gdf.crs)
print(dfp)
out = gpd.sjoin(dfp, gdf, predicate='within')
print(out[["geometry","NAME","BRK_GROUP"]])


print(out.columns)

# temp_df['county'] = temp_df.apply(lambda x: geolocator.reverse(f"{x['latitude']},{x['longitude']}").raw['address'].get('county',''),pip axis=1)

# temp_df['county'], temp_df['sub_area'] = '',''
# for i, row in temp_df.iterrows():
#     temp = geolocator.reverse(f"{temp_df.at[i,'latitude']},{temp_df.at[i,'longitude']}").raw['address']
#     temp_df.at[i,'county'] = temp.get('county', '')
#     temp_df.at[i,'sub_area'] = temp.get('suburb', '')
#     print(temp_df.iloc[i])
#     time.sleep(0.01)


# temp_df["geom"] = temp_df["latitude"].map(str) + ',' + temp_df["longitude"].map(str)
# print(temp_df['geom'])
# temp_df['address'] = temp_df['geom'].apply(lambda x: rgeocode(x["geom"]), axis = 1)
# print(temp_df)
# temp_df['city'] = frame.apply(lambda x: frame['c'] if frame['c']>0 else frame['b'],axis=0)

