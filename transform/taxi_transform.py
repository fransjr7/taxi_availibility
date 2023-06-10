import json
import pandas as pd 
import geopandas as gpd
from shapely.geometry import Point


class Taxi_Tr():
    def __init__(self):
        print("Taxi TR iniatated !")
        self.gdf = gpd.read_file('./lib/cities.geojson')

    def process_message(self, message:json):
        try:
            # preprocess data
            message = message['features'][0]
            ts = message['properties']['timestamp']
            count = message['properties']['taxi_count']
            temp_df = pd.DataFrame(message['geometry']['coordinates'], columns = ['longitude', 'latitude'])
            temp_df['timestamp']= ts
            print(f"Processing timestamp : {ts}")
            
            # get city data
            temp_df['geometry'] = temp_df.apply(lambda x : Point(x['longitude'],x['latitude']), axis =1 )
            dfp = gpd.GeoDataFrame({'geometry': temp_df['geometry']}, crs=self.gdf.crs)
            city_out = pd.DataFrame(gpd.sjoin_nearest(dfp, self.gdf, how='left')[["geometry","NAME"]]).drop_duplicates()
            temp_df= pd.merge(temp_df, city_out, on="geometry", how="left").fillna("none").reset_index(drop = True)
            temp_df = temp_df.rename(columns = {"NAME": "city_name"})
            temp_df = temp_df[["timestamp", "longitude", "latitude","city_name"]]

            #count validation
            data_complete = temp_df.shape[0]==count
            dim_df = pd.DataFrame([[ts, count,data_complete]], columns = ['timestamp', 'count_taxi','data_complete'])
            # out data
            return temp_df, dim_df
        except:
            raise("Error in processing message, check process_message input !")

            
    def process_bulk_messages(self, messages:list):
        try:
            temp_df = pd.DataFrame()
            temp_dim_df = pd.DataFrame()
            for mes in messages:
                data_df, dim_df = self.process_message(mes)
                if temp_df.shape[0]==0 and temp_dim_df.shape[0]==0:
                    temp_df=data_df
                    temp_dim_df = dim_df
                else:
                    temp_df = temp_df.append(data_df)
                    temp_dim_df = temp_dim_df.append(dim_df)
            return temp_df, temp_dim_df
        except:
            raise("Error in processing bulk messages, check process_message input !")
