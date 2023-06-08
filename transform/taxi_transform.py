import json
import pandas as pd 
from geopy.geocoders import Nominatim


class Taxi_Tr():
    def __init__(self):
        print("Taxi TR iniatated !")
        self.geolocator =  Nominatim(user_agent="SingaporeTaxi")
    def process_message(self, message:json):
        try:
            # preprocess data
            message = message['features'][0]
            ts = message['properties']['timestamp']
            count = message['properties']['taxi_count']
            temp_df = pd.DataFrame(message['geometry']['coordinates'], columns = ['longitude', 'latitude'])
            temp_df['timestamp']= ts
            temp_df['sub_area'] = temp_df[['latitude','longitude']].apply(lambda x: self.geolocator.reverse(f"{x.latitude},{x.longitude}", axis=1))
            #count validation
            data_complete = temp_df.shape[0]==count
            dim_df = pd.DataFrame([ts, count,data_complete], columns = ['timestamp', 'count_taxi','data_complete'])
            # out data
            return temp_df, dim_df
        except:
            raise("Error in processing message, check process_message input !")
        finally:
            del temp_df
            del dim_df
            
    def process_bulk_messages(self, messages:list):
        try:
            temp_df = pd.DataFrame()
            temp_dim_df = pd.DataFrame()
            for mes in messages:
                data_df, dim_df = self.process_message(mes)
                if not(temp_df) and not(temp_dim_df):
                    temp_df=data_df
                    temp_dim_df = dim_df
                else:
                    temp_df.append(data_df, ignore_index=True)
                    temp_dim_df.append(dim_df, ignore_index=True)
            return temp_df, temp_dim_df
        except:
            raise("Error in processing bulk messages, check process_message input !")
        finally:
            del temp_df, temp_dim_df, data_df, dim_df