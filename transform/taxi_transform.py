import json
import pandas as pd 
from endpoint.taxi_data_source import TaxiDS


class Taxi_Tr():
    def __init__(self):
        print("Taxi TR iniatated !")
        
    def process_message(self, message:json):
        try:
            # preprocess data
            ts = message['properies']['timestamp']
            count = message['properies']['taxi_count']
            temp_df = pd.DataFrame(message['geometry']['coordinates'], columns = ['longitude', 'latitude'])
            temp_df['timestamp']= ts
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