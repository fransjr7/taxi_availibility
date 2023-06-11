import json
from datetime import datetime, date , timedelta
import pandas as pd
from repository.postgre_repo import PostgreRepo
from endpoint.taxi_data_source import TaxiDS
from transform.taxi_transform import Taxi_Tr
from jinja2 import Template
from lib.query import del_query_1_key, no_taxi_region

class TaxiUsecase():
    def __init__(self, taxi_conf:json, postgre_repo:PostgreRepo):
        self.conf = taxi_conf
        #initiate repo and transform 
        self.taxi_ds = TaxiDS(taxi_conf['url'], taxi_conf['headers'])
        self.taxi_tr = Taxi_Tr()
        self.postgre_repo = postgre_repo
      
    def load_data_postgre(self, data, dim):
        try: 
            # Removing data before loading 
            del_query = Template(del_query_1_key)
            param ={
                "table_name": "",
                "key_col": "timestamp",
                "key_val": ""
            }
            for ts in dim['timestamp']:
                # delete dim table
                param["table_name"], param["key_val"]= self.conf['dim_table'], ts
                query = del_query.render(param = param)

                self.postgre_repo.exec_query(query)
                # delete data table 
                param["table_name"], param["key_val"]= self.conf['data_table'], ts
                query = del_query.render(param = param)
                self.postgre_repo.exec_query(query=query)
            
            # Load data
            self.postgre_repo.load_to_db(dim,self.conf['dim_table'])
            self.postgre_repo.load_to_db(data,self.conf['data_table'])
        except Exception as error:
            raise(f"error in load_data_postgre : {error}")
        
    def backfill_data_date(self, start_date:date, end_date:date, seconds_interval:int):
        try:
            bulk_data = self.taxi_ds.get_bulk_data(start_date=start_date, end_date=end_date,seconds_interval=seconds_interval)
            print(len(bulk_data))
            data, dim = self.taxi_tr.process_bulk_messages(bulk_data)
            self.load_data_postgre(data,dim)
        except Exception as error:
            raise(f"error in backfill_data_date : {error}")
        # finally:
        #     del data, dim
    
    def get_current_data(self):
        try:
            data, dim = self.taxi_tr.process_message(self.taxi_ds.get_current_data())
            self.load_data_postgre(data,dim)
        except Exception as error:
            raise(f"error in get_current_data : {error}")
        # finally:
        #     del data, dim
   
    def get_all_region(self, target_table:str):
        try:
            region_data = self.taxi_tr.get_all_region()
            self.postgre_repo.update_db(region_data,target_table)
        except Exception as error:
            raise(f"error in get_all_region : {error}")

    def get_region_no_taxi(self):
        self.get_current_data()
        self.get_all_region(self.conf["region_table"])
        return(self.postgre_repo.exec_query_pd(no_taxi_region))
    
    def finish(self):
        self.postgre_repo.close()