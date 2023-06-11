import pandas as pd 
import psycopg2
import os 
from sqlalchemy import create_engine



class PostgreRepo():
    def __init__(self, config):
        # Init postgre connection
        self.config = config
        self.conn = psycopg2.connect(
            host = config["host"],
            database = config["database"],
            user = config["user"],
            password = config["password"])
        self.pg = self.conn.cursor()
        self.db = config["database"]
        self.engine = create_engine(f'postgresql+psycopg2://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}')
    
    def change_db(self, db:str):
        try:
            self.conn.close()
            self.engine.dispose()
            self.conn = psycopg2.connect(
                host = self.config["host"],
                database = db,
                user = self.config["user"],
                password = self.config["password"])
            self.pg = self.conn.cursor()
            self.engine = create_engine(f'postgresql+psycopg2://{self.config["user"]}:{self.config["password"]}@{self.config["host"]}/{db}')
        except Exception as error:
            print("Failed to change database ")
            print(error)

    def exec_query_pd(self,query:str):
        try:
            res = pd.read_sql(query, self.engine)
            return res
        except Exception as error:
            print(f"error in executing query : {error}")

    def exec_query(self, query:str):
        try:
            self.pg.execute(query)
            self.conn.commit()
        except Exception as error:
            print(f"error in executing query : {error}")

    def load_to_db(self, data:pd.DataFrame, target_table:str):
        try:
            data.to_sql(target_table, con=self.engine, if_exists="append",index =False)
        except Exception as error:
            raise(f"error in load into db : {error}")
    
    def update_db(self, data:pd.DataFrame, target_table:str):
        try:
            data.to_sql(target_table, con=self.engine, if_exists="replace",index =False)
        except Exception as error:
            raise(f"error in load into db : {error}")


    def close(self):
        self.conn.close()
        self.engine.dispose()