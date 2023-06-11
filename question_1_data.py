from usecase.taxi_usecase import TaxiUsecase
from repository.postgre_repo import PostgreRepo
from config.config import Config 
from datetime import datetime, date , timedelta
from jinja2 import Template
from lib.query import *
import warnings
warnings.filterwarnings('ignore')


# Initiate config, repository and usecase
cfg = Config().conf
pg_repo = PostgreRepo(cfg["postgre"])
taxi_usecase = TaxiUsecase(cfg["taxi_config"],pg_repo)


# Question 1 - Point A  top 10 highest taxi population region
# Get latest data to postgre
taxi_usecase.get_current_data()
# query latest data to get result
top_10_region = pg_repo.exec_query_pd(top_10_area)
print(top_10_region)

# Question 1 - Point B  top 10 lowest taxi population region
# Get all region in sg
taxi_usecase.get_all_region(cfg["taxi_config"]["region_table"])
# query latest data to get result
taxi_usecase.get_current_data()
bottom_10_region = pg_repo.exec_query_pd(bottom_10_area)
print(bottom_10_region)

# Question 1 - Point C  Hourly taxi availability
c_query = Template(hourly_taxi_avail)
c_query = c_query.render(date = '2023-06-09')
hourly_taxi = pg_repo.exec_query_pd(c_query)
print(hourly_taxi)