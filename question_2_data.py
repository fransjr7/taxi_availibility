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

# Get all region with no available taxi
# Alert can be served by sending email or served as API
region_no_taxi = taxi_usecase.get_region_no_taxi()
print(region_no_taxi)