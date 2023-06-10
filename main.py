from usecase.taxi_usecase import TaxiUsecase
from repository.postgre_repo import PostgreRepo
from config.config import Config 
from datetime import datetime, date , timedelta
import warnings
warnings.filterwarnings('ignore')


cfg = Config().conf
pg_repo = PostgreRepo(cfg["postgre"])
taxi_usecase = TaxiUsecase(cfg["taxi_config"],pg_repo)
# ETL Realtime testing, get current data, can be triggered every minute/30 secors
taxi_usecase.get_current_data()
print("Finish getting current Data !")

# Batch ETL for backfilling data, can backfill multiple date data
today = datetime.today().date() - timedelta(days = 1)
taxi_usecase.backfill_data_date(start_date = today, end_date = today, seconds_interval = 60)
print("Finish getting bulk data !")

taxi_usecase.finish()