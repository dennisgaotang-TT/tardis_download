import multiprocessing
from datetime import datetime, timedelta
from multiprocessing import Pool
from tardis.downloader import CSVDownloader
from utils import get_config
from logger import Logger

exchanges = ["binance-futures", "binance-delivery"]
from_date = '2019-01-01'
to_date = '2022-03-06'

job_name = "binance_all_20190101_20220306" 
logger = Logger(job_name, './logs/%s.log' % job_name)
cfg = get_config("./configs/tardis/downloader.json")
d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)
d.download_all_datasets_for_exchanges(exchanges, from_date, to_date)