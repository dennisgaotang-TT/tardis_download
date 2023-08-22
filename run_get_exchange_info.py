from multiprocessing import Pool
from tardis.downloader import CSVDownloader
from utils import get_config
from logger import Logger

exchange = "binance-futures"

job_name = "binance_all_20190101_20220306" 
logger = Logger(job_name, './logs/%s.log' % job_name)
cfg = get_config("./configs/tardis/downloader.json")
d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)

dic_of_info = d.get_all_symbols_for_datasets(exchange)
print("There are %s number of pairs in exchange '%s':" % (len(dic_of_info), exchange))
for pair in dic_of_info:
    print(pair)