import multiprocessing
from datetime import datetime, timedelta
from multiprocessing import Pool
from tardis.downloader import CSVDownloader
from utils import get_config
from logger import Logger

print("total cpu count: ", multiprocessing.cpu_count())


def download_by_symbols(exchange, from_date, to_date, symbols, dataset_types):
    job_name = "download_derivativeTicker_%s_%s_%s" % (exchange, from_date, to_date)
    logger = Logger(job_name, './logs/%s.log' % job_name)
    cfg = get_config("./configs/tardis/downloader.json")
    d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)
    d.download_datasets_customizable(exchange, from_date, to_date, symbols, dataset_types)


def main():
    # exchange = "binance-delivery"
    # symbols = ['BTCUSD_PERP', 'BTCUSD_230929', 'BTCUSD_231229', 'BTCUSD_230630', 'BTCUSD_230331', 'BTCUSD_221230', 'BTCUSD_220930', 'BTCUSD_220624', 'BTCUSD_220325','BTCUSD_211231', 'BTCUSD_210924','BTCUSD_210625','BTCUSD_210326','BTCUSD_201225','BTCUSD_200925', 
    # 'ETHUSD_PERP', 'ETHUSD_230929', 'ETHUSD_231229', 'ETHUSD_230630', 'ETHUSD_230331', 'ETHUSD_221230','ETHUSD_220930', 'ETHUSD_220624', 'ETHUSD_220325','ETHUSD_211231','ETHUSD_210924','ETHUSD_210625','ETHUSD_210326','ETHUSD_201225','ETHUSD_200925']
    # dataset_types = ['derivative_ticker']
    # download_by_symbols(exchange, '2019-01-01', '2023-08-16', symbols, dataset_types)

    # exchange = "binance-futures"
    # symbols = ['BTCUSDT', 'BTCUSDT_230929', 'BTCUSDT_230630', 'BTCUSDT_230331', 'BTCUSDT_221230', 'BTCUSDT_220930', 'BTCUSDT_220624', 'BTCUSDT_220325', 'BTCUSDT_211231','BTCUSDT_210924', 'BTCUSDT_210625','BTCUSDT_210326','BTCBUSD_210129','BTCBUSD_210226','BTCBUSD', 'BTCDOMUSDT','BTCSTUSDT','ETHBTC', 
    # 'ETHUSDT', 'ETHBUSD', 'ETHUSDT_230929', 'ETHUSDT_230630', 'ETHUSDT_230331', 'ETHUSDT_221230','ETHUSDT_220930', 'ETHUSDT_220624', 'ETHUSDT_220325','ETHUSDT_211231','ETHUSDT_210924','ETHUSDT_210625','ETHUSDT_210326']
    # dataset_types = ['derivative_ticker']
    # download_by_symbols(exchange, '2019-01-01', '2023-08-16', symbols, dataset_types)

    # exchange = "okex-swap"
    # symbols = ['BTC-USD-SWAP','BTC-USDT-SWAP','BTC-USDC-SWAP','ETH-USD-SWAP','ETH-USDT-SWAP','ETHW-USDT-SWAP','ETH-USDC-SWAP']
    # dataset_types = ['derivative_ticker']
    # download_by_symbols(exchange, '2019-01-01', '2023-08-16', symbols, dataset_types)


    exchange = "okex-swap"
    from_date = '2019-01-01'
    to_date = '2023-08-16'
    dataset_types = ['derivative_ticker']

    job_name = "download_derivativeTicker_%s_%s_%s" % (exchange, from_date, to_date)
    logger = Logger(job_name, './logs/%s.log' % job_name)
    cfg = get_config("./configs/tardis/downloader.json")
    d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)
    d.download_specific_datasets_for_all_pairs(exchange, from_date, to_date, dataset_types)

if __name__ == "__main__":
    main()