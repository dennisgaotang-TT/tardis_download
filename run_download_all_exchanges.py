import multiprocessing
from datetime import datetime, timedelta
from multiprocessing import Pool
from tardis.downloader import CSVDownloader
from utils import get_config
from logger import Logger

print("total cpu count: ", multiprocessing.cpu_count())

def get_prev_n_day(day, n):
        date = datetime.strptime(day, "%Y-%m-%d")
        prev_n_day = date - timedelta(days=n)
        return prev_n_day.strftime("%Y-%m-%d")


# get previous N intervals before end day
def get_intervals(end_day, until_prev_day, n):
    days = []
    while True:
            prev_day = get_prev_n_day(end_day, n)
            days.append((prev_day, end_day))
            end_day = prev_day
            if end_day < until_prev_day:
                    break
    return days

def process(args):
    exchange, from_date, to_date = args
    job_name = "download_%s_%s_%s" % (exchange, from_date, to_date)
    logger = Logger(job_name, './logs/%s.log' % job_name)
    cfg = get_config("./configs/tardis/downloader.json")
    d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)
    d.download_all_datasets_for_an_exchange(exchange, from_date, to_date)
    return None

def download_all_by_days(from_date, to_date):
    job_name = "download_all_%s_%s" % (from_date, to_date)
    logger = Logger(job_name, './logs/%s.log' % job_name)
    cfg = get_config("./configs/tardis/downloader.json")
    d = CSVDownloader(cfg["api_key"], cfg["db_uri"], logger)
    exchanges = ["binance-futures", "binance-delivery", "okex-futures", "okex-swap", "gate-io-futures", "bybit"]
    d.download_all_datasets_for_exchanges(exchanges, from_date, to_date)


def main():

    # result = get_intervals("2023-03-06", "2022-03-06", 3)
    # for from_date, to_date in result:
    #     download_all_by_days(from_date, to_date)

    # # tmux session 1
    # prev_2nd_year = get_intervals("2022-03-06", "2021-03-06", 90)
    # for from_date, to_date in prev_2nd_year:
    #     download_all_by_days(from_date, to_date)

    # prev 3rd year
    prev_3nd_year = get_intervals("2021-03-06", "2020-03-06", 1)
    for from_date, to_date in prev_3nd_year:
        download_all_by_days(from_date, to_date)

    # params = []
    # exchanges = ["binance-futures"]
    # for e in exchanges:
    #     params += [(e, start, end) for start, end in result]

    # num_processes = len(params)
    # with Pool(num_processes) as p:
    #     p.map(process, params)

if __name__ == "__main__":
    main()