# pip install tardis-dev
# requires Python >=3.6
import os
import time
from tardis_dev import datasets, get_exchange_details
from datetime import datetime, timedelta
from utils import ensure_dir
from multiprocessing import Pool
from functools import partial

# class CSVParser:
#     def parse(self, filepath):
#         with open(filepath, "r") as fd:
#             header = fd.readline()
#             for line in fd.readlines():
#                 pass

"""Download """
class CSVDownloader:
    def __init__(self, api_key, db_uri, logger):
        self.api_key = api_key
        self.db_uri = db_uri
        self.logger = logger
        self.DOWNLOAD_FOLDER = "/tt_data/tardis_datasets"

    """multiprocess download all the symbols' all types of available datasets for all exchanges in the list for a given day
       args: exchanges: []
       return: none
    """
    def download_all_datasets_for_exchanges(self, exchanges, from_date, to_date):
        # Use functools.partial to create a picklable function that binds self to the instance method
        download_func = partial(self.download_all_datasets_for_an_exchange, from_date=from_date, to_date=to_date)
        if len(exchanges) > 0:
            if len(exchanges) == 1:
                self.download_all_datasets_for_an_exchange(exchanges[0], from_date, to_date)
            else:
                num_processes = len(exchanges)
                with Pool(num_processes) as pool:
                    pool.map(download_func, exchanges)
    
    """This function downloads all the symbols' all types of available datasets traded in a specified exchange for a given day"""
    def download_all_datasets_for_an_exchange(self, exchange_to_download, from_date, to_date):
        print("start download {} from {} to {} ".format(exchange_to_download, from_date, to_date))
        # user specified variables
        # exchange_to_download = "binance-futures"
        expected_from_date = from_date # "2023-07-30"
        expected_to_date = to_date # excluded
        # Specify the path of the directory you want to create
        directory_path = os.path.join(self.DOWNLOAD_FOLDER, exchange_to_download)

        start_download_flag = True
        # undownloaded_pair_due_to_time = []
        # loop through all the symbols in the exchange to download
        start_time = time.time()
        for pair in self.get_all_symbols_for_datasets(exchange_to_download):
            # if there is no overlap between the time window and the original pair trading time window, skip this pair and record
            if (datetime.strptime(expected_to_date, "%Y-%m-%d") <= datetime.strptime(pair['availableSince'][0:10], "%Y-%m-%d") or datetime.strptime(expected_from_date, "%Y-%m-%d") >= datetime.strptime(pair['availableTo'][0:10], "%Y-%m-%d")):
                # undownloaded_pair_due_to_time.append(pair)
                continue
            if start_download_flag:  
                # make a subfolder for this trade pair
                ensure_dir(os.path.join(directory_path, pair['id']))
                for dt in pair["dataTypes"]:
                    most_inner_path = os.path.join(directory_path, pair['id'], dt)
                    ensure_dir(most_inner_path)
                    try:
                        datasets.download(
                        exchange=exchange_to_download,
                        data_types=[dt],
                        from_date=self.get_later_date(expected_from_date, pair['availableSince'][0:10] ),
                        to_date=self.get_older_date(expected_to_date , pair['availableTo'][0:10]) ,
                        symbols=[pair['id']], # specify the instruments being traded
                        api_key=self.api_key, 
                        download_dir=most_inner_path  # Pass the output directory parameter
                        )
                        # 
                    except Exception as e:
                        print(f"Download failed: {e}")
                        self.logger.error("Download %s failed for symbol: %s in exchange: %s from_date: %s to_date: %s; error: %s" % (dt, pair['id'], exchange_to_download, from_date, to_date, str(e)))
                # self.record_finished_download_to_db(exchange_to_download, pair, from_date, to_date)
                self.logger.info("All datasets download finished for symbol: %s in exchange: %s from %s to %s" % 
                (pair['id'], exchange_to_download, from_date, to_date))
        end_time = time.time()
        self.logger.info("# Download finished for exchange: {} - time consumed: {} ".format(exchange_to_download, end_time-start_time))
        # datasets.download(
        #     exchange=exchange,
        #     data_types=[
        #         "incremental_book_L2",
        #     ],
        #     from_date=self.get_prev_day(day),
        #     to_date=day,
        #     symbols=[symbol],
        #     api_key=self.api_key,
        #     download_dir=download_dir,
        # )

    def download_all_datasets_for_some_symbols(self, exchange_to_download, from_date, to_date, symbols):
        print("start download {} from {} to {} ".format(exchange_to_download, from_date, to_date))
        # user specified variables
        # exchange_to_download = "binance-futures"
        expected_from_date = from_date # "2023-07-30"
        expected_to_date = to_date # excluded
        # Specify the path of the directory you want to create
        directory_path = os.path.join(self.DOWNLOAD_FOLDER, exchange_to_download)

        # undownloaded_pair_due_to_time = []
        # loop through all the symbols in the exchange to download
        start_time = time.time()
        for pair in self.get_all_symbols_for_datasets(exchange_to_download):
            # if there is no overlap between the time window and the original pair trading time window, skip this pair and record
            if (datetime.strptime(expected_to_date, "%Y-%m-%d") <= datetime.strptime(pair['availableSince'][0:10], "%Y-%m-%d") or datetime.strptime(expected_from_date, "%Y-%m-%d") >= datetime.strptime(pair['availableTo'][0:10], "%Y-%m-%d")):
                # undownloaded_pair_due_to_time.append(pair)
                continue
            if pair['id'] in symbols:  
                # make a subfolder for this trade pair
                ensure_dir(os.path.join(directory_path, pair['id']))
                for dt in pair["dataTypes"]:
                    self.logger.info("start download %s dataset: %s" % (pair['id'], dt))
                    most_inner_path = os.path.join(directory_path, pair['id'], dt)
                    ensure_dir(most_inner_path)
                    try:
                        datasets.download(
                        exchange=exchange_to_download,
                        data_types=[dt],
                        from_date=self.get_later_date(expected_from_date, pair['availableSince'][0:10] ),
                        to_date=self.get_older_date(expected_to_date , pair['availableTo'][0:10]) ,
                        symbols=[pair['id']], # specify the instruments being traded
                        api_key=self.api_key, 
                        download_dir=most_inner_path  # Pass the output directory parameter
                        )
                        # 
                    except Exception as e:
                        print(f"Download failed: {e}")
                        self.logger.error("Download %s failed for symbol: %s in exchange: %s from_date: %s to_date: %s; error: %s" % (dt, pair['id'], exchange_to_download, from_date, to_date, str(e)))
                    self.logger.info("symbol:%s dataset: %s downloaded." % (pair['id'], dt))
                # self.record_finished_download_to_db(exchange_to_download, pair, from_date, to_date)
                self.logger.info("All datasets download finished for symbol: %s in exchange: %s from %s to %s" % 
                (pair['id'], exchange_to_download, from_date, to_date))
        end_time = time.time()
        self.logger.info("# Download finished for exchange: {} - time consumed: {} ".format(exchange_to_download, end_time-start_time))

    """this functions allow users to customize the download by providing list of symbols and datatypes"""
    def download_datasets_customizable(self, exchange_to_download, from_date, to_date, symbols, dataset_types):
        print("start download {} from {} to {} ".format(exchange_to_download, from_date, to_date))
        # user specified variables
        # exchange_to_download = "binance-futures"
        expected_from_date = from_date # "2023-07-30"
        expected_to_date = to_date # excluded
        # Specify the path of the directory you want to create
        directory_path = os.path.join(self.DOWNLOAD_FOLDER, exchange_to_download)

        # undownloaded_pair_due_to_time = []
        # loop through all the symbols in the exchange to download
        start_time = time.time()
        for pair in self.get_all_symbols_for_datasets(exchange_to_download):
            # if there is no overlap between the time window and the original pair trading time window, skip this pair and record
            if (datetime.strptime(expected_to_date, "%Y-%m-%d") <= datetime.strptime(pair['availableSince'][0:10], "%Y-%m-%d") or datetime.strptime(expected_from_date, "%Y-%m-%d") >= datetime.strptime(pair['availableTo'][0:10], "%Y-%m-%d")):
                # undownloaded_pair_due_to_time.append(pair)
                continue
            if pair['id'] in symbols:  
                # make a subfolder for this trade pair
                ensure_dir(os.path.join(directory_path, pair['id']))
                for dt in pair["dataTypes"]:
                    if dt in dataset_types:
                        self.logger.info("start download %s dataset: %s" % (pair['id'], dt))
                        most_inner_path = os.path.join(directory_path, pair['id'], dt)
                        ensure_dir(most_inner_path)
                        try:
                            datasets.download(
                            exchange=exchange_to_download,
                            data_types=[dt],
                            from_date=self.get_later_date(expected_from_date, pair['availableSince'][0:10] ),
                            to_date=self.get_older_date(expected_to_date , pair['availableTo'][0:10]) ,
                            symbols=[pair['id']], # specify the instruments being traded
                            api_key=self.api_key, 
                            download_dir=most_inner_path  # Pass the output directory parameter
                            )
                            # 
                        except Exception as e:
                            print(f"Download failed: {e}")
                            self.logger.error("Download %s failed for symbol: %s in exchange: %s from_date: %s to_date: %s; error: %s" % (dt, pair['id'], exchange_to_download, from_date, to_date, str(e)))
                        self.logger.info("symbol:%s dataset: %s downloaded." % (pair['id'], dt))
                    # self.record_finished_download_to_db(exchange_to_download, pair, from_date, to_date)
                # self.logger.info("All datasets download finished for symbol: %s in exchange: %s from %s to %s" % 
                # (pair['id'], exchange_to_download, from_date, to_date))
        end_time = time.time()
        self.logger.info("# Download finished for exchange: {} - time consumed: {} ".format(exchange_to_download, end_time-start_time))

    def download_specific_datasets_for_all_pairs(self, exchange_to_download, from_date, to_date, dataset_types):
        print("start download all {} for exchange- {} from {} to {} ".format(dataset_types, exchange_to_download, from_date, to_date))
        # user specified variables
        # exchange_to_download = "binance-futures"
        expected_from_date = from_date # "2023-07-30"
        expected_to_date = to_date # excluded
        # Specify the path of the directory you want to create
        directory_path = os.path.join(self.DOWNLOAD_FOLDER, exchange_to_download)

        # undownloaded_pair_due_to_time = []
        # loop through all the symbols in the exchange to download
        start_time = time.time()
        for pair in self.get_all_symbols_for_datasets(exchange_to_download):
            # if there is no overlap between the time window and the original pair trading time window, skip this pair and record
            if (datetime.strptime(expected_to_date, "%Y-%m-%d") <= datetime.strptime(pair['availableSince'][0:10], "%Y-%m-%d") or datetime.strptime(expected_from_date, "%Y-%m-%d") >= datetime.strptime(pair['availableTo'][0:10], "%Y-%m-%d")):
                # undownloaded_pair_due_to_time.append(pair)
                continue
            # make a subfolder for this trade pair
            ensure_dir(os.path.join(directory_path, pair['id']))
            for dt in pair["dataTypes"]:
                if dt in dataset_types:
                    self.logger.info("start download %s dataset: %s" % (pair['id'], dt))
                    most_inner_path = os.path.join(directory_path, pair['id'], dt)
                    ensure_dir(most_inner_path)
                    try:
                        datasets.download(
                        exchange=exchange_to_download,
                        data_types=[dt],
                        from_date=self.get_later_date(expected_from_date, pair['availableSince'][0:10] ),
                        to_date=self.get_older_date(expected_to_date , pair['availableTo'][0:10]) ,
                        symbols=[pair['id']], # specify the instruments being traded
                        api_key=self.api_key, 
                        download_dir=most_inner_path  # Pass the output directory parameter
                        )
                        self.logger.info("All %s download finished for symbol: %s in exchange: %s from %s to %s" % (dt, pair['id'], exchange_to_download, from_date, to_date))
                        # 
                    except Exception as e:
                        print(f"Download failed: {e}")
                        self.logger.error("Download %s failed for symbol: %s in exchange: %s from_date: %s to_date: %s; error: %s" % (dt, pair['id'], exchange_to_download, from_date, to_date, str(e)))

        end_time = time.time()
        self.logger.info("# Download finished for exchange: {} - time consumed: {} ".format(exchange_to_download, end_time-start_time))

    
    def record_finished_download_to_db(self, exchange, symbol, day):
        task_id = "%s-%s-%s" % (exchange, symbol, day)
        init_task = {
            "id": task_id,
            "symbol": symbol,
            "exchange": exchange,
            "day": day,
            "downloader_status": "success",
            "generate_binary_status": "init"
        }
        # with get_db_instance(self.db_uri) as db:
        #     db.tardis_tasks.insert_one(init_task)
        return "success"
    
    def get_next_day(self, day):
        date = datetime.strptime(day, "%Y-%m-%d")
        next_day = date + timedelta(days=1)
        return next_day.strftime("%Y-%m-%d")
    
    def get_prev_n_day(self, day, n):
        date = datetime.strptime(day, "%Y-%m-%d")
        prev_n_day = date - timedelta(days=n)
        return prev_n_day.strftime("%Y-%m-%d")

    """ A helper function to call the tardis API to get the available symbols and their info for a specified exchange
    This helper is called to make sure a required symbol/datasetType/dates are valid when calling tardis.datasets.download()
    Return: a list of dicts with keys = dict_keys(['id', 'type', 'dataTypes', 'availableSince', 'availableTo', 'stats'])
    example:
        {'id': 'PERPETUALS', 'type': 'perpetual', 'dataTypes': ['trades', 'derivative_ticker', 'liquidations'], 'availableSince': '2019-03-30T00:00:00.000Z', 'availableTo': '2023-07-17T00:00:00.000Z', 'stats': {'trades': 10042302746, 'bookChanges': 35168537560}}
    """
    def get_all_symbols_for_datasets(self, exchange):
        exchange_details = get_exchange_details(exchange)
        # exchange_details.key = dict_keys(['id', 'name', 'enabled', 'availableSince', 'availableChannels', 'availableSymbols', 'datasets', 'incidentReports'])
        datasets_info = exchange_details['datasets']
        # datasets_info.dict_keys(['formats', 'exportedFrom', 'exportedUntil', 'stats', 'symbols'])
        available_symbols_for_datasets = datasets_info['symbols'] # list of dicts
        return available_symbols_for_datasets

    # helper functions for date strings comparison:
    def get_older_date(self, date1, date2):
        # Parse the date strings into datetime objects
        dt1 = datetime.strptime(date1, '%Y-%m-%d')
        dt2 = datetime.strptime(date2, '%Y-%m-%d')
        # Compare the dates and get the older one
        older_date = min(dt1, dt2)
        # Convert the older_date back to a string if needed
        older_date_string = older_date.strftime('%Y-%m-%d')
        return older_date_string

    def get_later_date(self, date1, date2):
        # Parse the date strings into datetime objects
        dt1 = datetime.strptime(date1, '%Y-%m-%d')
        dt2 = datetime.strptime(date2, '%Y-%m-%d')
        # Compare the dates and get the older one
        older_date = max(dt1, dt2)
        # Convert the older_date back to a string if needed
        older_date_string = older_date.strftime('%Y-%m-%d')
        return older_date_string
