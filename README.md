
This repo contains python scripts to download datasets from tardis.dev.

## Before download:
Before download, user needs to know which [supported exchange](https://docs.tardis.dev/faq/general#which-exchanges-instruments-and-currency-pairs-are-supported) to download.
After figuring out which exchange and since when this exchange is available on Tardis, you need to know the following two items: trading symbol/pair, supported datatypes  
### 1. run_get_exchange_info.py: 
* edit the exchange parameter and run "python3 run_get_exchange_info.py >> ./exchange_info/<YOUR_EXCHANGE>.txt"
* you will get the information of this exchange which contains types of pair/symbols traded in this exchange, their available dataset types and available time period

## Dependencies: 
* "pip install tardis-dev"
* ./configs/tardis/downloader.json

## CSVDownloader class in ./tardis/downloader.py
This class mainly contains wrapper instance functions of tardis.datasets API to download .csv.gz datasets according to user needs. It takes care of the error raised by inputing out-of-range time period.
**User can specified an out of range [from_date, to_date], but the downloader will only download the overlapped time window with the pair's [availableSince, availableTo] without causing exception.**
Users need to first instanciate a CSVDownloader instance and then call the according functions. The destination folder of the download files is specified in the constructor of the class.

* use case 1: **download all datasets of all pairs for specified: several exchanges and timeperiod**:
  download_all_datasets_for_exchanges(self, exchanges, from_date, to_date)
  
* use case 2: **download all datasets of all pairs for specified: one exchange and timeperiod**:
  download_all_datasets_for_an_exchange(self, exchange_to_download, from_date, to_date)
  
* use case 3: **download all datasets for specified: some pair/symbols, one exchange and timeperiod**:
  download_all_datasets_for_some_symbols(self, exchange_to_download, from_date, to_date, symbols)
  
* use case 4: **download datasets with specified: some pair/symbols, some dataset_types, one exchange, and timeperiod**:
  download_datasets_customizable(self, exchange_to_download, from_date, to_date, symbols, dataset_types)
  
## Example scripts to download datasets by needs
Depending on your use case, you need to modified the script to specified the parameters and the log files.

### 1. run_download_all_exchanges.py: 
this scipt serves as an example of the above use case 1. 
It provides flexibility to download a long period of datasets by smaller truncated intervals. For example, you may want to download on a 3-day interval basis for a whole years data.
run by Command: "python3 run_download_all_exchanges.py"
### 2. run_download_binance.py: 
A simple scripts to download all the data for an exchange

## Observation:
1. you can repeatedly run a download script for multiple times and the program will not download the existing files again.
2. Since the datasets is very large, creating multiple processes might not help with the download speed as it is bounded by the internet bandwidth.
