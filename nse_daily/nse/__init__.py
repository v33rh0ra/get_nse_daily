# get nse daily bhav
# https://www1.nseindia.com/content/historical/EQUITIES/2020/JUN/cm12JUN2020bhav.csv.zip
from datetime import datetime, timedelta
from time import sleep
from typing import Optional
import requests
import os
from pathlib import Path
from fake_useragent import UserAgent
from numpy import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
from multiprocessing.pool import ThreadPool
from nse_daily.common import _get_exception, _errorify


class NSEDaily(object):
    """
    NSE Daily
    """

    def __init__(self,
                 default_date_pattern: Optional[str] = '%Y%m%d',
                 file_pattern: Optional[str] = "cm{date_part}bhav.csv.zip",
                 file_date_part_format: Optional[str] = '%d%b%Y',
                 uri_pattern: Optional[
                     str] = "https://archives.nseindia.com/content/historical/EQUITIES/{yyyy_mon}/{file_name}",
                 uri_yy_mm_format: Optional[str] = '%Y/%b',
                 download_path: Optional[str] = None
                 ):
        """
        NSE Daily bhav copy can be downloaded from the following
            URI: https://www1.nseindia.com/content/historical/EQUITIES/2020/JUN/cm12JUN2020bhav.csv.zip
            NEW WEBSITE URL
           URI: https://archives.nseindia.com/content/historical/EQUITIES/2023/MAY/cm11MAY2023bhav.csv.zip
        Parameters are set by default to match the above URI.
        If the URI Changes, please change the parameters below, to avoid breaking of code.
        :param default_date_pattern: The default input date pattern to be used for parsing dates passed to the functions
        :param file_pattern: The pattern of the BSE Daily bhav file, i.e. for cm12JUN2020bhav.csv.zip pass in cm{date_part}bhav.csv.zip
        :param file_date_part_format: The date format of the date part in the NSE Daily bhav file pattern i.e. for 130122 pass in %d%m%y
        :param uri_pattern: The uri from where the BSE Daily bhav copy needs to be downloaded, default=https://www1.nseindia.com/content/historical/EQUITIES/{yyyy_mon}/{file_name}
        :param uri_yy_mm_format: The date part format in the uri , i.e. for 2020/JUN the default has been set to '%Y/%b'
        :param download_path: The local filesystem path where the NSE Daily bhav copy will be downloaded
        """
        self.default_date_pattern = default_date_pattern
        self.nse_file_pattern = file_pattern
        self.nse_file_date_part_format = file_date_part_format
        self.nse_uri_pattern = uri_pattern
        self.nse_uri_yy_mm_format = uri_yy_mm_format
        self.download_path = download_path
        if self.download_path is None or str(self.download_path).strip() == '':
            appdir = str(Path.cwd())
            self.download_path = os.path.join(appdir, 'downloads')
        self._create_session()

    def _create_session(self):
        self.session = requests.Session()
        retry = Retry(total=5,
                      read=5,
                      connect=5,
                      status_forcelist=(500, 502, 504),
                      method_whitelist=frozenset(['GET', 'POST']),
                      backoff_factor=1
                      )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        ua = UserAgent()
        self.session.headers.update({"User-Agent": str(ua.chrome)})

    def _check_reponse(self, response: requests.Response):
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            print('HTTP Error %s', response.reason)
            print(response.text)
            return False

    def _download_by_date(self, file_date: datetime):
        """
        Internal function to request the download for a single date. Function has a random uniform distribution sleep
        time between 1 to 3 seconds to avoid getting blocked during multiple concurrent requests. Function also
        checks and skips the download if date is a weekend.
        :param file_date: The date for which the download is being requested
        :return: (file_date, download_file_path)
        """
        ################################################################################
        # Adding random sleep time to avoid being blocked for multiple requests
        sleep(random.uniform(1, 3))
        #########################################################################
        daynum = file_date.weekday()
        if daynum >= 5:
            print("{} is weekend, file skipped".format(file_date.strftime(self.default_date_pattern)))
            return file_date, None
        file_date_str = file_date.strftime(self.nse_file_date_part_format).upper()
        nse_yy_mm = file_date.strftime(self.nse_uri_yy_mm_format).upper()
        nse_file_name = self.nse_file_pattern.format(date_part=file_date_str)
        nse_uri = self.nse_uri_pattern.format(yyyy_mon=nse_yy_mm, file_name=nse_file_name)
        nse_download_file_path = os.path.join(self.download_path, nse_file_name)
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        print(nse_uri)
        response = self.session.request(method='GET', url=nse_uri, allow_redirects=True)
        # r = requests.get(nse_uri, allow_redirects=True)
        status = self._check_reponse(response)
        if not status:
            return file_date, None
        content_type = response.headers.get('content-type')
        if content_type in ['application/zip', 'application/x-zip-compressed', 'application/x-7z-compressed',
                            'text/csv', 'application/gzip', 'application/x-tar', 'text/plain']:
            with open(nse_download_file_path, 'wb') as nse_file:
                nse_file.write(response.content)
            print("{} download complete".format(nse_file_name))
        else:
            e = _errorify("INVALID_CONTENT_TYPE", f"content-type {content_type} being returned is not supported..")
            raise Exception(e)
        return file_date, nse_download_file_path

    def download_by_date(self, date_str, date_format: Optional[str] = '%Y%m%d'):
        """
        Function to download the NSE Daily bhav copy for a date
        :param date_str: Input date string i.e. '20210105' for 5th Jab 2021
        :param date_format: The date format of the input date string, default = '%Y%m%d'
        :return:
        """
        try:
            file_date = datetime.strptime(date_str, date_format)
            return self._download_by_date(file_date)
        except:
            e = _get_exception()
            raise Exception(e)

    def download_by_date_range(self, date_start: str, date_end: str, date_format: Optional[str] = '%Y%m%d',
                               num_workers: Optional[int] = 1):
        """
        Function to download multiple Daily NSE bhav copies for a date range
        :param date_start: The start date of the date range
        :param date_end: The end date of the date range
        :param date_format: The format of the input dates, default='%Y%m%d'
        :param num_workers: The number of workers to be utilized to get the files, default=1
        :return:
        """
        try:
            start = datetime.strptime(date_start, date_format)
            end = datetime.strptime(date_end, date_format)
            l_dates = [start + timedelta(days=x) for x in range(0, (end - start).days)]
            tpool = ThreadPool(processes=num_workers)
            l_e = tpool.map(self._download_by_date, l_dates)
            return l_e
        except:
            e = _get_exception()
            raise Exception(e)
            
            
            
            
# getting data for full bhavcopy and security deliverable data
class nsedelivery(object):
    """
    NSE Delivery
    """

    def __init__(self,
                 default_date_pattern: Optional[str] = '%Y%m%d',
                 file_pattern: Optional[str] = "sec_bhavdata_full_{date_part}.csv",
                 file_date_part_format: Optional[str] = '%d%m%Y',
                 uri_pattern: Optional[
                     str] = "https://archives.nseindia.com/products/content/{file_name}",
                 download_path: Optional[str] = None
                 ):
        """
        NSE Daily bhav copy can be downloaded from the following
            URI:https://archives.nseindia.com/products/content/sec_bhavdata_full_11052023.csv

        Parameters are set by default to match the above URI.
        If the URI Changes, please change the parameters below, to avoid breaking of code.
        :param default_date_pattern: The default input date pattern to be used for parsing dates passed to the functions
        :param file_pattern: The pattern of the BSE Daily bhav file, i.e. for cm12JUN2020bhav.csv.zip pass in cm{date_part}bhav.csv.zip
        :param file_date_part_format: The date format of the date part in the NSE Daily bhav file pattern i.e. for 130122 pass in %d%m%y
        :param uri_pattern: The uri from where the BSE Daily bhav copy needs to be downloaded, default=https://www1.nseindia.com/content/historical/EQUITIES/{yyyy_mon}/{file_name}
        :param uri_yy_mm_format: The date part format in the uri , i.e. for 2020/JUN the default has been set to '%Y/%b'
        :param download_path: The local filesystem path where the NSE Daily bhav copy will be downloaded
        """
        self.default_date_pattern = default_date_pattern
        self.nse_file_pattern = file_pattern
        self.nse_file_date_part_format = file_date_part_format
        self.nse_uri_pattern = uri_pattern
        #self.nse_uri_yy_mm_format = uri_yy_mm_format
        self.download_path = download_path
        if self.download_path is None or str(self.download_path).strip() == '':
            appdir = str(Path.cwd())
            self.download_path = os.path.join(appdir, 'downloads')
        self._create_session()

    def _create_session(self):
        self.session = requests.Session()
        retry = Retry(total=5,
                      read=5,
                      connect=5,
                      status_forcelist=(500, 502, 504),
                      method_whitelist=frozenset(['GET', 'POST']),
                      backoff_factor=1
                      )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        ua = UserAgent()
        self.session.headers.update({"User-Agent": str(ua.chrome)})

    def _check_reponse(self, response: requests.Response):
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            print('HTTP Error %s', response.reason)
            print(response.text)
            return False

    def _download_by_date(self, file_date: datetime):
        """
        Internal function to request the download for a single date. Function has a random uniform distribution sleep
        time between 1 to 3 seconds to avoid getting blocked during multiple concurrent requests. Function also
        checks and skips the download if date is a weekend.
        :param file_date: The date for which the download is being requested
        :return: (file_date, download_file_path)
        """
        ################################################################################
        # Adding random sleep time to avoid being blocked for multiple requests
        sleep(random.uniform(1, 3))
        #########################################################################
        daynum = file_date.weekday()
        if daynum >= 5:
            print("{} is weekend, file skipped".format(file_date.strftime(self.default_date_pattern)))
            return file_date, None
        file_date_str = file_date.strftime(self.nse_file_date_part_format).upper()
        nse_yy_mm = file_date.strftime(self.nse_uri_yy_mm_format).upper()
        nse_file_name = self.nse_file_pattern.format(date_part=file_date_str)
        nse_uri = self.nse_uri_pattern.format(file_name=nse_file_name)
        nse_download_file_path = os.path.join(self.download_path, nse_file_name)
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        print(nse_uri)
        response = self.session.request(method='GET', url=nse_uri, allow_redirects=True)
        # r = requests.get(nse_uri, allow_redirects=True)
        status = self._check_reponse(response)
        if not status:
            return file_date, None
        content_type = response.headers.get('content-type')
        if content_type in ['application/zip', 'application/x-zip-compressed', 'application/x-7z-compressed',
                            'text/csv', 'application/gzip', 'application/x-tar', 'text/plain']:
            with open(nse_download_file_path, 'wb') as nse_file:
                nse_file.write(response.content)
            print("{} download complete".format(nse_file_name))
        else:
            e = _errorify("INVALID_CONTENT_TYPE", f"content-type {content_type} being returned is not supported..")
            raise Exception(e)
        return file_date, nse_download_file_path

    def download_by_date(self, date_str, date_format: Optional[str] = '%Y%m%d'):
        """
        Function to download the NSE Daily bhav copy for a date
        :param date_str: Input date string i.e. '20210105' for 5th Jab 2021
        :param date_format: The date format of the input date string, default = '%Y%m%d'
        :return:
        """
        try:
            file_date = datetime.strptime(date_str, date_format)
            return self._download_by_date(file_date)
        except:
            e = _get_exception()
            raise Exception(e)

    def download_by_date_range(self, date_start: str, date_end: str, date_format: Optional[str] = '%Y%m%d',
                               num_workers: Optional[int] = 1):
        """
        Function to download multiple Daily NSE bhav copies for a date range
        :param date_start: The start date of the date range
        :param date_end: The end date of the date range
        :param date_format: The format of the input dates, default='%Y%m%d'
        :param num_workers: The number of workers to be utilized to get the files, default=1
        :return:
        """
        try:
            start = datetime.strptime(date_start, date_format)
            end = datetime.strptime(date_end, date_format)
            l_dates = [start + timedelta(days=x) for x in range(0, (end - start).days)]
            tpool = ThreadPool(processes=num_workers)
            l_e = tpool.map(self._download_by_date, l_dates)
            return l_e
        except:
            e = _get_exception()
            raise Exception(e)
