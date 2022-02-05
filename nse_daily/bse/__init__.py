# get bse daily bhav
# https://www.bseindia.com/download/BhavCopy/Equity/EQ130122_CSV.ZIP
from datetime import datetime, timedelta
from typing import Optional, Any

import requests
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
from numpy import random
from time import sleep
from multiprocessing.pool import ThreadPool

from nse_daily.common import _get_exception, _errorify
from fake_useragent import UserAgent


class BSEDaily(object):
    """
    BSE Daily
    """

    def __init__(self,
                 default_date_pattern: Optional[str] = '%Y%m%d',
                 file_pattern: Optional[str] = "EQ{date_part}_CSV.ZIP",
                 file_date_part_format: Optional[str] = '%d%m%y',
                 uri_pattern: Optional[str] = "https://www.bseindia.com/download/BhavCopy/Equity/{file_name}",
                 download_path: Optional[str] = None
                 ):
        """
        BSE Daily bhav copy can be downloaded from the following
            URI: https://www.bseindia.com/download/BhavCopy/Equity/EQ130122_CSV.ZIP
        Parameters are set by default to match the above URI.
        If the URI Changes, please change the parameters below, to avoid breaking of code parameters.
        :param default_date_pattern: The default input date pattern to be used for parsing dates passed to the functions
        :param file_pattern: The pattern of the BSE Daily bhav file, i.e. for EQ130122_CSV.zip pass in EQ{date_part}_CSV.ZIP
        :param file_date_part_format: The date format of the date part in the DSE Daily bhav file pattern i.e. for 130122 pass in %d%m%y
        :param uri_pattern: The uri from where the BSE Daily bhav copy needs to be downloaded
        :param download_path: The local filesystem path where the BSE Daily bhav copy will be downloaded
        """
        self.default_date_pattern = default_date_pattern
        self.file_pattern = file_pattern
        self.file_date_part_format = file_date_part_format
        self.uri_pattern = uri_pattern
        # self.uri_yy_mm_format = uri_yy_mm_format
        self.download_path = download_path
        if self.download_path is None or str(self.download_path).strip() == '':
            appdir = str(Path.cwd())
            self.download_path = os.path.join(appdir, 'downloads')
        self._create_session()

    def _create_session(self):
        """
        Function to create and set the requests.Session
        :return:
        """
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

    def _download_by_date(self, file_date: datetime) -> (str, Any):
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
        file_date_str = file_date.strftime(self.file_date_part_format).upper()
        # nse_yy_mm = file_date.strftime(self.uri_yy_mm_format).upper()
        file_name = self.file_pattern.format(date_part=file_date_str)
        uri = self.uri_pattern.format(file_name=file_name)
        download_file_path = os.path.join(self.download_path, file_name)
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        print(uri)
        response = self.session.request(method='GET', url=uri, allow_redirects=True)
        # r = requests.get(nse_uri, allow_redirects=True)
        status = self._check_reponse(response)
        if not status:
            return file_date, None
        content_type = response.headers.get('content-type')
        print(content_type)

        if content_type in ['application/zip', 'application/x-zip-compressed', 'application/x-7z-compressed',
                            'text/csv', 'application/gzip', 'application/x-tar', 'text/plain']:
            with open(download_file_path, 'wb') as file_pointer:
                file_pointer.write(response.content)
            print("{} download complete".format(file_name))
        else:
            e = _errorify("INVALID_CONTENT_TYPE",f"content-type {content_type} being returned is not supported..")
            raise Exception(e)
        return file_date, download_file_path

    def download_by_date(self, date_str, date_format: Optional[str] = '%Y%m%d'):
        """
        Function to download the BSE Daily bhav copy for a date
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
        Function to download multiple Daily BSE bhav copies for a date range

        :param str date_start: The start date of the date range
        :param str date_end: The end date of the date range
        :param str date_format: The format of the input dates, default='%Y%m%d'
        :param str num_workers: The number of workers to be utilized to get the files, default=1
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
