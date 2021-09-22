# get nse daily bhav
# https://www1.nseindia.com/content/historical/EQUITIES/2020/JUN/cm12JUN2020bhav.csv.zip
from datetime import datetime, timedelta
from typing import Optional

import requests
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry

from .common import _get_exception

from multiprocessing.pool import ThreadPool

class NSEDaily(object):
    def __init__(self,
                 default_date_pattern: Optional[str] = '%Y%m%d',
                 nse_file_pattern: Optional[str] = "cm{date_part}bhav.csv.zip",
                 nse_file_date_part_format: Optional[str] = '%d%b%Y',
                 nse_uri_pattern: Optional[
                     str] = "https://www1.nseindia.com/content/historical/EQUITIES/{yyyy_mon}/{file_name}",
                 nse_uri_yy_mm_format: Optional[str] = '%Y/%b',
                 download_path: Optional[str] = None
                 ):
        self.default_date_pattern = default_date_pattern
        self.nse_file_pattern = nse_file_pattern
        self.nse_file_date_part_format = nse_file_date_part_format
        self.nse_uri_pattern = nse_uri_pattern
        self.nse_uri_yy_mm_format = nse_uri_yy_mm_format
        self.download_path = download_path
        if self.download_path is None or str(self.download_path).strip() == '':
            appdir =str(Path.cwd())
            self.download_path = os.path.join(appdir, 'downloads')
        self._create_session()

    def _create_session(self):
        self.session = requests.Session()
        retry = Retry(total=5,
                      read=5,
                      connect=5,
                      status_forcelist=(500,502,504),
                      method_whitelist=frozenset(['GET','POST']),
                      backoff_factor=1
                      )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://',adapter)
        self.session.mount('https://',adapter)
    def _check_reponse(self,response:requests.Response):
        try:
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            print('HTTP Error %s',response.reason)
            print(response.text)
            return False

    def _download_by_date(self,file_date:datetime):
        daynum = file_date.weekday()
        if daynum>=5:
            print("{} is weekend, file skipped".format(file_date.strftime(self.default_date_pattern)))
            return file_date,None
        file_date_str = file_date.strftime(self.nse_file_date_part_format).upper()
        nse_yy_mm = file_date.strftime(self.nse_uri_yy_mm_format).upper()
        nse_file_name = self.nse_file_pattern.format(date_part=file_date_str)
        nse_uri = self.nse_uri_pattern.format(yyyy_mon=nse_yy_mm, file_name=nse_file_name)
        nse_download_file_path = os.path.join(self.download_path, nse_file_name)
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        response = self.session.request(method='GET',url=nse_uri,allow_redirects=True)
        # r = requests.get(nse_uri, allow_redirects=True)
        status = self._check_reponse(response)
        if not status:
            return file_date,None
        content_type = response.headers.get('content-type')
        if content_type == 'application/zip':
            with open(nse_download_file_path, 'wb') as nse_file:
                nse_file.write(response.content)
        print("{} download complete".format(nse_file_name))
        return file_date,nse_download_file_path

    def download_by_date(self, date_str,date_format:Optional[str]='%Y%m%d'):
        try:
            file_date = datetime.strptime(date_str,date_format)
            return self._download_by_date(file_date)
        except:
            e = _get_exception()
            raise Exception(e)

    def download_by_date_range(self, date_start:str,date_end:str,date_format:Optional[str]='%Y%m%d',num_workers:Optional[int]=1):
        try:
            start = datetime.strptime(date_start,date_format)
            end = datetime.strptime(date_end,date_format)
            l_dates = [start+timedelta(days=x) for x in range(0,(end-start).days)]
            tpool = ThreadPool(processes=num_workers)
            l_e = tpool.map(self._download_by_date,l_dates)
            return l_e
        except:
            e = _get_exception()
            raise Exception(e)