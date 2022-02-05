[![](https://i.imgur.com/kQOtbBk.png)](https://v33rh0ra.github.io/get_nse_daily/)
# get_nse_daily
# ============
### _pure python library built using requests to get daily day end data from nse india_
[**Project website**](https://v33rh0ra.github.io/get_nse_daily/nse_daily/index.html)

[Documentation]

[Documentation]: https://v33rh0ra.github.io/get_nse_daily/nse_daily/index.html

Installation
------------

    $ pip install get_nse_daily


Usage
-----
For downloading the NSE Daily Bhav Copy (Day end data) in python use below code, by either providing a date for a single day, or a date range for multiple days.

    from nse_daily import NSEDaily
    
    nd = NSEDaily()
    res= nd.download_by_date('20200904')
    res2 = nd.download_by_date_range(date_start='20200907',date_end='20200915',num_workers=3)

For downloading the BSE Daily Bhav Copy (Day end data) in python use below code.

    from nse_daily import BSEDaily
    
    bd = BSEDaily()
    res = bse.download_by_date('20220113')
    res2 = bse.download_by_date_range(date_start='20220101',date_end='20220129',num_workers=5)



