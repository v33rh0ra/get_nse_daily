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
Usage in python shown below.

    from nse_daily import NSEDaily
    
    nd = NSEDaily()
    res= nd.download_by_date('20200904')
    res2 = nd.download_by_date_range(date_start='20200907',date_end='20200915',num_workers=3)


