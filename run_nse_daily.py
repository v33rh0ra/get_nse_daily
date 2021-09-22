from nse_daily import NSEDaily
from datetime import datetime

def main():
    print("{} Run Started..".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    nd = NSEDaily()
    res= nd.download_by_date('20200904')
    res2 = nd.download_by_date_range(date_start='20200907',date_end='20200915',num_workers=3)
    print("{} Run Completed..".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    main()