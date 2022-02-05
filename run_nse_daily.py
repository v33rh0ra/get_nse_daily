from nse_daily import NSEDaily, BSEDaily
from datetime import datetime

def main():
    print("{} Run Started..".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # nd = NSEDaily()
    # res= nd.download_by_date('20220103')
    # res2 = nd.download_by_date_range(date_start='20200907',date_end='20200915',num_workers=3)


    bd = BSEDaily()
    res = bd.download_by_date('20220113')
    print(res)
    print("{} Run Completed..".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
if __name__ == '__main__':
    main()