import os
import time
import logging
import requests
import datetime
from pandas import DataFrame
from requests.exceptions import RequestException

os.environ['HTTP_PROXY'] = 'http://10.16.42.23:7890'
os.environ['HTTPS_PROXY'] = 'http://10.16.42.23:7890'


def get_html(url, page):
    params = {
        "page": page, "size": 1000, "order": "desc", "orderby": "percent", "order_by": "symbol", "market": "CN",
        "type": "sh_sz", "_": int(time.time() * 1000)
    }
    try:
        headers = {
             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        }
        response = requests.get(url, headers=headers, params=params)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response
        return None
    except RequestException:
        return None


def set_logger(logger, t):
    log_path = './run_log'
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler(os.path.join(log_path, 'run_ice_crawl_log_{}.log'
                                               .format(t)))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def ice_stock():
    t = str(datetime.datetime.now()).split(' ')[0] + '_' + str(crawl_time) + 'th'
    logger = logging.getLogger(__name__)
    set_logger(logger, t)
    logger.info('The {}th crawl start!'.format(crawl_time))
    url = "https://xueqiu.com/service/v5/stock/screener/quote/list"
    stock_hs = []
    page = -1
    while True:
        print('current page {}'.format(page))
        page += 1
        response = get_html(url, page)
        # error
        if response is None:
            logger.error('Response failed in {}th time crawler!'
                         .format(crawl_time))
            break
        response_json = response.json()
        # no data
        try:
            stock_info = response_json['data']['list']
        except:
            logger.info('The {}th time crawler finished at localtime {} total of {} pieces of data crawled!'
                        .format(1, datetime.datetime.now(), len(stock_hs)))
            break
        # finish
        page_count = 0
        for stock in stock_info:
            stock_detail = {
                '股票名称': stock['name'],
                '股票代码': stock['symbol'],
                '当前价': stock['current'],
                '涨跌额': stock['chg'],
                '涨跌幅%': stock['percent'],
                '年初至今': stock['current_year_percent'],
                '成交量': stock['volume'],
                '成交额(元)': stock['amount'],
                '换手率': stock['turnover_rate'],
                '市盈率': stock['pe_ttm'],
                '股息率%': stock['dividend_yield'],
                '市值(元)': stock['market_capital']
            }
            page_count += 1
            stock_hs.append(stock_detail)
        logger.info('Successfully completed crawl {} stock mess on page of {} at localtime {}'
                    .format(page_count, page, t))
    df = DataFrame(data=stock_hs)
    df.drop_duplicates(inplace=True)
    # 保存 dataframe
    file_name = f'stock_{t}.txt'
    df.to_csv(file_name, encoding='utf-8', index=False)
    print('ok')
    logger.info('The {}th crawl finished after dropping duplicates total {} data crawled!'.format(crawl_time, len(df)))


if __name__ == '__main__':
    crawl_time = 1
    ice_stock()
