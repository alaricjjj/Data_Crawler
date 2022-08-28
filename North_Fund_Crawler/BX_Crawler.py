__Author__ = "Alaric"

import pandas as pd
import requests
import json
import re
import time
import random


class NorthFundCrawler:
    """
    通过东方财富网爬取北向，南向资金数据
    """
    def __init__(self):
        self.init_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        self.init_params = {
            "callback": "jQuery1123009597432045781096_1660960497724",
            "sortColumns": "TRADE_DATE",
            "sortTypes": -1,
            "pageSize": 200,
            "pageNumber": 1,
            "reportName": "RPT_MUTUAL_DEAL_HISTORY",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            # 'filter': '(MUTUAL_TYPE="001")', # 001:沪股通；002：港股通（沪）；003：深股通；004：港股通（深）
        }

    def data_crawler(self):
        """
        通过网站获取原始数据，并转化为DataFrame格式
        :return: 原始数据
        """
        df_stock = pd.DataFrame()
        while True:
            try:
                print(f"正在访问第{self.init_params['pageNumber']}页")
                # 获取url内容
                r = requests.get(url=self.init_url, params=self.init_params, timeout=30)
                # 内容解析
                content = re.sub(r"\s", "", r.content.decode())  # 删除任意空白字符， [\t\n\r\f]
                content = re.findall(r"\(({.*})\)", content)[0]  # 提取字典部分
                js_content = json.loads(content)
            except Exception as e:
                print(f"出现{e}报错，暂停访问，保存已获取数据")
                break

            if js_content["result"] is not None:
                # 数据整理
                data = js_content["result"]["data"]
                # 将数据整理成表格
                df = pd.DataFrame(data)
                df_stock = pd.concat([df_stock, df], ignore_index=True)
            else:
                print("===数据已获取完毕===")
                break
            self.init_params["pageNumber"] += 1
            time.sleep(random.randint(1, 5))

        return df_stock

    @staticmethod
    def data_parse(df_stock):
        """
        对爬取的原始数据进行处理
        :param df_stock: 从东方财富网爬取的原始数据
        :return: 按资金方向整理好的DataFrame
        """
        df_stock["TRADE_DATE"] = pd.to_datetime(
            df_stock["TRADE_DATE"], format="%Y-%m-%d%H:%M:%S"
        )  # 设置为日期格式

        df_stock = df_stock.loc[
                   (df_stock["MUTUAL_TYPE"] != "005") & (df_stock["MUTUAL_TYPE"] != "006"), :
                   ]

        # 设置多重索引
        df_stock.set_index(["TRADE_DATE", "MUTUAL_TYPE"], inplace=True)
        df_stock = df_stock.sort_index()
        df_stock = df_stock.loc[~df_stock.index.duplicated(), :]  # 去除multi_index中的重复值
        df_stock = df_stock.unstack(level=1)
        df_stock = df_stock.swaplevel(axis=1)

        # 重命名(001:沪股通；002：港股通（沪）；003：深股通；004：港股通（深）)
        col_name = {"001": "hk2sh", "002": "sh2hk", "003": "hk2sz", "004": "sz2hk"}
        df_stock = df_stock.rename(columns=col_name, level=0)

        col_name = {
            "FUND_INFLOW": "当日资金流入",
            "NET_DEAL_AMT": "当日成交净买额",
            "QUOTA_BALANCE": "当日余额",
            "ACCUM_DEAL_AMT": "历史累计净买额",
            "BUY_AMT": "买入成交额",
            "SELL_AMT": "卖出成交额",
            "LEAD_STOCKS_CODE": "领涨股代码",
            "LEAD_STOCKS_NAME": "领涨股名称",
            "LS_CHANGE_RATE": "领涨股涨跌幅",
            "INDEX_CLOSE_PRICE": "上证指数",
            "INDEX_CHANGE_RATE": "上证指数涨跌幅"
        }
        df_stock.rename(columns=col_name, level=1, inplace=True)

        return df_stock

    def main_crawler(self, save_path):

        df_crawler = self.data_crawler()

        df_parsed = self.data_parse(df_crawler)

        # 提取北向资金
        df_bx = df_parsed.loc[:, ["hk2sh", "hk2sz"]]
        saved_path = f"{save_path}/北向资金.csv"
        df_bx.to_csv(saved_path, encoding="gbk")
        message = f'获取北向资金，路径为{saved_path}'
        print(message)

        # 提取南向资金
        df_nx = df_parsed.loc[:, ["sh2hk", "sz2hk"]]
        saved_path = f"{save_path}/南向资金.csv"
        df_nx.to_csv(saved_path, encoding="gbk")
        message = f'获取南向资金，路径为{saved_path}'
        print(message)


if __name__ == "__main__":
    NorthFundCrawler.main_crawler(save_path='/Volumes/Alaric_Disk/Quant_DB/北向资金')
