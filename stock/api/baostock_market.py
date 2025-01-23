# BaoStock
# http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3
from typing import Optional

import baostock as bs
import pandas as pd

from baostock.data.resultset import ResultData
from stock.base.time import TimeLevel


class BaoStockMarket:
    def __init__(self):
        self.is_login = False

    def __del__(self):
        pass

    def _check_login(self):
        if not self.is_login:
            _ = bs.login()
            self.is_login = True

    @staticmethod
    def _get_result(res: ResultData) -> pd.DataFrame:
        if res.error_code != '0':
            print(f"[Error] baostock get result error! {res.error_msg}")
        data_list = []
        while res.error_code == '0' and res.next():
            data_list.append(res.get_row_data())
        result = pd.DataFrame(data_list)
        return result

    def query_stock_history(self, code: str, start_date: str, end_date: str, frequency: TimeLevel) -> pd.DataFrame:
        self._check_login()
        f = frequency.get_baostock_freq()
        fields = "date,time,open,high,low,close,volume"
        if frequency.is_day_or_more():
            fields = "date,open,high,low,close,volume"

        r = bs.query_history_k_data_plus(code=code,
                                         fields=fields,
                                         start_date=start_date,
                                         end_date=end_date,
                                         frequency=f,
                                         adjustflag='2')  # 前复权
        result = self._get_result(r)

        if len(result) == 0:
            return pd.DataFrame()

        result.columns = r.fields
        result.replace('', 0, inplace=True)
        for item in ["open", "high", "low", "close"]:
            result[item] = result[item].astype(float)
        result["volume"] = result["volume"].astype(int)
        result = result.rename(
            columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
        return result

    def query_all_code(self, day: str = None) -> pd.DataFrame:
        self._check_login()
        r = bs.query_all_stock(day)
        return self._get_result(r)
