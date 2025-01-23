from pandas.tseries.holiday import previous_workday
from datetime import datetime, timedelta

from stock.base.time import TimeLevel
from stock.api.var import g_stock_market
import pandas as pd

from stock.data.baostock_dataset import g_baostock_dataset


def main(start: str):
    # 获取上一个工作日
    today = pd.to_datetime('today')
    next_today = today + timedelta(days=1)
    day = previous_workday(next_today).strftime("%Y-%m-%d")
    codes = g_stock_market.query_all_code(day)

    flag = True

    for code in codes[0]:
        for freq in [TimeLevel.Day1, TimeLevel.Hour1, TimeLevel.Week1, TimeLevel.Min15, TimeLevel.Month1]:
            # if flag and code != "sz.301526":
            #     continue
            # flag = False

            print(f"[update-dataset] {code} from {start} to {day}\n")
            g_baostock_dataset.update(code, freq, start, day)


if __name__ == "__main__":
    main("2024-12-27")
