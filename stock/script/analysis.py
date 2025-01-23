# 绘制指定股票的k线
import pandas as pd
import mplfinance as mpf

from stock.base.time import TimeLevel
from stock.data.baostock_dataset import g_baostock_dataset


def main(stock_code, start_date, fre: TimeLevel):
    price = g_baostock_dataset.get_data(
        stock_name=stock_code,
        time_level=fre,
        start=start_date
    )

    if fre == TimeLevel.Day1:
        freq = "B"
    elif fre == TimeLevel.Hour1:
        freq = "BH"
    elif fre == TimeLevel.Min15:
        freq = "15T"
    elif fre == TimeLevel.Week1:
        freq = "W"
    else:
        raise ValueError("未知频率")

    start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
    date_index = pd.date_range(start=start_date, periods=len(price), freq=freq)
    price.set_index(date_index, inplace=True)

    min_val = price["Volume"].min()
    max_val = price["Volume"].max()
    price['Volume_Norm'] = (price["Volume"] - min_val) / (max_val - min_val)
    price['Volume_SMA_3'] = price['Volume_Norm'].rolling(window=5).mean()
    ap = mpf.make_addplot(price["Volume_SMA_3"])

    mpf.plot(price, type='candle', volume=True, addplot=ap, warn_too_much_data=10000)


if __name__ == "__main__":
    # 股票代号
    code = "sh.000300"
    # 开始时间
    start = "2001-5-10"
    main(code, start, TimeLevel.Day1)
