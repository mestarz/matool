from datetime import datetime, timedelta
import pandas as pd

from stock.base.time import TimeLevel
from stock.api.var import g_stock_market
from stock.data.dataset import Dataset


class BaoStockDataset(Dataset):
    def __init__(self):
        super().__init__("baostock.db")

    def get_data(self, stock_name: str, time_level: TimeLevel, start: str) -> pd.DataFrame:
        today = datetime.today().strftime("%Y-%m-%d")
        datas = self._get_data(stock_name, time_level, start, today)
        results = pd.DataFrame(datas, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        return results

    def _get_data(self, stock_name: str, time_level: TimeLevel, start: str, end: str) -> list:
        start_timestamp = get_date_format_str(start, time_level)
        end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        end_timestamp = get_date_format_str(end, time_level)
        cur = self.conn.cursor()
        cur.execute(
            f'''
            SELECT * FROM {self.get_table_name(stock_name, time_level)}
            WHERE Timestamp BETWEEN {start_timestamp} AND {end_timestamp} ORDER BY Timestamp;
            '''
        )
        results = cur.fetchall()
        cur.close()
        return results

    def update(self, stock_name: str, time_level: TimeLevel, start: str, end: str):
        datas = g_stock_market.query_stock_history(
            code=stock_name,
            start_date=start,
            end_date=end,
            frequency=time_level,
        )
        if datas.empty:
            print("[update] do nothing\n")
            return

        if time_level.is_day_or_more():
            datas['ts'] = [int(datetime.strptime(ts, "%Y-%m-%d").strftime("%Y%m%d")) for ts in datas['date']]
        else:
            datas['ts'] = [int(ts) for ts in datas['time']]

        table_name = self.get_table_name(stock_name, time_level)
        self.create_non_exist(stock_name, time_level)
        self._update_data(table_name, datas)


def get_date_format_str(date: str, time_level: TimeLevel):
    base_str = int(datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d"))
    if time_level.is_day_or_more():
        return base_str
    return base_str + '0' * 9


g_baostock_dataset = BaoStockDataset()

if __name__ == "__main__":
    # g_baostock_dataset.update("sh.600000", TimeLevel.Day1, "2021-2-3", "")
    # e = datetime.datetime.strptime("2021-2-3", "%Y-%m-%d")
    # print(e.timestamp())
    # r = g_stock_market.query_stock_history("sh.600000", "2021-2-3", "", TimeLevel.Hour1)

    # print(r)
    r = g_baostock_dataset.get_data("sh.600000", TimeLevel.Day1, "2021-2-3")
    print(r)
