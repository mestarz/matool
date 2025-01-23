import os
import re
import sqlite3

import pandas as pd

from crypto.base.time import TimeLevel


# 每一个币种k个表，按时间跨度划分
# 时间戳, Open, High, Low, Close, Volume
class Dataset:
    def __init__(self, dataset_name):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, dataset_name)
        self.conn = sqlite3.connect(file_path)

    def close(self):
        self.conn.commit()
        self.conn.close()

    @staticmethod
    def get_table_name(capital_name: str, time_level: TimeLevel):
        table_name = f"{capital_name}_{time_level.name}"
        in_tab = "-."
        out_tab = "_D"
        tran_tab = str.maketrans(in_tab, out_tab)
        return table_name.translate(tran_tab)

    def create_non_exist(self, capital_name: str, time_level: TimeLevel):
        # 先判断表是否存在，不存在时创建表
        cur = self.conn.cursor()
        table_name = self.get_table_name(capital_name, time_level)
        cur.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        )
        if cur.fetchone():
            return
        cur.execute(f'''
        CREATE TABLE {table_name} (
            timestamp INTEGER PRIMARY KEY, 
            open REAL, 
            high REAL, 
            low REAL, 
            close REAL,
            volume REAL);
        ''')
        cur.close()

    def _update_data(self, table_name: str, datas: pd.DataFrame):
        cur = self.conn.cursor()
        for index, row in datas.iterrows():
            ts = row['ts']
            o = row['Open']
            h = row['High']
            l = row['Low']
            c = row['Close']
            v = row['Volume']

            cur.execute(f"SELECT * FROM {table_name} WHERE timestamp={ts};")
            if cur.fetchone() is None:
                cur.execute(
                    f'''
                    INSERT INTO {table_name} 
                    (timestamp, open, high, low, close, volume)
                    VALUES ({ts}, {o}, {h}, {l}, {c}, {v});
                    ''')
                continue
            cur.execute(f'''
                        UPDATE {table_name}
                        SET open = {o}, high = {h}, low = {l}, close = {c}, volume = {v}
                        WHERE timestamp = {ts}
                        ''')
        self.conn.commit()
        cur.close()


if __name__ == "__main__":
    pass
