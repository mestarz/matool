from stock.api.baostock_market import BaoStockMarket
from stock.base.time import TimeLevel

g_stock_market = BaoStockMarket()

if __name__ == "__main__":
    r = g_stock_market.query_stock_history("sh.600000", "2024-5-10", "2024-7-10", TimeLevel.Hour1)
    print(r)
