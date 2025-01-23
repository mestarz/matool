from stock.api.var import g_stock_market


def main():
    codes = g_stock_market.query_all_code("2024-12-27")
    codes.to_csv("stock_code.csv")


if __name__ == "__main__":
    main()
