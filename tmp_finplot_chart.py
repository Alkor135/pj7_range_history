# Тестовый код
import pandas as pd
import finplot as fplt
import yfinance

df = yfinance.download('AAPL')
# df = pd.read_csv('ohlcv.csv')
print(df)

# fplt.candlestick_ochl(df[['Open', 'Close', 'High', 'Low']])
# fplt.show()
