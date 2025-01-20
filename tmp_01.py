import pandas as pd
from lightweight_charts import Chart


if __name__ == '__main__':
    chart = Chart()
    
    df = pd.read_csv('ohlcv.csv')
    print(df)
    chart.set(df)
    
    chart.show(block=True)
