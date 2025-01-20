from pathlib import Path

import pandas as pd
# from lightweight_charts import Chart
import plotly.graph_objects as go


def create_range_bars(tick_df, range_size=100):
    """
    Создает Range бары из тикового датафрейма.

    Parameters:
        tick_df (pd.DataFrame): Датафрейм с тиковыми данными (колонки: 'datetime', '<LAST>', '<VOL>').
        range_size (float): Размер диапазона для Range баров.

    Returns:
        pd.DataFrame: Датафрейм с Range барами (колонки: 'datetime', 'open', 'high', 'low', 'close', 'vol').
    """
    # Инициализация переменных
    range_bars = []
    open_price = None
    high_price = None
    low_price = None
    vol = 0
    bar_start_time = None  # Время открытия текущего бара

    for _, row in tick_df.iterrows():
        price = row['<LAST>']
        volume = row['<VOL>']
        datetime = row['datetime']

        # Если новый бар, инициализируем его
        if open_price is None:
            open_price = price
            high_price = price
            low_price = price
            bar_start_time = datetime  # Устанавливаем время первого тика

        # Обновляем high, low и объем
        high_price = max(high_price, price)
        low_price = min(low_price, price)
        vol += volume

        # Проверяем, превышен ли range_size
        if high_price - low_price >= range_size:
            # Закрываем текущий бар
            range_bars.append({
                'datetime': bar_start_time,  # Используем время первого тика в баре
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': price,
                'vol': vol
            })
            # Инициализируем следующий бар
            open_price = price
            high_price = price
            low_price = price
            vol = 0
            bar_start_time = datetime  # Обновляем время начала нового бара

    # Добавляем последний бар, если он не завершен
    if open_price is not None:
        range_bars.append({
            'datetime': bar_start_time,  # Используем время первого тика в баре
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': price,
            'vol': vol
        })

    return pd.DataFrame(range_bars)


def make_timestamps_unique(df, time_column='datetime'):
    """
    Делает временные метки уникальными, добавляя миллисекунды к повторяющимся.

    Parameters:
        df (pd.DataFrame): Датафрейм, в котором нужно сделать уникальные временные метки.
        time_column (str): Название колонки с временными метками.

    Returns:
        pd.DataFrame: Датафрейм с уникальными временными метками.
    """
    df = df.copy()
    seen_times = {}
    
    for idx, timestamp in enumerate(df[time_column]):
        # Если временная метка уже встречалась, добавляем миллисекунду
        if timestamp in seen_times:
            seen_times[timestamp] += 1
            df.loc[idx, time_column] = timestamp + pd.Timedelta(milliseconds=seen_times[timestamp])
        else:
            seen_times[timestamp] = 0
    
    return df


def plot_candlestick_chart_plotly(range_bars_df):
    """
    Отображает свечной график из DataFrame с Range барами с использованием Plotly.
    
    Parameters:
        range_bars_df (pd.DataFrame): DataFrame с Range барами 
        (должны быть колонки: 'datetime', 'open', 'high', 'low', 'close').
    """
    # Создание фигуры
    fig = go.Figure(data=[go.Candlestick(
        x=range_bars_df['datetime'],
        open=range_bars_df['open'],
        high=range_bars_df['high'],
        low=range_bars_df['low'],
        close=range_bars_df['close'],
        name='Range Bars'
    )])

    # Настройка графика
    fig.update_layout(
        title='Candlestick Chart of Range Bars',
        xaxis_title='Datetime',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False  # Отключаем полосу прокрутки
    )

    # Отображение
    fig.show()


if __name__ == "__main__":
    # Путь к папке с Parquet-файлами
    parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')
    # Размерность Range баров
    range_size = 1000

    # Список для хранения путей ко всем Parquet-файлам
    parquet_files = []

    # Чтение всех партиций
    for year_folder in parquet_folder.glob('year=*'):
        for month_folder in year_folder.glob('month=*'):
            # Сохранение путей ко всем файлам в текущей партиции
            for parquet_file in month_folder.glob('*.parquet'):
                parquet_files.append(parquet_file)

    # Вывод списка файлов
    # print(f"Найдено {len(parquet_files)} файлов:")
    # for file in parquet_files:
    #     print(file)

    # Пример чтения одного файла Parquet
    if parquet_files:
        example_file = parquet_files[0]  # Выберите первый файл или любой другой
        df = pd.read_parquet(
            example_file, columns=['datetime', '<LAST>', '<VOL>'], engine='pyarrow'
            )
        df['datetime'] = pd.to_datetime(df['datetime'])
        # print("Пример данных из файла:")
        # print(df)

    # Получаем DF с Range барами
    range_bars_df = create_range_bars(df, range_size)
    
    # Применяем функцию для уникальности временных меток
    range_bars_df = make_timestamps_unique(range_bars_df)

    # Преобразуем колонку 'date' в текстовый формат
    range_bars_df['datetime'] = range_bars_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    print(range_bars_df)

    # chart = Chart()
    
    # # Columns: time | open | high | low | close | volume 
    # # df = pd.read_csv('ohlcv.csv')
    # chart.set(range_bars_df)
    
    # chart.show(block=True)

    # Отображение графика
    plot_candlestick_chart_plotly(range_bars_df)
