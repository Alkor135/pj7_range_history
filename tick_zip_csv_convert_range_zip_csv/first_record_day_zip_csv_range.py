"""
Создает дата фрейм range баров с зазором и записывает в csv со сжатием zip
"""

from pathlib import Path
from datetime import datetime
import zipfile

import pandas as pd
import numpy as np


def file_select(folder_path):
    """
    Из указанной папки с файлами выбирает файл с самой первой датой в имении возвращает его имя.
    """
    # Инициализируем переменные для хранения файла с самой ранней датой
    earliest_file = None
    earliest_date = None

    # Обход всех файлов с расширением .zip
    for file in folder_path.glob("*.zip"):
        try:
            # Извлекаем дату из имени файла (до расширения)
            file_date = datetime.strptime(file.stem, "%Y%m%d")
            # Если это первая дата или она меньше текущей минимальной
            if earliest_date is None or file_date < earliest_date:
                earliest_date = file_date
                earliest_file = file
        except ValueError:
            # Пропускаем файлы с неверным форматом имени
            continue

    # Результат
    if earliest_file:
        return earliest_file
    else:
        print("Файлы с корректной датой не найдены.")


def create_range_bars(tick_df, range_size, tick_size=10):
    """
    Создает Range бары из тикового дата фрейма с зазором в один тик между барами.

    Parameters:
        tick_df (pd.DataFrame): Дата фрейм с тиковыми данными
        (колонки: 'datetime', 'last', 'volume').
        range_size (float): Размер диапазона для Range баров.
        tick_size (float, optional): Шаг цены (тик-сайз). Если не указан, будет вычислен автоматически.

    Returns:
        pd.DataFrame: Дата фрейм с Range барами
        (колонки: 'datetime', 'open', 'high', 'low', 'close', 'volume').
    """
    # Если тик-сайз не задан, вычислим как минимальную разницу между ценами
    if tick_size is None:
        tick_size = tick_df['last'].diff().abs().replace(0, np.nan).min()

    # Инициализация переменных
    range_bars = []
    open_price = None
    high_price = None
    low_price = None
    price = None
    vol = 0
    bar_start_time = None  # Время открытия текущего бара

    for _, row in tick_df.iterrows():
        price = row['last']
        volume = row['volume']
        date_time = row['datetime']

        # Инициализация нового бара
        if open_price is None:
            open_price = price
            high_price = price
            low_price = price
            vol = volume
            bar_start_time = date_time
            continue  # Переходим к следующей итерации

        # Обновление параметров текущего бара
        high_price = max(high_price, price)
        low_price = min(low_price, price)
        vol += volume

        # Проверка на превышение диапазона
        if high_price - low_price >= range_size:
            # Закрываем текущий бар
            range_bars.append({
                'datetime': bar_start_time,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': price,
                'volume': vol
            })

            # Определение направления нового бара
            if price == high_price:
                # Вверх — начинаем новый бар на +1 тик
                next_open_price = price + tick_size
            else:
                # Вниз — начинаем новый бар на -1 тик
                next_open_price = price - tick_size

            # Инициализация следующего бара с зазором
            open_price = next_open_price
            high_price = next_open_price
            low_price = next_open_price
            vol = 0
            bar_start_time = date_time  # Устанавливаем время начала нового бара

    # Добавляем последний бар, если он не завершен
    if open_price is not None:
        range_bars.append({
            'datetime': bar_start_time,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': price,
            'volume': vol
        })

    df = pd.DataFrame(range_bars)
    df['size'] = range_size
    return df


def tick_zip_csv_convert_range_zip_csv(path_tick, path_range, range_size=300):
    """  """
    # Нахождение файла с наименьшей датой
    tick_file = file_select(path_tick)
    # Чтение тиковых данных из файла в DF
    df_tick = pd.read_csv(tick_file, compression='zip')
    # Создание DF с range барами
    df = create_range_bars(df_tick, range_size, tick_size=10)
    print(df)

    with zipfile.ZipFile(path_range, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with zf.open('range.csv', mode="w") as buffer:
            df.to_csv(buffer, index=False)

    print(f'Файл {path_range} записан.')


if __name__ == "__main__":
    # Параметры
    ticker = 'RTS'  # Тикер
    range_size = 300  # Приблизительное количество range баров в день (влияет на размерность)
    # Путь к папке с zip архивами csv ticks
    path_tick = Path(r"C:\data_quote\data_finam_RTS_tick_zip")
    path_range = Path(fr'c:\Users\Alkor\gd\data_quote_zip\{ticker}_range.zip')
    # --------------------------------------------------------------------------------------------

    if not path_range.exists():
        tick_zip_csv_convert_range_zip_csv(path_tick, path_range, range_size)
    else:
        print('Файл уже существует. Прочти README.')
