import asyncio
from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from config.settings import settings
from parsers.google_parser import GoogleParser
from parsers.tavily_parser import TavilyParser
from parsers.telegram_parser import TelegramParser
from parsers.website_parser import WebsiteParser
from tools.normalize_data import identification_region


def fill_raw_data_html(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заполняет недостающие raw_data HTML-контентом страниц, используя UniversalParser

    Параметры:
        df: DataFrame с колонками ['url', 'raw_data', ...]

    Возвращает:
        Обновленный DataFrame с заполненными raw_data
    """
    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Создаем экземпляр парсера один раз для всех запросов
    parser = WebsiteParser()

    def parse_url(url: str) -> str:
        """Вспомогательная функция для парсинга URL"""
        try:
            content = parser.parse(url)
            return content if content else ""
        except Exception as e:
            print(f"Ошибка при парсинге {url}: {str(e)}")
            return ""

    # Находим строки, где нужно собрать данные
    mask = (df['raw_data'].isna()) | (df['raw_data'] == '')

    # Применяем парсер только к этим строкам
    if mask.any():
        # Используем progress_apply для отображения прогресса (если установлен tqdm)
        try:
            from tqdm import tqdm
            tqdm.pandas(desc="Парсинг URL")
            df.loc[mask, 'raw_data'] = df.loc[mask, 'url'].progress_apply(parse_url)
        except ImportError:
            df.loc[mask, 'raw_data'] = df.loc[mask, 'url'].apply(parse_url)

    return df


def collect_raw_data_sync(
        sources: list[str],
        category: str,
        region: str,
        period: str,
        to_excel: bool,
        month_begin: datetime = date.today().replace(day=1),
        month_begin_utc: datetime = datetime.now(timezone.utc).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0)
) -> pd.DataFrame:
    """Синхронный сбор сырых данных из различных источников"""
    print(f'**** СБОР СЫРЫХ ДАННЫХ ****')

    fields = {
        'region': pd.Series(dtype='str'),
        'category': pd.Series(dtype='str'),
        'period': pd.Series(dtype='str'),
        'month_begin': pd.Series(dtype='datetime64[ns]'),
        'approved': pd.Series(dtype='bool')
    }
    full_data = pd.DataFrame(fields)

    for source in sources:
        print(f"\nОбработка источника: {source}")

        match source:
            case 'Google':
                new_data = GoogleParser(
                    category, region, period, month_begin, to_excel
                ).raw_data
            case 'Tavily':
                new_data = TavilyParser(
                    category, region, period, month_begin, to_excel
                ).raw_data
            case 'Telegram':
                dir = Path(settings.OUTPUT_DIR_PROCESSED)
                matching_files = [
                    f for f in dir.iterdir()
                    if f.is_file() and f"Telegram_{category}_BASE_{period}_{month_begin_utc}.xlsx" in f.name
                ]

                if matching_files:
                    print(f'Файл {matching_files[0]} найден!')
                    new_data = pd.read_excel(matching_files[0])
                else:
                    # Для Telegram используем синхронную версию парсера
                    new_data = TelegramParser(
                        category, region, period, month_begin_utc, to_excel
                    ).raw_data

                new_data = identification_region(region, new_data)
                new_data = new_data.loc[new_data['region'] == region]
                print(f'Размер данных: {len(new_data)}')

        required_cols = ['url', 'region', 'category', 'period', 'date_from', 'approved', 'raw_data']
        new_data = new_data[[col for col in required_cols if col in new_data.columns]]
        full_data = pd.concat([full_data, new_data], ignore_index=True)

    full_data.drop_duplicates(keep='last', inplace=True)
    return full_data


async def parse_websites_only_async(
        full_data: pd.DataFrame,
        max_concurrent: int = 3
) -> pd.DataFrame:
    """Только асинхронный парсинг сайтов"""
    print(f'\n**** ПАРСИНГ ДАННЫХ С САЙТОВ ****')

    mask = (full_data['raw_data'].isna()) | (full_data['raw_data'] == '')
    urls_to_parse = full_data.loc[mask, 'url'].tolist()

    if not urls_to_parse:
        print("Нет URL для парсинга - все данные уже заполнены")
        return full_data

    print(f"Найдено {len(urls_to_parse)} URL для парсинга")

    async with WebsiteParser(
            headless=True,
            timeout=15000
    ) as parser:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def parse_single_url(url):
            async with semaphore:
                return await parser.parse(url)

        tasks = [parse_single_url(url) for url in urls_to_parse]

        parsed_contents = []
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Парсинг URL"):
            result = await future
            parsed_contents.append(result)

    full_data.loc[mask, 'raw_data'] = parsed_contents
    return full_data


def get_raw_data(
        sources: list[str],
        category: str,
        region: str,
        period: str,
        to_excel: bool,
        month_begin: datetime = date.today().replace(day=1),
        month_begin_utc: datetime = datetime.now(timezone.utc).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ),
        max_concurrent: int = 3
) -> pd.DataFrame:
    """
    Синхронная функция, которая:
    1. Собирает сырые данные синхронно
    2. Запускает только асинхронный парсинг сайтов
    """
    # 1. Синхронный сбор данных
    full_data = collect_raw_data_sync(
        sources, category, region, period, to_excel,
        month_begin, month_begin_utc
    )

    # 2. Асинхронный парсинг сайтов
    return asyncio.run(
        parse_websites_only_async(full_data, max_concurrent)
    )