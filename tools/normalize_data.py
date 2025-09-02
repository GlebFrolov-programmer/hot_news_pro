import re

import pandas as pd

# from config.settings import settings


def clean_text(text):
        """Очистка текста от лишних пробелов и специальных символов"""
        if not text:
            return ""

        # Удаляем все HTML-теги
        text = re.sub(r'<[^>]+>', ' ', text)
        # Заменяем множественные пробелы/переносы на один пробел
        text = re.sub(r'\s+', ' ', text)
        # Удаляем спецсимволы (оставляем только буквы, цифры и основные знаки препинания)
        text = re.sub(r'[^\w\s.,!?;:()\-–—\'\"%$€₽«»„“‘’]', ' ', text)
        # Финальная очистка пробелов
        text = text.strip()
        return text


def identification_region(region: str, df: pd.DataFrame) -> str:
        """Проверяет наличие региона в тексте по ключевым словам из словаря."""
        if len(df) == 0 or not settings.REGION_KEYWORDS[region]:
            return ""

        # Создаем копию DataFrame для безопасности
        df = df.copy()

        # Приводим текст и ключевые слова к нижнему регистру
        content_lower = df['raw_data'].str.lower()
        pattern = '|'.join([kw.lower() for kw in settings.REGION_KEYWORDS[region]])

        # Находим строки, где content содержит ключевые слова региона
        mask = content_lower.str.contains(pattern, na=False, regex=True)

        # Заполняем region только для найденных строк с Undefined
        df.loc[mask & (df['region'] == 'Undefined'), 'region'] = region

        return df