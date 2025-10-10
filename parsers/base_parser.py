import json
import os
from datetime import datetime
from abc import ABC, abstractmethod

import pandas as pd

from news.news_item import NewsItem


class BaseParser(ABC):


    @property
    @abstractmethod
    def class_name(self) -> str:
        """Должен возвращать имя класса как строку"""
        pass

    @property
    @abstractmethod
    def raw_data(self) -> list:
        """Должен возвращать список данных"""
        pass

    @property
    @abstractmethod
    def parameters(self) -> dict:
        """Должен возвращать словарь с параметрами"""
        pass

    @property
    @abstractmethod
    def requests_to_parse(self) -> list[str]:
        """Должен возвращать список запросов для парсинга"""
        pass

    @property
    @abstractmethod
    def metadata(self) -> dict:
        """Должен возвращать словарь метаданных"""
        pass

    @abstractmethod
    def parse(self) -> list[NewsItem]:
        """Основной метод для парсинга данных из разных источников"""

    def to_excel(self):

        # Данные для сохранения
        data_for_excel = []
        for item in self.raw_data:
            data_for_excel.append(item.get_full_data_dict())
        df = pd.DataFrame(data_for_excel)

        # Путь сохранения
        file_name = self.parameters['TEMPLATES_FILENAME'][self.class_name].format(**self.metadata)
        filepath = os.path.join(self.parameters['OUTPUT_DIR_PROCESSED'],
                                f"{self.class_name}_{file_name}.xlsx")

        # Сохранение
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
            print(f"    >> Data EXCEL was saved!")

    def to_json(self):
        # Данные для сохранения — список словарей с полями объектов
        data_for_json = [item.get_full_data_dict() for item in self.raw_data]

        # Формируем путь к файлу с расширением .json
        file_name = self.parameters['TEMPLATES_FILENAME'][self.class_name].format(**self.metadata)
        filepath = os.path.join(self.parameters['OUTPUT_DIR_PROCESSED'],
                                f"{self.class_name}_{file_name}.json")

        # Сохраняем в JSON с отступами и utf-8
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data_for_json, f, ensure_ascii=False, indent=4)

        print(f"    >> Data JSON was saved!")

    def print_statistics(self):
        total = len(self.raw_data)
        verified = len([i for i in self.raw_data if i.approved])
        try:
            print(f"Total sources collected: {total}")
            print(f"Verified sources: {verified} ({verified / total:.1%})")
        except:
            raise
            print('ERROR FOR PARSING SOURCE!!!')

    def check_approved_source(self, source) -> bool:
        return (
                any(domain in source.lower() for domain in self.parameters.get('TRUSTED_SOURCES_DOMAINS', []))
                or
                any(channel in source.lower() for channel in self.parameters.get('TRUSTED_SOURCES_TELEGRAM_CHANNELS', []))
                )


    # @staticmethod
    # def get_full_page_text_by_url(url):
    #     """Получаем весь текст из body страницы"""
    #     try:
    #         headers = {
    #             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    #         }
    #
    #         # Добавляем таймауты и обработку ошибок
    #         response = requests.get(url, headers=headers, timeout=15)
    #         response.raise_for_status()
    #
    #         soup = BeautifulSoup(response.text, 'html.parser')
    #
    #         # Удаляем ненужные элементы перед извлечением текста
    #         for element in soup(
    #                 ['script', 'style', 'nav', 'footer', 'iframe', 'noscript', 'svg', 'img', 'button', 'form']):
    #             element.decompose()
    #
    #         # Получаем весь текст из body
    #         body = soup.find('body')
    #         if not body:
    #             return "Не удалось найти body на странице"
    #
    #         full_text = body.get_text(separator='\n', strip=True)
    #         return clean_text(full_text)
    #
    #     except Exception as e:
    #         return f"Ошибка при загрузке страницы: {str(e)}"
