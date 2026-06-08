import json
import os
import time
from abc import ABC
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd

# Импорты для Yandex API
from yandex_cloud_ml_sdk import YCloudML

from parsers.base_parser import BaseParser
from news.news_item import NewsItem
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError


class YandexParser(BaseParser, ABC):
    def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
        super().__init__()
        self.class_name = 'Yandex'
        self.requests_to_parse = requests_to_parse
        self.metadata = metadata
        self.parameters = parameters
        self.sdk = None
        self.search_api = None

        # Инициализация Yandex SDK
        self.init_yandex_sdk()

        try:
            self.raw_data = [i for i in list(set(self.parse()))]
        except RetryError as e:
            print(f"Parsing failed after retries: {e}, продолжаем работу без результатов.")
            self.raw_data = []
        except Exception as e:
            print(f"Ошибка парсинга: {e}")
            self.raw_data = []

        self.save_to = save_to

        if save_to['TO_EXCEL']:
            self.to_excel()
        if save_to['TO_JSON']:
            self.to_json()
        self.print_statistics()

    @property
    def class_name(self) -> str:
        return self._class_name

    @class_name.setter
    def class_name(self, value: str):
        self._class_name = value

    @property
    def raw_data(self) -> list:
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value: list):
        self._raw_data = value

    @property
    def requests_to_parse(self) -> list[str]:
        return self._requests_to_parse

    @requests_to_parse.setter
    def requests_to_parse(self, value: list[str]):
        self._requests_to_parse = value

    @property
    def metadata(self) -> dict:
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict):
        self._metadata = value

    @property
    def parameters(self) -> dict:
        return self._parameters

    @parameters.setter
    def parameters(self, value: dict):
        self._parameters = value

    def init_yandex_sdk(self):
        """Инициализация Yandex Cloud ML SDK"""
        try:
            print("🔧 Инициализация Yandex Search API...")

            # Получаем параметры из конфигурации
            folder_id = self.parameters['AUTHENTICATION']['YANDEX_FOLDER_ID']
            auth_token = self.parameters['AUTHENTICATION']['YANDEX_AUTH_API']
            user_agent = self.parameters.get('USER_AGENT',
                                             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36")

            # Инициализация SDK
            self.sdk = YCloudML(
                folder_id=folder_id,
                auth=auth_token
            )

            # Настройка логирования
            # self.sdk.setup_default_logging("error")

            # Создание Search API объекта
            self.search_api = self.sdk.search_api.web(
                search_type=self.parameters.get('SEARCH_TYPE', 'ru'),
                user_agent=user_agent,
            )

            print("✅ Yandex Search API инициализирован")

        except Exception as e:
            print(f"❌ Ошибка инициализации Yandex SDK: {e}")
            raise

    def perform_api_search(self, query: str, page: int = 0) -> Optional[str]:
        """Выполнение поискового запроса через Yandex API"""
        try:
            print(f"🔍 API поиск: '{query}' (страница {page + 1})")

            # Получаем формат результатов
            format = self.parameters.get('RESULT_FORMAT', 'xml')

            # Выполняем асинхронный запрос через API
            operation = self.search_api.run_deferred(query, format=format, page=page)

            # Ждем завершения операции
            print("⏳ Ожидание ответа от API...")
            search_result = operation.wait(poll_interval=1)

            # Декодируем результат
            result_content = search_result.decode('utf-8')

            return result_content

        except Exception as e:
            print(f"❌ Ошибка при API поиске '{query}': {e}")
            return None

    def parse_xml_response(self, api_response: str) -> List[Dict]:
        """Парсинг XML ответа от API"""
        results = []

        try:
            from xml.etree import ElementTree as ET

            # Парсим XML
            root = ET.fromstring(api_response)

            # Ищем все элементы с результатами
            # Предполагаемая структура ответа
            for doc in root.findall('.//doc'):
                result = {}

                # Извлекаем заголовок
                title_elem = doc.find('title')
                if title_elem is not None and title_elem.text:
                    result['title'] = title_elem.text.strip()

                # Извлекаем URL
                url_elem = doc.find('url')
                if url_elem is not None and url_elem.text:
                    result['url'] = url_elem.text.strip()

                # Извлекаем дату, если есть
                date_elem = doc.find('date')
                if date_elem is not None and date_elem.text:
                    result['date'] = date_elem.text.strip()

                # Добавляем результат только если есть заголовок и URL
                if result.get('title') and result.get('url'):
                    results.append(result)

        except Exception as e:
            print(f"❌ Ошибка парсинга XML: {e}")

        return results

    def parse_html_response(self, api_response: str) -> List[Dict]:
        """Парсинг HTML ответа от API"""
        results = []

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(api_response, 'html.parser')

            # Ищем элементы результатов в HTML
            result_divs = soup.find_all('div', class_='serp-item') or soup.find_all('li', class_='serp-item')

            for div in result_divs:
                result = {}

                # Извлекаем заголовок и ссылку
                title_elem = div.find('h2') or div.find('a', class_='OrganicTitle-Link')
                if title_elem:
                    title_text = title_elem.get_text(strip=True)
                    if title_text:
                        result['title'] = title_text

                    url = title_elem.get('href', '')
                    if url:
                        result['url'] = url

                # Извлекаем дату
                date_elem = div.find('span', class_='datetime')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    if date_text:
                        result['date'] = date_text

                # Добавляем результат только если есть заголовок и URL
                if result.get('title') and result.get('url'):
                    results.append(result)

        except Exception as e:
            print(f"❌ Ошибка парсинга HTML: {e}")

        return results

    def parse_api_response(self, api_response: str) -> List[Dict]:
        """Парсинг ответа от API в зависимости от формата"""
        format = self.parameters.get('RESULT_FORMAT', 'xml').lower()

        if format == 'xml':
            return self.parse_xml_response(api_response)
        elif format == 'html':
            return self.parse_html_response(api_response)
        else:
            print(f"⚠️ Неизвестный формат: {format}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=8, max=15),
        retry=retry_if_exception_type((Exception,))
    )
    def parse(self) -> list[NewsItem]:
        """Основной метод парсинга с использованием Yandex API"""

        print(f'\n🔎 YANDEX API SCRAPING {self.metadata}')

        if not self.search_api:
            raise Exception("Search API не инициализирован")

        news_items = []
        total_queries = len(self.requests_to_parse)

        for i, request in enumerate(self.requests_to_parse, 1):
            query = request['query'] if isinstance(request, dict) else request

            # Определяем лимит результатов
            if isinstance(request, dict) and 'search_limit' in request:
                max_results = request['search_limit']
            else:
                max_results = self.parameters.get('SEARCH_LIMIT_YANDEX', 15)

            print(f'    [{i}/{total_queries}] QUERY: {query}')
            print(f'    Лимит результатов: {max_results}')

            try:
                all_results = []
                page = 0

                # Парсим результаты со всех страниц
                while len(all_results) < max_results:
                    page += 1
                    print(f"      📖 Страница {page}")

                    # Выполняем API запрос
                    api_response = self.perform_api_search(query, page - 1)

                    if not api_response:
                        print(f"      ⚠️ Пустой ответ от API")
                        break

                    # Парсим ответ
                    page_results = self.parse_api_response(api_response)

                    # Фильтруем дубликаты по URL
                    unique_urls = set()
                    filtered_results = []

                    for result in page_results:
                        url = result.get('url', '')
                        if url and url not in unique_urls:
                            unique_urls.add(url)
                            filtered_results.append(result)

                    all_results.extend(filtered_results)

                    print(f"      📊 Найдено на странице: {len(filtered_results)}")
                    print(f"      📊 Всего найдено: {len(all_results)}")

                    # Проверяем лимит
                    if len(all_results) >= max_results:
                        all_results = all_results[:max_results]
                        print(f"      ✅ Достигнут лимит в {max_results} результатов")
                        break

                    # Проверяем, есть ли еще результаты
                    if len(page_results) == 0:
                        print(f"      ⚠️ Больше нет результатов")
                        break

                # Создаем NewsItem для каждого результата
                for j, result in enumerate(all_results, 1):
                    title = result.get('title', '')
                    url = result.get('url', '')

                    if title and url:
                        news_items.append(
                            NewsItem(
                                source=self.class_name,
                                metadata=self.metadata,
                                url=url,
                                title=title,
                                approved=self.check_approved_source(url)
                            )
                        )
                        print(f"        {j}. {title[:70]} {url}...")

                print(f"      ✅ Всего уникальных результатов: {len(all_results)}")

            except Exception as e:
                print(f"Error processing query '{query}': {e}")
                raise e

        return news_items