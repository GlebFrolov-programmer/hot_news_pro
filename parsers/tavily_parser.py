import json
import os
import random
import time
import requests
import pandas as pd
import datetime
from tavily import TavilyClient

from parsers.base_parser import BaseParser
from news.news_item import NewsItem
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError


class TavilyParser(BaseParser):
    def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
        super().__init__()
        self.tavily_client = TavilyClient(api_key=parameters['AUTHENTICATION']['TAVILY_API_KEY'])
        self.class_name = 'Tavily'
        self.requests_to_parse = requests_to_parse  # список поисковых запросов (может включать категории, регионы, периоды)
        self.metadata = metadata
        self.parameters = parameters
        self.save_to = save_to

        try:
            self.raw_data = [i for i in list(set(self.parse()))]
        except RetryError as e:
            print(f"Parsing failed after retries: {e}, продолжаем работу без результатов.")
            self.raw_data = []

        if save_to.get('TO_EXCEL', False):
            self.to_excel()
        if save_to.get('TO_JSON', False):
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

    def get_limit_search(self):
        # Определяем максимальное количество результатов для каждого запроса
        try:
            limit = self.parameters.get('SEARCH_LIMIT_TAVILY', 5)
            if not isinstance(limit, int):
                limit = 5
        except KeyError:
            limit = 5

        try:
            coefficient = len(self.parameters.get('SUBCATEGORIES', []))
            if not isinstance(coefficient, int):
                coefficient = 5
        except KeyError:
            coefficient = 5

        return limit * coefficient

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=10),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    def parse(self) -> list[NewsItem]:
        """Парсинг через Tavily API для каждого поискового запроса"""
        print(f'\nTAVILY SCRAPING {self.metadata}')

        news_items = []

        for request in self.requests_to_parse:
            print(f'    QUERY: {request["query"]}')
            # Подготовка фильтра и параметров из запроса
            # Предполагается, что запрос может включать категорию, регион, период, разделенные, например, пробелами
            # Можно адаптировать парсинг по необходимости

            # max_results = self.get_limit_search()
            try:
                search_limit = request["search_limit"]
            except Exception as e:
                search_limit = self.get_limit_search()
            try:
                raw_data = self.tavily_client.search(
                    query=request["query"],
                    search_depth="advanced",
                    include_answer=True,
                    max_results=search_limit,
                    )
            except requests.exceptions.ConnectionError:
                print("Connection failed, retrying...")
                raise
            except Exception as e:
                print(f"Error during Tavily API request: {e}")
                raise

            for result in raw_data.get('results', []):
                news_items.append(
                    NewsItem(
                        source=self.class_name,
                        metadata=self.metadata,
                        url=result.get('url', ''),
                        title=result.get('title', ''),
                        approved=self.check_approved_source(result.get('url', '')),
                    )
                )

            # time.sleep(random.uniform(1, 3))  # задержка между запросами

        return news_items
