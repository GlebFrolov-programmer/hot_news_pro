import json
import os
import random
import time
from abc import ABC
import asyncio
import random
import requests
from googlesearch import search
import pandas as pd
from parsers.base_parser import BaseParser
from news.news_item import NewsItem
# from config.settings import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError


class GoogleParser(BaseParser, ABC):
    def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
        super().__init__()
        self.class_name = 'Google'
        self.requests_to_parse = requests_to_parse
        self.metadata = metadata
        self.parameters = parameters
        try:
            self.raw_data = [i for i in list(set(self.parse()))]
        except RetryError as e:
            print(f"Parsing failed after retries: {e}, продолжаем работу без результатов.")
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

    def get_limit_search(self):

        try:
            search_limit = self.parameters['SEARCH_LIMIT_GOOGLE']
            if not isinstance(search_limit, int):
                search_limit = 5
        except KeyError:
            search_limit = 5

        try:
            coefficient = len(self.parameters['SUBCATEGORIES'])
            if not isinstance(search_limit, int):
                coefficient = 5
        except KeyError:
            coefficient = 5

        return search_limit * coefficient

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=8, max=15),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    def parse(self) -> list[NewsItem]:

        print(f'\nGOOGLE SCRAPING {self.metadata}')

        def get_public_ip():
            response = requests.get('https://api.ipify.org')
            return response.text

        print("Ваш публичный IP:", get_public_ip())

        news_items = []
        for request in self.requests_to_parse:
            print(f'    QUERY: {request["query"]}')

            try:
                search_limit = request["search_limit"]
            except Exception as e:
                search_limit = self.get_limit_search()

            try:
                search_params = {
                    # "num_results": self.get_limit_search(),
                    "num_results": search_limit,
                    "lang": "ru",
                    "region": "ru",
                    "advanced": True,
                    "sleep_interval": random.uniform(1, 3),  # Random delay between queries
                }

                for obj in search(request['query'], **search_params):
                    news_items.append(
                        NewsItem(
                            source=self.class_name,
                            metadata=self.metadata,
                            url=obj.url,
                            title=obj.title,
                            approved=self.check_approved_source(obj.url)
                        )
                    )
                # Добавляем случайную задержку между разными категориями
                # time.sleep(random.uniform(5, 10))

            except Exception as e:
                print(f"Error processing query: {e}")
                raise e

        return news_items
