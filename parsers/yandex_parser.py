import json
import os
import random
import time
from abc import ABC
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qs
from typing import List, Dict, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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
        self.driver = None
        self.setup_driver()

        try:
            self.raw_data = [i for i in list(set(self.parse()))]
        except RetryError as e:
            print(f"Parsing failed after retries: {e}, продолжаем работу без результатов.")
            self.raw_data = []
        finally:
            self.close_driver()

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

    def setup_driver(self):
        """Настройка драйвера Selenium"""
        try:
            chrome_options = Options()

            # Stealth-опции
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # Дополнительные опции
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # User-Agent
            chrome_options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # Установка драйвера
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Скрываем WebDriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            return True

        except Exception as e:
            print(f"❌ Ошибка настройки драйвера: {e}")
            raise WebDriverException(f"Driver setup failed: {e}")

    def close_driver(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()
            print("✅ Драйвер закрыт")

    def random_sleep(self, min_time: float, max_time: float):
        """Случайная задержка"""
        sleep_time = random.uniform(min_time, max_time)
        time.sleep(sleep_time)
        return sleep_time

    def get_timings(self) -> Dict:
        """Получение настроек таймингов"""
        default_timings = {
            'page_load': 15,
            'element_wait': 15,
            'typing_delay_min': 0.01,
            'typing_delay_max': 0.03,
            'between_queries_min': 2,
            'between_queries_max': 3,
            'after_search_min': 3,
            'after_search_max': 3,
            'between_pages_min': 1,
            'between_pages_max': 2,
        }

        # Можно добавить логику для кастомных таймингов из parameters
        if self.parameters.get('timings'):
            default_timings.update(self.parameters['timings'])

        return default_timings

    def perform_search(self, query: str, timings: Dict) -> bool:
        """Выполнение поискового запроса в Яндексе"""
        try:
            print(f"🔍 Поиск: '{query}'")

            # Формируем URL с параметрами для фильтра по времени
            params = {
                'text': query,
                # 'lr': 213,  # Москва и область
                'p': 0,  # страница
                'within': 1  # 2 недели
            }

            base_url = "https://yandex.ru/search/"
            search_url = f"{base_url}?{urlencode(params)}"

            print(f"🌐 Открываем Яндекс: {search_url}")
            self.driver.get(search_url)

            print("⏳ Ждем загрузки результатов...")
            WebDriverWait(self.driver, timings['page_load']).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".serp-item"))
            )
            print("✓ Результаты поиска загружены")

            # Задержка после поиска
            pause = self.random_sleep(
                timings['after_search_min'],
                timings['after_search_max']
            )
            print(f"⏸️ Пауза после поиска: {pause:.1f} сек")

            return True

        except Exception as e:
            print(f"❌ Ошибка при поиске '{query}': {e}")
            return False

    def is_advertisement(self, item) -> bool:
        """Проверка, является ли результат рекламным"""
        try:
            item.find_element(By.CSS_SELECTOR, ".label_theme_ad")
            return True
        except NoSuchElementException:
            return False

    def extract_title_and_url(self, item) -> tuple[Optional[str], Optional[str]]:
        """Извлечение заголовка и ссылки из элемента результата"""
        try:
            title_element = item.find_element(By.CSS_SELECTOR, ".OrganicTitle-Link, .serp-item__title")
            title = title_element.text.strip()
            url = title_element.get_attribute("href")
            return title, url

        except NoSuchElementException:
            try:
                title_element = item.find_element(By.CSS_SELECTOR, "h2 a")
                title = title_element.text.strip()
                url = title_element.get_attribute("href")
                return title, url
            except:
                return None, None

    def extract_date_info(self, item) -> str:
        """Извлечение информации о дате публикации"""
        try:
            date_element = item.find_element(By.CSS_SELECTOR, ".OrganicTextContentSpan, .datetime")
            return date_element.text.strip()
        except NoSuchElementException:
            return "Дата не указана"

    def parse_page(self) -> List[Dict]:
        """Парсинг результатов на текущей странице"""
        results = []

        try:
            # Ищем все элементы с результатами
            search_items = self.driver.find_elements(By.CSS_SELECTOR, ".serp-item")
            print(f"📋 Найдено элементов на странице: {len(search_items)}")

            for i, item in enumerate(search_items, 1):
                try:
                    # Пропускаем рекламные результаты
                    if self.is_advertisement(item):
                        print(f"  Элемент {i}: реклама - пропускаем")
                        continue

                    # Извлекаем заголовок и ссылку
                    title, url = self.extract_title_and_url(item)

                    if title and url:
                        date_info = self.extract_date_info(item)

                        results.append({
                            'title': title,
                            'url': url,
                            'date': date_info,
                        })

                        print(f"  {i}. {title[:60]}...")

                except Exception as e:
                    print(f"  ⚠️ Ошибка при парсинге элемента {i}: {e}")
                    continue

        except Exception as e:
            print(f"❌ Ошибка при парсинге страницы: {e}")

        return results

    def navigate_to_page(self, query: str, page_number: int, timings: Dict) -> bool:
        """Переход на страницу с изменением параметра p в URL"""
        try:
            if page_number == 1:
                return True  # Первая страница уже загружена

            print(f"📄 Переходим на страницу {page_number}...")

            params = {
                'text': query,
                # 'lr': 213,  # Москва и область
                'p': page_number - 1,  # страницы нумеруются с 0
                'within': 1  # 2 недели
            }

            base_url = "https://yandex.ru/search/"
            search_url = f"{base_url}?{urlencode(params)}"

            print(f"🌐 Переходим по URL: {search_url}")
            self.driver.get(search_url)

            print("⏳ Ждем загрузки результатов...")
            WebDriverWait(self.driver, timings['page_load']).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".serp-item"))
            )

            self.random_sleep(timings['between_pages_min'], timings['between_pages_max'])
            print(f"✓ Успешно перешли на страницу {page_number}")
            return True

        except Exception as e:
            print(f"❌ Ошибка при переходе на страницу {page_number}: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=8, max=15),
        retry=retry_if_exception_type((WebDriverException, TimeoutException))
    )
    def parse(self) -> list[NewsItem]:
        """Основной метод парсинга с использованием Selenium и пагинацией"""

        print(f'\n🔎 YANDEX SCRAPING {self.metadata}')

        if not self.driver:
            raise WebDriverException("Драйвер не инициализирован")

        news_items = []
        timings = self.get_timings()
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
                # Выполняем поиск
                if self.perform_search(query, timings):
                    all_results = []
                    page = 0

                    # Парсим результаты со всех страниц
                    while len(all_results) < max_results:
                        page += 1
                        print(f"      📖 Страница {page}")

                        # Парсим текущую страницу
                        page_results = self.parse_page()
                        all_results.extend(page_results)

                        print(f"      📊 Найдено на странице: {len(page_results)}")
                        print(f"      📊 Всего найдено: {len(all_results)}")

                        # Проверяем лимит
                        if len(all_results) >= max_results:
                            all_results = all_results[:max_results]
                            print(f"      ✅ Достигнут лимит в {max_results} результатов")
                            break

                        # Пробуем перейти на следующую страницу
                        if not self.navigate_to_page(query, page + 1, timings):
                            print(f"      ⚠️ Не удалось перейти на следующую страницу")
                            break

                    # Создаем NewsItem для каждого результата
                    for j, result in enumerate(all_results, 1):
                        news_items.append(
                            NewsItem(
                                source=self.class_name,
                                metadata=self.metadata,
                                url=result['url'],
                                title=result['title'],
                                approved=self.check_approved_source(result['url'])
                            )
                        )
                        print(f"        {j}. {result['title'][:70]}...")

                    print(f"      ✅ Всего уникальных результатов: {len(all_results)}")
                else:
                    print(f"      ❌ Не удалось выполнить поиск")

            except Exception as e:
                print(f"Error processing query '{query}': {e}")
                raise e

            # Пауза между запросами
            if i < total_queries:
                pause = self.random_sleep(
                    timings['between_queries_min'],
                    timings['between_queries_max']
                )
                print(f"      ⏳ Пауза между запросами: {pause:.1f} сек...")

        return news_items