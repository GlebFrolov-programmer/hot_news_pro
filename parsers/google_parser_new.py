import json
import os
import random
import time
from abc import ABC
import asyncio
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from parsers.base_parser import BaseParser
from news.news_item import NewsItem
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
from selenium.common.exceptions import WebDriverException
from urllib.parse import urlparse, parse_qs


class GoogleParser(BaseParser, ABC):
    def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
        super().__init__()
        self.class_name = 'Google'
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

            # Headless режим (можно вынести в параметры)
            # chrome_options.add_argument("--headless=new")

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

    def accept_cookies(self):
        """Принятие cookies"""
        try:
            cookie_selectors = [
                "//button[contains(., 'Принять все')]",
                "//button[contains(., 'Accept all')]",
                "//button[contains(., 'I agree')]",
            ]

            for selector in cookie_selectors:
                try:
                    cookie_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    cookie_button.click()
                    print("✓ Cookie приняты")
                    return True
                except:
                    continue

            return False

        except Exception as e:
            print(f"⚠️ Ошибка при обработке cookie: {e}")
            return False

    def perform_search(self, query: str, timings: Dict) -> bool:
        """Выполнение поискового запроса"""
        try:
            print(f"🔍 Поиск: '{query}'")
            print("🌐 Открываем Google...")

            # Открываем Google
            self.driver.get("https://www.google.com")
            print("✓ Google загружен")

            # self.random_sleep(2, 3)

            # Принимаем cookies
            # print("🍪 Проверяем cookies...")
            # if self.accept_cookies():
            #     print("✓ Cookies приняты")
            # else:
            #     print("⚠️ Cookies не найдены")

            # Ищем поисковую строку
            print("🔎 Ищем поисковую строку...")
            search_box = WebDriverWait(self.driver, timings['element_wait']).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            print("✓ Поисковая строка найдена")

            # Очищаем и вводим запрос
            print("⌨️ Вводим запрос...")
            search_box.clear()
            for char in query:
                search_box.send_keys(char)
                self.random_sleep(
                    timings['typing_delay_min'],
                    timings['typing_delay_max']
                )
            print("✓ Заполнен поисковый запрос")

            # Нажимаем Enter
            print("⏎ Отправляем запрос...")
            search_box.send_keys(Keys.RETURN)

            # Ждем загрузки результатов
            print("⏳ Ждем загрузки результатов...")
            WebDriverWait(self.driver, timings['page_load']).until(
                EC.presence_of_element_located((By.ID, "search"))
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

    def navigate_to_page(self, page_number: int, timings: Dict) -> bool:
        """Переход на указанную страницу пагинации"""
        try:
            if page_number == 1:
                return True  # Первая страница уже загружена

            print(f"📄 Переходим на страницу {page_number}...")

            # Пробуем разные способы навигации по страницам
            navigation_methods = [
                self._navigate_via_pagination_buttons,
                self._navigate_via_url_parameter
            ]

            for method in navigation_methods:
                if method(page_number, timings):
                    print(f"✓ Успешно перешли на страницу {page_number}")
                    return True

            print(f"❌ Не удалось перейти на страницу {page_number}")
            return False

        except Exception as e:
            print(f"❌ Ошибка при переходе на страницу {page_number}: {e}")
            return False

    def _navigate_via_pagination_buttons(self, page_number: int, timings: Dict) -> bool:
        """Навигация через кнопки пагинации"""
        try:
            # Ищем кнопки пагинации
            pagination_selectors = [
                f"//a[@aria-label='Page {page_number}']",
                f"//a[contains(text(), '{page_number}')]",
                "//td[@class='YyVfkd']/a",
                "//a[@id='pnnext']",
                "//a[contains(@href, 'start=')]"
            ]

            for selector in pagination_selectors:
                try:
                    page_buttons = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_all_elements_located((By.XPATH, selector))
                    )

                    for button in page_buttons:
                        if str(page_number) in button.text or str(page_number) in button.get_attribute(
                                'aria-label') or '':
                            button.click()
                            # Ждем загрузки новой страницы
                            WebDriverWait(self.driver, timings['page_load']).until(
                                EC.presence_of_element_located((By.ID, "search"))
                            )
                            self.random_sleep(1, 2)
                            return True

                except:
                    continue

            return False

        except Exception as e:
            print(f"Ошибка в навигации через кнопки: {e}")
            return False

    def _navigate_via_url_parameter(self, page_number: int, timings: Dict) -> bool:
        """Навигация через параметр URL"""
        try:
            current_url = self.driver.current_url
            parsed_url = urlparse(current_url)
            query_params = parse_qs(parsed_url.query)

            # Вычисляем start параметр для пагинации (10 результатов на страницу)
            start = (page_number - 1) * 10

            # Обновляем параметры URL
            query_params['start'] = [str(start)]

            # Собираем новый URL
            new_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
            new_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"

            # Переходим по новому URL
            self.driver.get(new_url)

            # Ждем загрузки результатов
            WebDriverWait(self.driver, timings['page_load']).until(
                EC.presence_of_element_located((By.ID, "search"))
            )

            self.random_sleep(1, 2)
            return True

        except Exception as e:
            print(f"Ошибка в навигации через URL: {e}")
            return False

    def extract_links(self) -> List[Dict]:
        """Извлечение ссылок с результатов поиска"""
        links = []
        try:
            # Используем BeautifulSoup для парсинга
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Пробуем разные селекторы для результатов
            selectors = ["div.g a", "div.tF2Cxc a", "div.MjjYud a", "h3 a"]

            for selector in selectors:
                results = soup.select(selector)
                if results:
                    for result in results:
                        href = result.get('href', '')
                        if (href and href.startswith('http') and
                                'google.com' not in href and
                                not href.startswith('/')):
                            title = result.get_text(strip=True) or "No title"

                            # Проверяем уникальность URL
                            if not any(link['url'] == href for link in links):
                                links.append({
                                    'url': href,
                                    'title': title[:200]  # Ограничиваем длину заголовка
                                })
                    break

            return links

        except Exception as e:
            print(f"❌ Ошибка при извлечении ссылок: {e}")
            return []

    def get_timings(self) -> Dict:
        """Получение настроек таймингов"""
        default_timings = {
            'page_load': 5,
            'element_wait': 3,
            'typing_delay_min': 0.01,
            'typing_delay_max': 0.03,
            'between_queries_min': 1,
            'between_queries_max': 2,
            'after_search_min': 1,
            'after_search_max': 2,
            'between_pages_min': 1,
            'between_pages_max': 3,
        }

        # Можно добавить логику для кастомных таймингов из parameters
        if self.parameters.get('timings'):
            default_timings.update(self.parameters['timings'])

        return default_timings

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=8, max=15),
        retry=retry_if_exception_type((WebDriverException, TimeoutException))
    )
    def parse(self) -> list[NewsItem]:
        """Основной метод парсинга с использованием Selenium и пагинацией"""

        print(f'\nGOOGLE SCRAPING {self.metadata}')

        if not self.driver:
            raise WebDriverException("Драйвер не инициализирован")

        news_items = []
        timings = self.get_timings()
        total_queries = len(self.requests_to_parse)

        for i, request in enumerate(self.requests_to_parse, 1):
            query = request['query'] if isinstance(request, dict) else request

            # Определяем лимит результатов и количество страниц
            if isinstance(request, dict) and 'search_limit' in request:
                max_results = request['search_limit']
            else:
                max_results = self.parameters.get('SEARCH_LIMIT_GOOGLE', 10)

            # Вычисляем количество страниц (10 результатов на страницу)
            pages_to_scrape = (max_results + 9) // 10  # Округление вверх

            print(f'    [{i}/{total_queries}] QUERY: {query}, Страниц: {pages_to_scrape}, Лимит: {max_results}')

            try:
                # Выполняем поиск
                if self.perform_search(query, timings):
                    all_links = []

                    # Проходим по всем страницам пагинации
                    for page in range(1, pages_to_scrape + 1):
                        print(f"      📖 Страница {page}/{pages_to_scrape}")

                        # Переходим на нужную страницу (для первой страницы переход не нужен)
                        if page > 1:
                            if not self.navigate_to_page(page, timings):
                                print(f"      ⚠️ Не удалось перейти на страницу {page}, пропускаем")
                                break

                        # Извлекаем ссылки с текущей страницы
                        page_links = self.extract_links()
                        print(f"      📋 Найдено ссылок на странице: {len(page_links)}")

                        # Добавляем уникальные ссылки
                        for link in page_links:
                            if not any(l['url'] == link['url'] for l in all_links):
                                all_links.append(link)

                        # Проверяем, достигли ли мы лимита
                        if len(all_links) >= max_results:
                            all_links = all_links[:max_results]
                            print(f"      ✅ Достигнут лимит в {max_results} результатов")
                            break

                        # Пауза между страницами
                        if page < pages_to_scrape:
                            pause = self.random_sleep(
                                timings['between_pages_min'],
                                timings['between_pages_max']
                            )
                            print(f"      ⏳ Пауза между страницами: {pause:.1f} сек...")

                    # Создаем NewsItem для каждой ссылки
                    for j, link in enumerate(all_links, 1):
                        news_items.append(
                            NewsItem(
                                source=self.class_name,
                                metadata=self.metadata,
                                url=link['url'],
                                title=link['title'],
                                approved=self.check_approved_source(link['url'])
                            )
                        )
                        print(f"        {j}. {link['title']}")

                    print(f"      ✅ Всего уникальных результатов: {len(all_links)}")
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




#  ИНСТРУМЕНТ SCRAPER.DO

# import json
# import os
# import random
# import time
# from abc import ABC
# import asyncio
# import pandas as pd
# import requests
# import urllib.parse
# from typing import List, Dict, Optional
# from parsers.base_parser import BaseParser
# from news.news_item import NewsItem
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
# from urllib.parse import urlparse, parse_qs, quote
#
#
# class GoogleParser(BaseParser, ABC):
#     def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
#         super().__init__()
#         self.class_name = 'Google'
#         self.requests_to_parse = requests_to_parse
#         self.metadata = metadata
#         self.parameters = parameters
#         self.scrape_do_token = parameters.get('SCRAPE_DO_TOKEN', '')
#
#         if not self.scrape_do_token:
#             raise ValueError("SCRAPE_DO_TOKEN is required for GoogleParser")
#
#         try:
#             self.raw_data = [i for i in list(set(self.parse()))]
#         except RetryError as e:
#             print(f"Parsing failed after retries: {e}, продолжаем работу без результатов.")
#             self.raw_data = []
#
#         self.save_to = save_to
#
#         if save_to['TO_EXCEL']:
#             self.to_excel()
#         if save_to['TO_JSON']:
#             self.to_json()
#         self.print_statistics()
#
#     @property
#     def class_name(self) -> str:
#         return self._class_name
#
#     @class_name.setter
#     def class_name(self, value: str):
#         self._class_name = value
#
#     @property
#     def raw_data(self) -> list:
#         return self._raw_data
#
#     @raw_data.setter
#     def raw_data(self, value: list):
#         self._raw_data = value
#
#     @property
#     def requests_to_parse(self) -> list[str]:
#         return self._requests_to_parse
#
#     @requests_to_parse.setter
#     def requests_to_parse(self, value: list[str]):
#         self._requests_to_parse = value
#
#     @property
#     def metadata(self) -> dict:
#         return self._metadata
#
#     @metadata.setter
#     def metadata(self, value: dict):
#         self._metadata = value
#
#     @property
#     def parameters(self) -> dict:
#         return self._parameters
#
#     @parameters.setter
#     def parameters(self, value: dict):
#         self._parameters = value
#
#     def random_sleep(self, min_time: float, max_time: float):
#         """Случайная задержка между запросами"""
#         sleep_time = random.uniform(min_time, max_time)
#         time.sleep(sleep_time)
#         return sleep_time
#
#     def make_scrape_do_request(self, url: str) -> Optional[str]:
#         """Выполнение запроса через scrape.do API"""
#         try:
#             # Кодируем URL для API
#             encoded_url = quote(url)
#             api_url = f"http://api.scrape.do/?url={encoded_url}&token={self.scrape_do_token}"
#
#             # Добавляем дополнительные параметры если нужно
#             # if self.parameters.get('SCRAPE_DO_PREMIUM'):
#                 # api_url += "&premium=true"
#             # if self.parameters.get('SCRAPE_DO_RENDER'):
#             #     api_url += "&render=true"
#
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
#             }
#
#             response = requests.get(api_url, headers=headers, timeout=30)
#             response.raise_for_status()
#
#             return response.text
#
#         except requests.exceptions.RequestException as e:
#             print(f"❌ Ошибка scrape.do API для {url}: {e}")
#             return None
#
#     def build_google_search_url(self, query: str, page: int = 1) -> str:
#         """Построение URL для поиска в Google"""
#         # Кодируем поисковый запрос
#         encoded_query = quote(query)
#
#         # Вычисляем параметр start для пагинации (10 результатов на страницу)
#         start = (page - 1) * 10
#
#         # Собираем URL
#         base_url = "https://www.google.com/search"
#         url = f"{base_url}?q={encoded_query}&num=10"
#
#         if start > 0:
#             url += f"&start={start}"
#
#         return url
#
#     def extract_links_from_html(self, html: str) -> List[Dict]:
#         """Извлечение ссылок из HTML страницы результатов Google"""
#         from bs4 import BeautifulSoup
#
#         links = []
#         try:
#             soup = BeautifulSoup(html, 'html.parser')
#
#             # Пробуем разные селекторы для результатов Google
#             selectors = ["div.g a", "div.tF2Cxc a", "div.MjjYud a", "h3 a"]
#
#             for selector in selectors:
#                 results = soup.select(selector)
#                 if results:
#                     for result in results:
#                         href = result.get('href', '')
#
#                         # Обрабатываем Google-редиректы
#                         if href.startswith('/url?q='):
#                             href = href.split('/url?q=')[1].split('&')[0]
#                             href = urllib.parse.unquote(href)
#
#                         if (href and href.startswith('http') and
#                                 'google.com' not in href and
#                                 not href.startswith('/')):
#                             title = result.get_text(strip=True) or "No title"
#
#                             # Проверяем уникальность URL
#                             if not any(link['url'] == href for link in links):
#                                 links.append({
#                                     'url': href,
#                                     'title': title[:200]  # Ограничиваем длину заголовка
#                                 })
#                     break
#
#             return links
#
#         except Exception as e:
#             print(f"❌ Ошибка при извлечении ссылок из HTML: {e}")
#             return []
#
#     def get_timings(self) -> Dict:
#         """Получение настроек таймингов"""
#         default_timings = {
#             'between_queries_min': 2,
#             'between_queries_max': 5,
#             'between_pages_min': 1,
#             'between_pages_max': 3,
#         }
#
#         if self.parameters.get('timings'):
#             default_timings.update(self.parameters['timings'])
#
#         return default_timings
#
#     @retry(
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=1, min=8, max=15),
#         retry=retry_if_exception_type((requests.exceptions.RequestException,))
#     )
#     def parse(self) -> list[NewsItem]:
#         """Основной метод парсинга с использованием scrape.do API"""
#
#         print(f'\nGOOGLE SCRAPING (scrape.do) {self.metadata}')
#         print(f'Using token: {self.scrape_do_token[:10]}...')
#
#         news_items = []
#         timings = self.get_timings()
#         total_queries = len(self.requests_to_parse)
#
#         for i, request in enumerate(self.requests_to_parse, 1):
#             query = request['query'] if isinstance(request, dict) else request
#
#             # Определяем лимит результатов и количество страниц
#             if isinstance(request, dict) and 'search_limit' in request:
#                 max_results = request['search_limit']
#             else:
#                 max_results = self.parameters.get('SEARCH_LIMIT_GOOGLE', 10)
#
#             # Вычисляем количество страниц (10 результатов на страницу)
#             pages_to_scrape = (max_results + 9) // 10  # Округление вверх
#
#             print(f'    [{i}/{total_queries}] QUERY: {query}, Страниц: {pages_to_scrape}, Лимит: {max_results}')
#
#             try:
#                 all_links = []
#
#                 # Проходим по всем страницам пагинации
#                 for page in range(1, pages_to_scrape + 1):
#                     print(f"      📖 Страница {page}/{pages_to_scrape}")
#
#                     # Строим URL для поиска
#                     search_url = self.build_google_search_url(query, page)
#                     print(f"      🔗 URL: {search_url}")
#
#                     # Выполняем запрос через scrape.do
#                     html_content = self.make_scrape_do_request(search_url)
#
#                     if not html_content:
#                         print(f"      ❌ Не удалось получить данные для страницы {page}")
#                         continue
#
#                     # Извлекаем ссылки из HTML
#                     page_links = self.extract_links_from_html(html_content)
#                     print(f"      📋 Найдено ссылок на странице: {len(page_links)}")
#
#                     # Добавляем уникальные ссылки
#                     for link in page_links:
#                         if not any(l['url'] == link['url'] for l in all_links):
#                             all_links.append(link)
#
#                     # Проверяем, достигли ли мы лимита
#                     if len(all_links) >= max_results:
#                         all_links = all_links[:max_results]
#                         print(f"      ✅ Достигнут лимит в {max_results} результатов")
#                         break
#
#                     # Пауза между страницами
#                     if page < pages_to_scrape:
#                         pause = self.random_sleep(
#                             timings['between_pages_min'],
#                             timings['between_pages_max']
#                         )
#                         print(f"      ⏳ Пауза между страницами: {pause:.1f} сек...")
#
#                 # Создаем NewsItem для каждой ссылки
#                 for j, link in enumerate(all_links, 1):
#                     news_items.append(
#                         NewsItem(
#                             source=self.class_name,
#                             metadata=self.metadata,
#                             url=link['url'],
#                             title=link['title'],
#                             approved=self.check_approved_source(link['url'])
#                         )
#                     )
#                     print(f"        {j}. {link['title']}")
#
#                 print(f"      ✅ Всего уникальных результатов: {len(all_links)}")
#
#             except Exception as e:
#                 print(f"Error processing query '{query}': {e}")
#                 # Не прерываем выполнение для других запросов
#                 continue
#
#         return news_items
