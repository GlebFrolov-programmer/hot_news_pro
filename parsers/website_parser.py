# async_website_parser.py
from typing import Optional
import asyncio
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import random
import logging
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


class WebsiteParser:
    def __init__(self, headless: bool = True, page_load_timeout: int = 10000, show_browser: bool = False):
        self.headless = headless
        self.page_load_timeout = page_load_timeout
        self.show_browser = show_browser
        self.driver = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Асинхронная инициализация Selenium WebDriver"""
        chrome_options = Options()

        if self.headless and not self.show_browser:
            chrome_options.add_argument("--headless=new")
        elif self.show_browser:
            chrome_options.add_argument("--window-size=1920,1080")
        else:
            chrome_options.add_argument("--headless=new")

        # Опции для скорости
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument(f"user-agent={self._generate_user_agent()}")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            # Запускаем в отдельном потоке, так как Selenium синхронный
            loop = asyncio.get_event_loop()
            self.driver = await loop.run_in_executor(None, webdriver.Chrome, chrome_options)

            if not self.show_browser:
                await loop.run_in_executor(None, self.driver.set_window_size, 1920, 1080)
        except Exception as e:
            logger.error(f"Ошибка инициализации драйвера: {e}")
            raise

    async def close(self):
        if self.driver:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.driver.quit)
            except Exception as e:
                logger.error(f"Ошибка при закрытии драйвера: {e}")

    def _generate_user_agent(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        return random.choice(agents)

    async def parse(self, url: str) -> Optional[str]:
        """Асинхронный парсинг URL"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._parse_with_selenium, url)

    def _parse_with_selenium(self, url: str) -> Optional[str]:
        """Синхронный парсинг (выполняется в отдельном потоке)"""
        try:
            timeout_seconds = self.page_load_timeout / 1000
            self.driver.set_page_load_timeout(timeout_seconds)

            start_time = time.time()

            try:
                self.driver.get(url)
                print(f"✓ Загружаем: {url}")
            except TimeoutException:
                print(f"⚠ Страница {url} не полностью загружена, парсим что есть")
                pass
            except Exception as e:
                print(f"✗ Ошибка загрузки {url}: {e}")
                return None

            # Ждем появления body
            body_timeout = max(5, timeout_seconds / 2)
            try:
                WebDriverWait(self.driver, body_timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print(f"⚠ Body не появился для {url}, продолжаем")

            # Даем время для загрузки
            elapsed = time.time() - start_time
            remaining_time = max(0, timeout_seconds - elapsed - 1)
            if remaining_time > 0:
                time.sleep(min(2, remaining_time))

            # Скроллинг
            self._quick_behavior()

            # Получаем HTML
            html_content = self.driver.page_source

            # Очищаем контент
            cleaned_content = self._clean_content(html_content)

            # Выводим результат в реальном времени
            if cleaned_content:
                print(f"✓ Спарсено: {url} → {len(cleaned_content)} символов")
            else:
                print(f"✗ Не удалось спарсить: {url}")

            return cleaned_content

        except Exception as e:
            print(f"✗ Ошибка парсинга {url}: {e}")
            try:
                html_content = self.driver.page_source
                result = self._clean_content(html_content)
                if result:
                    print(f"✓ Спарсено (после ошибки): {url} → {len(result)} символов")
                return result
            except:
                print(f"✗ Критическая ошибка для {url}")
                return None

    def _quick_behavior(self):
        """Быстрый скроллинг"""
        try:
            self.driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(0.3)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.2)
        except Exception:
            # Игнорируем ошибки скроллинга
            pass

    @staticmethod
    def _remove_sensitive_and_urls(text: str) -> str:
        url_pattern = r'https?://\S+|www\.\S+'
        text = re.sub(url_pattern, '', text, flags=re.IGNORECASE)

        sensitive_words_pattern = r'\b(ИНН|БИК|ОГРН|Паспорт|СНИЛС|КПП|Карта|Телефон|Email)\b'
        text = re.sub(sensitive_words_pattern, '', text, flags=re.IGNORECASE)

        return re.sub(r'\s+', ' ', text).strip()

    def _clean_content(self, html: str) -> str:
        try:
            if not html or len(html.strip()) < 100:
                return ""

            soup = BeautifulSoup(html, 'html.parser')

            for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()

            ad_selectors = [
                '[class*="ads"]', '[class*="banner"]', '[class*="advertisement"]',
                '[class*="promo"]', '[id*="ads"]', '[id*="banner"]'
            ]

            for selector in ad_selectors:
                for element in soup.select(selector):
                    element.decompose()

            text = soup.get_text(separator='\n', strip=True)
            cleaned_lines = []

            for line in text.splitlines():
                line = line.strip()
                if (line and len(line) > 5 and
                        not any(x in line.lower() for x in ['cookie', 'реклама', 'ads', 'banner', 'advertisement'])):
                    cleaned_line = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', line)
                    cleaned_lines.append(cleaned_line)

            clean_text = self._remove_sensitive_and_urls('\n'.join(cleaned_lines))
            return clean_text[:50000]

        except Exception:
            try:
                return html[:20000]
            except:
                return ""