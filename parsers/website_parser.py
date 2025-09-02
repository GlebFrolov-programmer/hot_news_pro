import asyncio
from typing import List, Optional, Dict, Any
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
import re
from playwright.async_api import async_playwright
import random
import async_timeout
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import time


class WebsiteParser:
    def __init__(self, headless: bool = True, playwright_timeout: int = 3000, selenium_timeout: int = 7000):
        self.headless = headless
        self.playwright_timeout = playwright_timeout  # 3 секунды для Playwright
        self.selenium_timeout = selenium_timeout  # 10 секунд для Selenium
        self.browser = None
        self.context = None
        self.playwright = None
        self.selenium_driver = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self._generate_user_agent()
        )

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.selenium_driver:
            self.selenium_driver.quit()

    def _generate_user_agent(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        return random.choice(agents)

    async def parse(self, url: str) -> Optional[str]:
        # Сначала пробуем парсить через Playwright с таймаутом 3 секунды
        playwright_result = await self._parse_with_playwright(url)

        # Проверяем условия для fallback на Selenium
        if self._should_use_selenium_fallback(playwright_result):
            selenium_result = await self._parse_with_selenium(url)
            return selenium_result

        return playwright_result

    async def _parse_with_playwright(self, url: str) -> Optional[str]:
        page = None
        try:
            # Устанавливаем таймаут 3 секунды для Playwright
            async with async_timeout.timeout(self.playwright_timeout / 1000):
                page = await self.context.new_page()
                await page.goto(url, timeout=self.playwright_timeout, wait_until="domcontentloaded")
                await self._minimal_behavior(page)
                content = await page.content()
                return self._clean_content(content)
        except asyncio.TimeoutError:
            # При таймауте возвращаем None, чтобы перейти к Selenium
            return None
        except Exception:
            # При любой другой ошибке возвращаем None
            return None
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

    async def _parse_with_selenium(self, url: str) -> Optional[str]:
        """Парсинг с использованием Selenium с таймаутом 10 секунд"""
        print('*selenium parsing*')
        try:
            # Инициализируем Selenium драйвер только при необходимости
            if self.selenium_driver is None:
                self._init_selenium_driver()

            # Устанавливаем неявный таймаут для Selenium
            self.selenium_driver.implicitly_wait(self.selenium_timeout / 1000)

            # Устанавливаем таймаут для загрузки страницы
            self.selenium_driver.set_page_load_timeout(self.selenium_timeout / 1000)

            self.selenium_driver.get(url)

            # Ждем загрузки страницы с таймаутом 10 секунд
            WebDriverWait(self.selenium_driver, self.selenium_timeout / 1000).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Получаем HTML контент
            html_content = self.selenium_driver.page_source

            return self._clean_content(html_content)

        except (TimeoutException, WebDriverException, Exception):
            # При любой ошибке возвращаем None
            return None

    def _init_selenium_driver(self):
        """Инициализация Selenium WebDriver"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self._generate_user_agent()}")

        self.selenium_driver = webdriver.Chrome(options=chrome_options)
        self.selenium_driver.set_window_size(1920, 1080)

    def _should_use_selenium_fallback(self, raw_data: Optional[str]) -> bool:
        """Проверяет условия для использования Selenium fallback"""
        if raw_data is None:
            return True
        if not raw_data.strip():
            return True
        if len(raw_data) <= 100:
            return True
        return False

    async def _minimal_behavior(self, page):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(0.3)
        except:
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
            soup = BeautifulSoup(html, 'html.parser')
            for element in soup(['script', 'style', 'nav', 'footer']):
                element.decompose()
            text = soup.get_text(separator='\n', strip=True)
            cleaned_lines = []
            for line in text.splitlines():
                if line.strip() and len(line.strip()) > 10:
                    cleaned_line = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', line.strip())
                    cleaned_lines.append(cleaned_line)
            clean_text = self._remove_sensitive_and_urls('\n'.join(cleaned_lines))
            return clean_text[:50000]
        except Exception:
            return ""


async def parse_single_url_with_timeout(url: str, parser: WebsiteParser, timeout: int) -> Optional[str]:
    try:
        # Используем общий таймаут для всей операции парсинга
        return await asyncio.wait_for(parser.parse(url), timeout=timeout / 1000)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        # При таймауте возвращаем None
        return None
    except Exception:
        # При любой другой ошибке возвращаем None
        return None


async def parse_urls_batch(data: List[Dict[str, Any]], max_concurrent: int = 5, process_timeout: int = 15000) -> List[
    Optional[str]]:
    results = [None] * len(data)

    async with WebsiteParser(playwright_timeout=3000, selenium_timeout=10000) as parser:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_item(index: int, url: str):
            async with semaphore:
                # Пытаемся получить данные с общим таймаутом
                results[index] = await parse_single_url_with_timeout(url, parser, process_timeout)

        tasks = []
        for i, item in enumerate(data):
            url = item.get('url')
            if url:
                tasks.append(asyncio.create_task(process_item(i, url)))

        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Парсинг URL"):
            try:
                await f
            except Exception:
                continue

    return results


async def fill_raw_data_html_async(data: List[Dict[str, Any]], max_concurrent: int = 5, process_timeout: int = 15000) -> \
List[Dict[str, Any]]:
    incomplete_items = [item for item in data if not item.get('raw_data')]
    if not incomplete_items:
        return data

    print(f"Найдено {len(incomplete_items)} URL для парсинга")

    parsed_contents = await parse_urls_batch(incomplete_items, max_concurrent, process_timeout)

    # Обновляем оригинальный список
    parsed_index = 0
    for item in data:
        if not item.get('raw_data'):
            # Если результат None (ничего не спарсилось), устанавливаем пустую строку
            item['raw_data'] = parsed_contents[parsed_index] or ""
            parsed_index += 1

    return data


def fill_raw_data_html(data: List[Dict[str, Any]], max_concurrent: int = 5, process_timeout: int = 15000) -> List[
    Dict[str, Any]]:
    return asyncio.run(fill_raw_data_html_async(data, max_concurrent, process_timeout))