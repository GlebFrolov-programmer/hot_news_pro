import os
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
from datetime import date

from parsers.base_parser import BaseParser
from news.news_item import NewsItem
from tools.normalize_data import clean_text


class TelegramParser(BaseParser):
    def __init__(self, requests_to_parse: list[str], parameters: dict, metadata: dict, save_to: dict):
        super().__init__()
        self.class_name = 'Telegram'
        self.requests_to_parse = requests_to_parse  # список поисковых запросов (может включать категории, регионы, периоды)
        self.metadata = metadata
        self.parameters = parameters
        self.save_to = save_to
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
            search_limit = self.parameters['SEARCH_LIMIT_TELEGRAM']
            if not isinstance(search_limit, int):
                search_limit = 100
        except KeyError:
            search_limit = 100

        return search_limit

    def get_date_from(self) -> datetime.date:
        date_from = self.metadata.get('DATE_FROM', datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0,
                                                                                      microsecond=0))
        if isinstance(date_from, str):
            try:
                date_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc,
                                                                             hour=0,
                                                                             minute=0,
                                                                             second=0,
                                                                             microsecond=0)
            except ValueError:
                print("Ошибка: неверный формат даты, должна быть строка в формате 'YYYY-MM-DD'")
                raise

        return date_from

    @staticmethod
    def get_title_from_post(text):
        """Метод возвращает либо жирный текст, либо первое предложение"""
        title = ''
        try:
            if '**' in text:
                title = [i for i in text.split('**') if i != ''][1]
            else:
                title = text.split('.')[0]

            return title[:64]
        except Exception:
            return ''


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=10),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    async def _get_channel_messages(self, channel_name, search_limit, date_from):
        load_dotenv()

        # Настройки клиента
        api_id = self.parameters['AUTHENTICATION']['TELEGRAM_API_ID']
        api_hash = self.parameters['AUTHENTICATION']['TELEGRAM_API_HASH']
        phone = self.parameters['AUTHENTICATION']['PHONE_NUM']
        session_name = os.getenv('SESSION_NAME', 'default_session')

        async with TelegramClient(session_name, api_id, api_hash) as client:
            await client.start(phone)

            try:
                channel = await client.get_entity(channel_name)
                # search_limit = self.get_limit_search()
                messages = []
                async for message in client.iter_messages(channel):
                    if message.date > date_from and search_limit > len(messages):
                        if message.text:
                            messages.append(
                                NewsItem(
                                    source=self.class_name,
                                    metadata=self.metadata,
                                    url=self.parameters.get('TEMPLATE_URL_TELEGRAM',
                                                            "https://t.me/s/{CHANNEL_NAME}").format(
                                        CHANNEL_NAME=channel_name),
                                    title=self.get_title_from_post(message.text),
                                    raw_data=message.text,
                                    approved=self.check_approved_source(channel_name)
                                )
                            )
                    else:
                        break

                return messages

            except ChannelPrivateError:
                print(f"Ошибка: Канал {channel_name} приватный или у вас нет доступа.")
                return None
            except Exception as e:
                print(f"Ошибка в канале {channel_name}: {e}")
                return None

    async def _process_channels(self, channel_list):
        all_messages = []

        date_from = self.get_date_from()

        for request in channel_list:

            channel = request['query'].split('/')[-1]
            print(f"    CHANNEL {channel}: ", end='')
            try:
                search_limit = request["search_limit"]
            except Exception as e:
                search_limit = self.get_limit_search()

            messages = await self._get_channel_messages(channel, search_limit, date_from)

            if messages:
                all_messages.extend(messages)
                print(f"{len(messages)} сообщений")
            else:
                print(f"Не удалось получить сообщения из {channel}")

        return all_messages

    def parse(self) -> list[NewsItem]:
        """Реализация парсинга Telegram каналов"""
        print(f'\nTELEGRAM SCRAPING {self.metadata}')

        return asyncio.run(self._process_channels(self.requests_to_parse))
