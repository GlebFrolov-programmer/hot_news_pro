import asyncio
import os
import re
from dataclasses import dataclass, field
import hashlib
import json
import glob
from typing import List, Dict, Any
from tqdm.asyncio import tqdm_asyncio

# from parsers.google_parser import GoogleParser
from parsers.google_parser_new import GoogleParser
from parsers.tavily_parser import TavilyParser
from parsers.telegram_parser import TelegramParser
# from parsers.website_parser import fill_raw_data_by_parse_websites_async
from parsers.website_parser import WebsiteParser
# from parsers.website_parser_old import WebsiteParser


@dataclass
class ContainerNewsItem:
    """Контейнер для информации о задаче сбора данных"""
    container_name: str
    to_parse: Dict[str, list]
    metadata: Dict[str, Any]
    parameters: Dict[str, str]
    save_to: Dict[str, bool]
    post_processing: List
    _config_hash: str = field(init=False, repr=False)

    def __post_init__(self):
        # Для стабильности сериализуем словари в json с сортировкой ключей
        to_parse_serialized = json.dumps(self.to_parse, sort_keys=True)
        metadata_serialized = json.dumps(self.metadata, sort_keys=True)
        hash_input = (to_parse_serialized + metadata_serialized).encode('utf-8')
        self._config_hash = hashlib.md5(hash_input).hexdigest()

    @property
    def config_hash(self):
        return self._config_hash

    def __hash__(self) -> int:
        return hash((frozenset((k, tuple(v)) for k, v in self.to_parse.items()),
                     frozenset(self.metadata.items())))

    def __eq__(self, other):
        if not isinstance(other, ContainerNewsItem):
            return False
        return self.to_parse == other.to_parse and self.metadata == other.metadata

    def parse_processed_data(self) -> None:

        print('**** SCRAPING PROCESSED DATA ****')
        results = []

        # Маппинг названий источников из to_parse в классы парсеров
        parser_map = {
            'Google': GoogleParser,
            'Tavily': TavilyParser,
            'Telegram': TelegramParser,
        }

        for source, queries_or_urls in self.to_parse.items():
            parser_class = parser_map.get(source)
            if parser_class is None:
                # Неизвестный или неподдерживаемый источник
                print(f"Unknown source '{source}' — пропуск.")
                continue

            folder = self.parameters.get('OUTPUT_DIR_PROCESSED', '')
            if not (self.check_existed_data_in_folder(source, folder)):
                parser_class(requests_to_parse=self.to_parse[source],
                             parameters=self.parameters,
                             metadata=self.metadata,
                             save_to=self.save_to
                             )
            else:
                print(f'\n     >> SKIPPING {source} {self.metadata}, because files already exist!')
            # results.append(parser_instance.raw_data)

    @staticmethod
    def get_distinct_data(
            data: List[Dict[str, Any]],
            unique_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список уникальных словарей из data,
        уникальность определяется по значениям полей из unique_fields.

        :param data: Список словарей для фильтрации.
        :param unique_fields: Список полей, по которым определяется уникальность.
        :return: Список уникальных словарей.
        """
        seen = set()
        distinct_data = []

        for item in data:
            # Формируем ключ из значений полей unique_fields
            try:
                key = tuple(item[field] for field in unique_fields)
            except KeyError as e:
                # Если какого-то поля нет, можно пропустить или обработать иначе
                print(f"Отсутствует поле {e} в записи: {item}, пропускаем.")
                continue

            if key not in seen:
                seen.add(key)
                distinct_data.append(item)

        return distinct_data


    async def fill_raw_data_by_parse_websites_async(self, data: List[Dict[str, Any]], max_concurrent: int = 5,
                                                    process_timeout: int = 15000, show_browser: bool = False) -> List[
        Dict[str, Any]]:
        print(f"Общее количество записей: {len(data)}")

        # Разделяем на две части
        to_parse = [item for item in data if not item.get('raw_data')]
        already_filled = [item for item in data if item.get('raw_data')]

        if not to_parse:
            print("Все записи уже заполнены, парсинг не требуется.")
            return data

        semaphore = asyncio.Semaphore(max_concurrent)

        async def parse_item(item: Dict[str, Any]):
            async with semaphore:
                try:
                    async with WebsiteParser(page_load_timeout=process_timeout,
                                                  show_browser=show_browser) as parser:
                        result = await parser.parse(item['url'])
                        item['raw_data'] = result if result else ''
                except Exception as e:
                    print(f"Ошибка парсинга {item['url']}: {e}")
                    item['raw_data'] = ''

        tasks = [parse_item(item) for item in to_parse]
        await tqdm_asyncio.gather(*tasks, desc="Парсинг сайтов")

        # Объединяем обратно списки
        combined = already_filled + to_parse
        return combined

    def fill_raw_data_by_parse_websites(self, full_data: List[Dict[str, Any]],
                                        max_threads: int,
                                        page_load_timeout: int = 15000,
                                        show_browser: bool = True) -> List[Dict[str, Any]]:
        # Используем нашу асинхронную реализацию
        return asyncio.run(self.fill_raw_data_by_parse_websites_async(
            data=full_data,
            max_concurrent=max_threads,
            process_timeout=page_load_timeout,
            show_browser=show_browser
        ))
    # async def fill_raw_data_by_parse_websites_async(self, data: List[Dict[str, Any]], max_concurrent: int = 5,
    #                                                 process_timeout: int = 15000, show_browser: bool = False) -> List[Dict[str, Any]]:
    #     print(f"Общее количество записей: {len(data)}")
    #
    #     # Разделяем на две части
    #     to_parse = [item for item in data if not item.get('raw_data')]
    #     already_filled = [item for item in data if item.get('raw_data')]
    #
    #     if not to_parse:
    #         print("Все записи уже заполнены, парсинг не требуется.")
    #         return data
    #
    #     async with WebsiteParser() as parser:
    #         semaphore = asyncio.Semaphore(max_concurrent)
    #
    #         async def parse_item(item: Dict[str, Any]):
    #             async with semaphore:
    #                 try:
    #                     result = await asyncio.wait_for(parser.parse(item['url']), timeout=process_timeout / 1000)
    #                     item['raw_data'] = result if result else ''
    #                 except Exception as e:
    #                     print(f"Ошибка парсинга {item['url']}: {e}")
    #                     item['raw_data'] = ''
    #
    #         tasks = [parse_item(item) for item in to_parse]
    #         await tqdm_asyncio.gather(*tasks, desc="Парсинг сайтов")
    #
    #     # Объединяем обратно списки
    #     combined = already_filled + to_parse
    #     return combined
    #
    # def fill_raw_data_by_parse_websites(self, full_data: List[Dict[str, Any]],
    #                                     max_threads: int,
    #                                     page_load_timeout: int = 15000,
    #                                     show_browser: bool = True) -> List[Dict[str, Any]]:
    #     return asyncio.run(fill_raw_data_by_parse_websites_async(data=full_data,
    #                                                              max_concurrent=max_threads,
    #                                                              page_load_timeout=page_load_timeout,
    #                                                              show_browser=show_browser
    #                                                              ))
        # return asyncio.run(self.fill_raw_data_by_parse_websites_async(data=full_data,
        #                                                               max_concurrent=max_threads,
        #                                                               process_timeout=page_load_timeout,
        #                                                               show_browser=show_browser
        #                                                                 ))

    def parse_raw_data(self,
                       max_threads: int,
                       page_load_timeout: int = 15000,
                       show_browser: bool = True):
        print('\n**** PARSING RAW DATA FROM JSON FILES ****\n')
        folder = self.parameters.get('OUTPUT_DIR_RAW', '')
        if not folder or not os.path.isdir(folder):
            print(f"Папка для обработанных данных не найдена: {folder}")
            return []

        full_data = []
        filename_template = f"RAW_{self.parameters.get('TEMPLATES_FILENAME_BASE').format(**self.metadata)}.json"

        # Проверяем наличие файла с паттерном RAW_{template}.json
        raw_file_path = os.path.join(folder, filename_template)
        if os.path.isfile(raw_file_path):
            print(f"     >> SKIPPING {filename_template}, because files already exist!")
            return []

        print(f'Using data from: {list(self.to_parse.keys())}')
        for source in self.to_parse.keys():
            folder = self.parameters.get('OUTPUT_DIR_PROCESSED', '')
            filename_templates = self.parameters.get('TEMPLATES_FILENAME', {})
            filename_template = filename_templates.get(source)
            extensions = []
            try:
                if self.save_to.get('TO_JSON', False):
                    extensions.append('json')
            except Exception as e:
                print(f"Ошибка чтения save_to параметров: {e}")
                continue

            for ext in extensions:
                pattern = os.path.join(folder, f"{source}_{filename_template.format(**self.metadata)}.{ext}")
                files = glob.glob(pattern)

                if not files:
                    print(f"Файлы для {source} с шаблоном {pattern} не найдены.")
                    continue

                for filepath in files:
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                full_data.extend(data)
                            else:
                                full_data.append(data)
                    except Exception as e:
                        print(f"Ошибка при чтении файла {filepath}: {e}")

        # Удаление дубликатов
        full_data = self.get_distinct_data(full_data, ['url', 'raw_data'])
        
        # Исправление метаданных после сборки (например тг собирается только один раз, поэтому надо исправить регион)
        full_data = self.fix_metadata(full_data)

        # Заполнение данных из сайтов
        full_data = self.fill_raw_data_by_parse_websites(full_data=full_data,
                                                         max_threads=max_threads,
                                                         page_load_timeout=page_load_timeout,
                                                         show_browser=show_browser)

        # Сохранение в json
        self.to_json(full_data, 'RAW')

        return full_data

    def parse_post_processing(self):
        print('\n**** POST PROCESSING RAW DATA ****\n')
        folder = self.parameters.get('OUTPUT_DIR_POST_PROCESSING', '')
        if not folder or not os.path.isdir(folder):
            print(f"Папка с данными постобработки не найдена: {folder}")
            return []

        filename_template = f"POST_PROCESSING_{self.parameters.get('TEMPLATES_FILENAME_BASE').format(**self.metadata)}.json"
        post_processing_file_path = os.path.join(folder, filename_template)

        if os.path.isfile(post_processing_file_path):
            print(f"     >> SKIPPING {filename_template}, because file already exist!")
            return []

        try:
            folder = self.parameters.get('OUTPUT_DIR_RAW', '')
            filename_template = f"RAW_{self.parameters.get('TEMPLATES_FILENAME_BASE').format(**self.metadata)}.json"
            raw_file_path = os.path.join(folder, filename_template)
            with open(raw_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    full_data = data
                else:
                    full_data = [data]
        except Exception as e:
            print(f"Ошибка при чтении файла {raw_file_path}: {e}")
            return []

        # Применяем функции постобработки, если есть
        if hasattr(self, 'post_processing') and isinstance(self.post_processing, list):
            for func in self.post_processing:
                try:
                    full_data = func(full_data, parameters=self.parameters)
                except Exception as e:
                    print(f"Ошибка при применении постобработки {func}: {e}")

        self.to_json(full_data, 'POST_PROCESSING')
        return full_data

    def check_existed_data_in_folder(self, source: str, folder: str) -> bool:

        # Получаем шаблон имени файла для данного источника из TEMPLATES_FILENAME
        filename_template = self.parameters.get('TEMPLATES_FILENAME', {}).get(source)
        if not filename_template:
            # Если нет шаблона для источника — считаем, файл отсутствует
            return False

        try:
            extensions = {'.json': self.save_to['TO_JSON'],
                          '.xlsx': self.save_to['TO_EXCEL']}
            enabled_extensions = [ext for ext, enabled in extensions.items() if enabled]
        except KeyError as e:
            # Если не хватает параметра для форматирования — файл считается отсутствующим
            print(f"Missing key {e} for filename formatting")
            return False
        filepaths = [os.path.join(folder, f'{source}_{filename_template.format(**self.metadata)}{ext}') for ext in enabled_extensions]

        return any(os.path.isfile(path) for path in filepaths)

    def print_statistics(self, stage: str):
        # print("=== Статистика ContainerNewsItem ===")
        # for attr, value in self.__dict__.items():
        #     print(f"{attr}: {value}")
        # print("===================================")
        print(f'\n*********\n')
        print(f'КОНТЕЙНЕР ({stage}) {self.container_name}: {self.metadata}')
        print(f'{self.save_to}')
        print(f'\n')

    def to_json(self, raw_data, folder):
        # Данные для сохранения — список словарей с полями объектов
        data_for_json = raw_data

        # Формируем путь к файлу с расширением .json
        file_name = self.parameters['TEMPLATES_FILENAME_BASE'].format(**self.metadata)
        filepath = os.path.join(self.parameters[f'OUTPUT_DIR_{folder}'],
                                f"{folder}_{file_name}.json")

        # Сохраняем в JSON с отступами и utf-8
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data_for_json, f, ensure_ascii=False, indent=4)

        print(f"    >> Data JSON was saved!")

    def fix_metadata(self, full_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Обновляет словарь метаданных для каждого элемента в full_data.

        :param full_data: Список словарей с данными, которые нужно обновить
        :param metadata: Словарь с метаданными для обновления
        :return: Обновленный список данных
        """
        if not full_data:
            return full_data

        for item in full_data:
            # Если у элемента уже есть метаданные, обновляем их
            if 'metadata' in item and isinstance(item['metadata'], dict):
                item['metadata'].update(self.metadata)
            else:
                # Если метаданных нет, создаем новый словарь
                item['metadata'] = self.metadata.copy()

        return full_data
