import string
from datetime import datetime, timezone, timedelta, date

from config.settings import APISettings, ParserSettings, StorageSettings, RegionSettings
from itertools import product

from news.news_container import ContainerNewsItem
from tools.post_processing import *


class MacroRegionConfig(APISettings, ParserSettings, StorageSettings, RegionSettings):
    """
    Настройки по выгрузке новостей регионов с шаблонами запросов, категориями и телеграм-каналами.
    """

    def get_variables(self, var_names: list) -> dict:
        templates = {}
        for attr_name in dir(self):
            for var_name in var_names:
                if attr_name.startswith(var_name):
                    attr_value = getattr(self, attr_name)
                    # if isinstance(attr_value, str):  # учитываем только строковые шаблоны
                    templates[attr_name] = attr_value
        return templates

    @staticmethod
    def month_begin_to_period_prev(month_begin: date) -> str:
        months = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }
        # Получаем последний день предыдущего месяца
        last_of_prev = month_begin - timedelta(days=1)
        # Первый день предыдущего месяца
        month_name = months[last_of_prev.month]
        year = last_of_prev.year
        return f"{month_name} {year}"

    SAVE_TO = {
        'TO_EXCEL': True,
        'TO_JSON': False
    }

    MONTH_BEGIN = date.today().replace(day=1)
    MONTH_BEGIN_UTC = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    PERIOD = month_begin_to_period_prev(MONTH_BEGIN)
    DATE_FROM = str(MONTH_BEGIN)

    CONFIG_NAME = 'MacroRegion'

    TEMPLATES_FILENAME_BASE = '{AVAILABLE_CATEGORIES}_{AVAILABLE_REGIONS}_{PERIOD}_{DATE_FROM}'

    TEMPLATES_FILENAME = {'Google': '{AVAILABLE_CATEGORIES}_{AVAILABLE_REGIONS}_{PERIOD}_{DATE_FROM}',
                          'Tavily': '{AVAILABLE_CATEGORIES}_{AVAILABLE_REGIONS}_{PERIOD}_{DATE_FROM}',
                          'Telegram': '{AVAILABLE_CATEGORIES}_BASE_{PERIOD}_{DATE_FROM}'}

    TEMPLATES_PARSE = {'Google': '({SUBCATEGORIES}) {AVAILABLE_REGIONS} {PERIOD} after:{DATE_FROM}',
                       'Tavily': '({SUBCATEGORIES}) {AVAILABLE_REGIONS} {PERIOD}',
                       'Telegram': 'https://t.me/s/{CHANNEL_NAME}'}

    POST_PROCESSING = [filter_raw_data_by_region,
                       modify_urls]

    CATEGORIES_SEARCH = {
        'Тренды на рынке недвижимости': [
            'Обзор рынка недвижимости',
            'Спрос на рынке недвижимости',
            'Новости недвижимости',
            'Анализ рынка недвижимости',
            'Новости ипотеки',
            'Тенденции рынка недвижимости',
        ],
        'Цены на недвижимость': [
            'Цены недвижимости',
            'Почему изменилась цена на недвижимость',
            'Факторы изменения цены на недвижимость'
        ],
        'Первичное жильё': [
            'Новостройки и застройщики',
            'Первичное жильё',
            'Почему изменилась цена на первичное жильё',
            'Ипотека для первичного жилья',
            'Цены и динамика первичного жильё'
        ],
        'Вторичное жильё': [
            'Вторичное жильё',
            'Почему изменилась цена на вторичное жильё',
            'Ипотека для вторичного жилья',
            'Цены и динамика вторичного жильё'
        ],
        'Доступность недвижимости': [
            'Доступность жилья',
            'Барьеры для приобретения жилья',
            'Государственные субсидии и льготы на недвижимость'
        ],
        'Бизнес': [
            'Новости крупного бизнеса',
            'Новости среднего бизнеса',
            'Новости малого бизнеса',
            'Отраслевые тенденции в бизнесе',
            'Изменения законодательства для бизнеса',
            'Государственная поддержка бизнеса'
        ],
        'Фонд оплаты труда': [
            'Новости ФОТ',
            'Новости доходов населения',
            'Зарплаты по отраслям',
            'Анализ динамики зарплат',
        ]
    }

    apartments_channels = [
        "russianmacro",
        "domresearch",
        "okoloCB",
        "domclick",
        "ria_realty",
        "realty_rbc",
        "Jelezobetonniyzames",
        "kvadratnymaster",
        "nedvizha",
        "belaya_kaska",
        "propertyinsider",
        "cian_realtor",
        "avito_re_pro",
        "ipotekahouse",
        "filatofff",
        "pro_smarent",
        "Leonid_Rysev",
        "pataninnews",
        "rudakov_broker",
    ]

    CATEGORIES_TELEGRAM = {
        'Тренды на рынке недвижимости': apartments_channels,
        'Цены на недвижимость': apartments_channels,
        'Доступность недвижимости': apartments_channels,
        'Первичное жильё': apartments_channels,
        'Вторичное жильё': apartments_channels,
    }

    def generate_config_to_parse(self) -> list:
        def extract_keys_from_templates(templates_dict):
            formatter = string.Formatter()
            unique_keys = set()
            for template_str in templates_dict.values():
                keys = {field_name for _, field_name, _, _ in formatter.parse(template_str) if field_name}
                unique_keys.update(keys)
            return list(unique_keys)

        keys = extract_keys_from_templates(self.TEMPLATES_FILENAME)
        config_settings = {}
        for key in keys:
            value = getattr(self, key, None)
            if value is None:
                raise Exception(f'{key} must have value!')
            elif isinstance(value, (list, tuple, set)):
                config_settings[key] = value
            else:
                config_settings[key] = [value]

        keys = list(config_settings.keys())
        values_product = product(*config_settings.values())

        config_to_parse = []


        for combination in values_product:
            item = dict(zip(keys, combination))

            subcategories_cache = {}
            # Добавление подкатегорий
            category = item.get('AVAILABLE_CATEGORIES')
            # if category and category in self.CATEGORIES_SEARCH:
            #     # if category not in subcategories_cache:
            #     subcategories_cache[category] = " OR ".join(self.CATEGORIES_SEARCH[category])
            #     item['SUBCATEGORIES'] = subcategories_cache[category]
            # else:
            #     item['SUBCATEGORIES'] = ""
            self.SUBCATEGORIES = self.CATEGORIES_SEARCH[category]

            to_parse = {}

            for source in self.AVAILABLE_SOURCES:
                # Проверяем, есть ли шаблоны для данного источника
                if source in self.TEMPLATES_PARSE:
                    if source == 'Telegram':
                        channels = self.CATEGORIES_TELEGRAM.get(category, [])
                        if channels:
                            # Формируем список запросов для каналов
                            subqueries = []
                            for channel in channels:
                                subquery = {
                                    'query': self.TEMPLATES_PARSE[source].format(CHANNEL_NAME=channel),
                                    'search_limit': self.SEARCH_LIMIT_TELEGRAM
                                }
                                subqueries.append(subquery)

                            # to_parse[source] = [self.TEMPLATES_PARSE[source].format(CHANNEL_NAME=channel) for channel in channels]
                            to_parse[source] = subqueries

                    elif source == 'Google':
                        filter_categories = []
                        subqueries = []

                        for i, cat in enumerate(self.CATEGORIES_SEARCH[category]):
                            add = filter_categories + [cat]
                            item['SUBCATEGORIES'] = f'{" OR ".join(add)}'
                            # Проверяем длину итогового запроса (в словах) по шаблону
                            if len(self.TEMPLATES_PARSE[source].format(**item).split(' ')) <= 32:
                                filter_categories.append(cat)
                                # Если это последний элемент, добавляем текущий подзапрос в список
                                if i == len(self.SUBCATEGORIES) - 1:
                                    item['SUBCATEGORIES'] = f'{" OR ".join(filter_categories)}'
                                    subquery = {
                                        'query': self.TEMPLATES_PARSE[source].format(**item),
                                        'search_limit': self.SEARCH_LIMIT_GOOGLE * len(filter_categories)
                                    }
                                    subqueries.append(subquery)
                            else:
                                # Если текущий элемент не помещается, добавляем подзапрос из предыдущих категорий
                                item['SUBCATEGORIES'] = f'{" OR ".join(filter_categories)}'
                                subquery = {
                                    'query': self.TEMPLATES_PARSE[source].format(**item),
                                    'search_limit': self.SEARCH_LIMIT_GOOGLE * len(filter_categories)
                                }
                                subqueries.append(subquery)
                                # Начинаем новый подзапрос с текущей категорией
                                filter_categories = [cat]

                                # Если это последний элемент, то добавляем его тоже
                                if i == len(self.SUBCATEGORIES) - 1:
                                    item['SUBCATEGORIES'] = cat
                                    subquery = {
                                        'query': self.TEMPLATES_PARSE[source].format(**item),
                                        'search_limit': self.SEARCH_LIMIT_GOOGLE
                                    }
                                    subqueries.append(subquery)

                        to_parse[source] = subqueries

                    elif source == 'Tavily':

                        filter_categories = []
                        subqueries = []

                        for i, cat in enumerate(self.CATEGORIES_SEARCH[category]):
                            add = filter_categories + [cat]
                            item['SUBCATEGORIES'] = f'{" OR ".join(add)}'
                            # Проверяем длину итоговой строки запроса
                            if len(self.TEMPLATES_PARSE[source].format(**item)) <= 400:
                                filter_categories.append(cat)
                                # Если это последний элемент, добавляем текущий подзапрос
                                if i == len(self.SUBCATEGORIES) - 1:
                                    item['SUBCATEGORIES'] = f'{" OR ".join(filter_categories)}'
                                    subquery = {
                                        'query': self.TEMPLATES_PARSE[source].format(**item),
                                        'search_limit': self.SEARCH_LIMIT_GOOGLE * len(filter_categories)
                                    }
                                    subqueries.append(subquery)
                            else:
                                # Добавляем подзапрос из предыдущих категорий
                                item['SUBCATEGORIES'] = f'{" OR ".join(filter_categories)}'
                                subquery = {
                                    'query': self.TEMPLATES_PARSE[source].format(**item),
                                    'search_limit': self.SEARCH_LIMIT_GOOGLE * len(filter_categories)
                                }
                                subqueries.append(subquery)
                                # Начинаем новый подзапрос с текущей категорией
                                filter_categories = [cat]

                                # Если это последний элемент, добавляем его тоже отдельно
                                if i == len(self.SUBCATEGORIES) - 1:
                                    item['SUBCATEGORIES'] = cat
                                    subquery = {
                                        'query': self.TEMPLATES_PARSE[source].format(**item),
                                        'search_limit': self.SEARCH_LIMIT_GOOGLE
                                    }
                                    subqueries.append(subquery)

                        to_parse[source] = subqueries
            self.REGION_KEYS = self.REGIONS_KEYWORDS[item['AVAILABLE_REGIONS']]
            # Создаём объект ContainerNewsItem — хэш вычислится внутри конструктора
            сontainer_news_item = ContainerNewsItem(
                container_name=self.CONFIG_NAME,
                to_parse=to_parse,
                metadata={k: item[k] for k in keys if k in item},
                post_processing=self.POST_PROCESSING,
                # metadata=item.copy(),
                parameters=self.get_variables(['AUTHENTICATION',
                                               'TEMPLATES',
                                               'SEARCH_LIMIT',
                                               'TRUSTED_SOURCES',
                                               'SUBCATEGORIES',
                                               'OUTPUT',
                                               'REGION_KEYS']),

                save_to=self.SAVE_TO
            )

            config_to_parse.append(сontainer_news_item)

        return config_to_parse


# tasks = [NewsItem1, News_item2, ....]