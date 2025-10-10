import re
from urllib.parse import urlparse, parse_qs, urlencode


def modify_urls(data: list[dict], **kwargs) -> list[dict]:
    """
    Модифицирует значения ключа 'url' в списке словарей:
    - удаляет протокол;
    - удаляет ведущий 'www.' если есть;
    - разбивает строку по разделителям '.' и '/';
    меняет значение 'url' на список частей.

    Функция меняет данные на месте.
    """
    print('    ** Modify urls for security **')
    protocol_pattern = re.compile(r'^\w+://')
    www_pattern = re.compile(r'^www\.')

    for item in data:
        url = item.get('url', '')
        if not isinstance(url, str) or url == '':
            continue

        # Удаляем протокол
        url_no_protocol = protocol_pattern.sub('', url)
        # Удаляем ведущий www.
        url_no_www = www_pattern.sub('', url_no_protocol)

        # Разбиваем по точкам и слешам
        parts = re.split(r'[./]+', url_no_www)
        parts_filtered = '/'.join([part for part in parts if part])

        item['url'] = parts_filtered

    return data


def parse_urls_to_dict(data: list[dict], **kwargs) -> list[dict]:
    """
    Парсит значения ключа 'url' в списке словарей и создает структурированный словарь:
    - protocol (протокол)
    - domain_name (название домена: regnum)
    - domain_zone (зона домена: ru, com, net)
    - subdomain (поддомен, если есть)
    - path (путь)
    - query_params (параметры как словарь)
    - fragment (якорь)

    Добавляет новый ключ 'url_parts' со словарем компонентов.
    Исходные данные не меняются.
    """
    print('    ** Parse urls to structured dict **')

    for item in data:
        url = item.get('url', '')
        if not isinstance(url, str) or url == '':
            item['url_parts'] = {}
            continue

        try:
            parsed = urlparse(url)

            # Разбираем домен на название и зону
            netloc = parsed.netloc
            subdomain = None
            domain_name = None
            domain_zone = None

            if netloc:
                # Убираем www.
                netloc_clean = re.sub(r'^www\.', '', netloc)
                domain_parts = netloc_clean.split('.')

                if len(domain_parts) >= 2:
                    # Берем последнюю часть как зону, предпоследнюю как название
                    domain_zone = domain_parts[-1]
                    domain_name = domain_parts[-2]

                    # Остальные части - поддомены
                    if len(domain_parts) > 2:
                        subdomain = '.'.join(domain_parts[:-2])

            # Парсим query-параметры в словарь
            query_params = {}
            if parsed.query:
                raw_query = parse_qs(parsed.query)
                for key, value in raw_query.items():
                    query_params[key] = value[0] if len(value) == 1 else value

            # Создаем словарь с компонентами URL
            url_parts = {
                'protocol': parsed.scheme or None,
                'subdomain': subdomain,
                'domain_name': domain_name,
                'domain_zone': domain_zone,
                'path': parsed.path or None,
                'query_params': query_params,
                'fragment': parsed.fragment or None
            }

            item['url'] = url_parts

        except Exception as e:
            print(f"Warning: Could not parse URL '{url}': {e}")
            item['url'] = {}

    return data


def build_url_from_dict(url_parts: dict[str], **kwargs) -> str:
    """
    Собирает URL строку из словаря с компонентами.

    Args:
        url_parts: Словарь с компонентами URL

    Returns:
        Собранная URL строка
    """
    if not url_parts:
        return ""

    # Извлекаем компоненты
    protocol = url_parts.get('protocol', 'https')
    subdomain = url_parts.get('subdomain')
    domain_name = url_parts.get('domain_name')
    domain_zone = url_parts.get('domain_zone')
    path = url_parts.get('path', '')
    query_params = url_parts.get('query_params', {})
    fragment = url_parts.get('fragment', '')

    # Собираем домен
    domain_parts = []
    if subdomain:
        domain_parts.append(subdomain)
    if domain_name and domain_zone:
        domain_parts.append(f"{domain_name}.{domain_zone}")
    elif domain_name:
        domain_parts.append(domain_name)
    elif domain_zone:
        domain_parts.append(domain_zone)

    netloc = '.'.join(domain_parts) if domain_parts else ''

    # Добавляем www если есть поддомен и это не www
    if netloc and not any(part.startswith('www') for part in domain_parts) and len(domain_parts) == 1:
        netloc = 'www.' + netloc

    # Преобразуем query-параметры в строку
    query_string = ""
    if query_params:
        query_params_list = []
        for key, value in query_params.items():
            if isinstance(value, list):
                for item in value:
                    query_params_list.append((key, str(item)))
            else:
                query_params_list.append((key, str(value)))
        query_string = urlencode(query_params_list)

    # Собираем полный URL
    url = ""
    if protocol and netloc:
        url = f"{protocol}://{netloc}"

    if path:
        url += path

    if query_string:
        url += f"?{query_string}"

    if fragment:
        url += f"#{fragment}"

    return url


# Функция для работы со списком словарей
def build_urls_from_dict(data: list[dict], **kwargs) -> list[dict]:
    """
    Собирает URL из словарей в списке и добавляет обратно.

    Args:
        data: Список словарей с распарсенными URL
        url_parts_key: Ключ, где хранится словарь с компонентами URL
        result_key: Ключ для сохранения собранного URL

    Returns:
        Список словарей с добавленными URL
    """
    url_parts_key: str = 'url'
    result_key: str = 'url_reconstructed'

    for item in data:
        url_parts = item.get(url_parts_key, {})

        if url_parts:
            url = build_url_from_dict(url_parts)
            item[result_key] = url
        else:
            item[result_key] = ""

    return data


def filter_raw_data_by_region(data: list[dict], **kwargs) -> list[dict]:
    """
    Фильтрует список словарей, оставляя только те, где в raw_data
    есть хотя бы одно ключевое слово из kwargs['parameters']['REGION_KEYS'].
    Если ключи не найдены, возвращает исходный список.
    """
    print('    ** Filter raw data by region keywords **')
    try:
        # Получаем ключевые слова региона из kwargs
        region_keys = kwargs.get('parameters', {}).get('REGION_KEYS', [])
        if not region_keys:
            print("Ключевые слова региона не найдены в parameters, возвращаем исходные данные.")
            return data

        # Формируем паттерн для поиска (через "|", регистр игнорируется)
        pattern = '|'.join(re.escape(kw.lower()) for kw in region_keys)
        regex = re.compile(pattern, re.IGNORECASE)

        filtered_data = []
        for item in data:
            raw = item.get('raw_data', '')
            if isinstance(raw, str) and regex.search(raw):
                filtered_data.append(item)

        return filtered_data

    except Exception as e:
        print(f"Ошибка при фильтрации по ключевым словам региона: {e}")
        return data


def clean_sensitive_content(data: list[dict], **kwargs) -> list[dict]:
    """
    Комплексная очистка всех строковых полей от запрещенного контента:
    - Удаление URL-адресов (http, https, www)
    - Удаление чувствительной информации (ИНН, БИК, ОГРН и т.д.)
    - Модификация оставшихся URL-подобных строк
    - Нормализация пробелов

    Функция меняет данные на месте.
    """
    print('    ** Cleaning sensitive content from all fields **')

    # Паттерны для поиска
    url_pattern = r'https?://\S+|www\.\S+'
    sensitive_words_pattern = r'\b(ИНН|БИК|ОГРН|Паспорт|СНИЛС|КПП|Карта|Телефон|Email)\b'
    protocol_pattern = re.compile(r'^\w+://')
    www_pattern = re.compile(r'^www\.')

    for item in data:
        for key, value in item.items():
            if not isinstance(value, str) or not value.strip():
                continue

            # 1. Удаляем полные URL
            cleaned_value = re.sub(url_pattern, '', value, flags=re.IGNORECASE)

            # 2. Удаляем чувствительные слова
            cleaned_value = re.sub(sensitive_words_pattern, '', cleaned_value, flags=re.IGNORECASE)

            # 3. Модифицируем оставшиеся URL-подобные строки
            if protocol_pattern.search(cleaned_value) or www_pattern.search(cleaned_value):
                # Удаляем протокол
                value_no_protocol = protocol_pattern.sub('', cleaned_value)
                # Удаляем ведущий www
                value_no_www = www_pattern.sub('', value_no_protocol)
                # Разбиваем по точкам и слешам
                parts = re.split(r'[./]+', value_no_www)
                cleaned_value = '/'.join([part for part in parts if part])

            # 4. Нормализуем пробелы и обновляем значение
            item[key] = re.sub(r'\s+', ' ', cleaned_value).strip()

    return data