import re


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

