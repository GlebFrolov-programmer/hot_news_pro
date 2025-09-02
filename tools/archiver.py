import os
import zipfile
import glob
from typing import List


def create_archives(directory: str, extensions: List[str], max_size_mb: float):
    """
    Создает архивы из файлов с указанными расширениями

    Args:
        directory: Путь к директории с файлами
        extensions: Список расширений файлов (например, ['xlsx', 'xls', 'csv'])
        max_size_mb: Максимальный размер архива в МБ
    """
    # Проверяем существование директории
    if not os.path.isdir(directory):
        print(f"Ошибка: Директория '{directory}' не существует")
        return

    # Проверяем расширения
    if not extensions:
        print("Ошибка: Не указаны расширения файлов")
        return

    # Нормализуем расширения (убираем точки и приводим к нижнему регистру)
    extensions = [ext.lower().replace('.', '') for ext in extensions if ext.strip()]

    max_size_bytes = max_size_mb * 1024 * 1024
    archive_number = 1

    # Получаем все файлы с указанными расширениями
    all_files = []
    for extension in extensions:
        pattern = os.path.join(directory, f"*.{extension}")
        files = glob.glob(pattern)
        all_files.extend(files)

    if not all_files:
        print(f"Файлы с расширениями {extensions} не найдены в {directory}")
        return

    # Сортируем файлы по размеру для оптимального заполнения
    all_files.sort(key=os.path.getsize, reverse=True)
    print(f"Найдено {len(all_files)} файлов для архивирования")

    current_archive_files = []
    current_archive_size = 0

    for file_path in all_files:
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        # Пропускаем файлы, которые больше максимального размера
        if file_size > max_size_bytes:
            print(f"Пропускаем {file_name} - слишком большой ({file_size / 1024 / 1024:.2f} MB)")
            continue

        # Если добавление файла превысит лимит, создаем новый архив
        if current_archive_size + file_size > max_size_bytes and current_archive_files:
            _create_zip_archive(directory, current_archive_files, archive_number)
            archive_number += 1
            current_archive_files = []
            current_archive_size = 0

        current_archive_files.append(file_path)
        current_archive_size += file_size

    # Создаем архив для оставшихся файлов
    if current_archive_files:
        _create_zip_archive(directory, current_archive_files, archive_number)

    print("Архивирование завершено!")


def _create_zip_archive(directory: str, files: List[str], archive_number: int):
    """Создает ZIP архив с заданными файлами"""
    archive_name = os.path.join(directory, f"archive_{archive_number:03d}.zip")

    try:
        with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                zipf.write(file_path, os.path.basename(file_path))

        archive_size = os.path.getsize(archive_name) / 1024 / 1024
        file_names = [os.path.basename(f) for f in files]
        print(f"Создан архив #{archive_number}: {archive_name}")
        print(f"  Размер: {archive_size:.2f} MB")
        print(f"  Файлы: {len(files)}")
        print(f"  Содержимое: {', '.join(file_names[:3])}{'...' if len(files) > 3 else ''}")
        print()

    except Exception as e:
        print(f"Ошибка при создании архива {archive_name}: {e}")
