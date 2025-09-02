import fnmatch
import os
import smtplib
import re
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText


def send_archives_via_gmail(gmail_email, gmail_app_password, recipient_email,
                            directory_path, subject_prefix="", body_text="",
                            file_pattern="archive_*", sort_files=True):
    """
    Отправляет архивы через Gmail. Поддерживает структуру archive_001, archive_002, etc.

    Args:
        gmail_email (str): Ваш Gmail адрес
        gmail_app_password (str): Пароль приложения Gmail
        recipient_email (str): Email получателя
        directory_path (str): Путь к директории с архивами
        subject_prefix (str): Префикс для темы письма
        body_text (str): Текст письма
        file_pattern (str): Шаблон для поиска файлов (по умолчанию "archive_*")
        sort_files (bool): Сортировать ли файлы по номеру (True) или по имени (False)

    Returns:
        list: Список отправленных файлов
    """
    print('Отправка архивов по почте...')

    # Настройки Gmail
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587

    # Проверяем существование директории
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Директория {directory_path} не существует")

    # Получаем и сортируем архивные файлы
    archive_files = get_sorted_archive_files(directory_path, file_pattern, sort_files)

    if not archive_files:
        print("В директории не найдено архивных файлов")
        return []

    sent_files = []

    try:
        # Подключаемся к Gmail SMTP серверу
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(gmail_email, gmail_app_password)
        print("Успешное подключение к Gmail")
        print(f"Найдено файлов для отправки: {len(archive_files)}")

        for archive_file in archive_files:
            print(archive_file)
            try:
                # Создаем письмо
                msg = MIMEMultipart()
                msg['From'] = gmail_email
                msg['To'] = recipient_email

                # Формируем тему письма
                subject = f"{subject_prefix}{archive_file}" if subject_prefix else archive_file
                msg['Subject'] = subject

                # Добавляем текст письма
                email_body = body_text if body_text else f"Вложенный файл: {archive_file}"
                msg.attach(MIMEText(email_body, 'plain'))

                # Добавляем вложение
                file_path = os.path.join(directory_path, archive_file)
                attach_file(msg, file_path)

                # Отправляем письмо
                server.sendmail(gmail_email, recipient_email, msg.as_string())
                print(f"✓ Письмо с файлом '{archive_file}' отправлено успешно")
                sent_files.append(archive_file)

            except Exception as e:
                print(f"✗ Ошибка при отправке файла {archive_file}: {e}")

        server.quit()
        print("Отключение от сервера")

    except Exception as e:
        print(f"Ошибка подключения к Gmail: {e}")

    return sent_files


def get_sorted_archive_files(directory_path, file_pattern="archive_*", sort_by_number=True):
    """
    Получает и сортирует архивные файлы.

    Args:
        directory_path (str): Путь к директории
        file_pattern (str): Шаблон для поиска файлов
        sort_by_number (bool): Сортировать по номеру (True) или по имени (False)

    Returns:
        list: Отсортированный список файлов
    """
    archive_files = []

    for file in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file)
        if os.path.isfile(file_path) and is_archive_file(file):
            # Проверяем соответствие шаблону
            if file_pattern == "archive_*" or fnmatch.fnmatch(file, file_pattern):
                archive_files.append(file)

    if sort_by_number:
        # Сортируем по номеру в имени файла (archive_001, archive_002, etc.)
        archive_files.sort(key=extract_number_from_filename)
    else:
        # Сортируем по алфавиту
        archive_files.sort()

    return archive_files


def extract_number_from_filename(filename):
    """
    Извлекает число из имени файла для сортировки.
    Например: archive_001.zip → 1, archive_123.rar → 123
    """
    # Ищем числа в имени файла
    numbers = re.findall(r'\d+', filename)
    if numbers:
        return int(numbers[0])  # Возвращаем первое найденное число
    return 0  # Если чисел нет, ставим в начало


def is_archive_file(filename):
    """Проверяет, является ли файл архивным"""
    archive_extensions = ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2']
    return any(filename.lower().endswith(ext) for ext in archive_extensions)


def attach_file(msg, filepath):
    """Добавляет файл как вложение к письму"""
    with open(filepath, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    filename = os.path.basename(filepath)
    part.add_header('Content-Disposition', f'attachment; filename= {filename}')
    msg.attach(part)


