import time
from datetime import datetime, timezone, date
import warnings

from config import MacroRegionConfig
from tools.archiver import create_archives
from tools.email_sender import send_archives_via_gmail

warnings.filterwarnings("ignore")  # Отключает все warnings

if __name__ == "__main__":

    mr_conf = MacroRegionConfig()

    parser_settings = {
        'AVAILABLE_SOURCES': [
                            'Google',
                            'Tavily',
                            'Yandex',
                            'Telegram'
                              ],
        # 'AVAILABLE_SOURCES': ['Google'],
        # 'AVAILABLE_REGIONS': ['Россия'],
        'AVAILABLE_REGIONS': mr_conf.AVAILABLE_REGIONS,
        'AVAILABLE_CATEGORIES': [
            'Тренды на рынке недвижимости',
            'Доступность недвижимости',
            # 'Цены на недвижимость',
            'Фонд оплаты труда',
            'Бизнес',
            # 'Сельское хозяйство',
        ],
        # 'SEARCH_LIMIT_TELEGRAM': 100,
        # 'PERIOD': 'Октябрь 2025',
        # 'DATE_FROM': '2025-10-01',
        'SAVE_TO': {
            'TO_EXCEL': False,
            'TO_JSON': True
        },
        'MONTH_BEGIN': date.today().replace(day=1),
        'MONTH_BEGIN_UTC': datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    }

    mr_conf.set_parser_settings(parser_settings)

    tasks_to_parse = mr_conf.generate_config_to_parse()

    for task in tasks_to_parse:
        start_time = time.time()

        stage = f'{tasks_to_parse.index(task) + 1} / {len(tasks_to_parse)}'

        task.print_statistics(stage)

        task.parse_processed_data()

        task.parse_raw_data(max_threads=6,
                            page_load_timeout=15000,
                            show_browser=False
                            )

        task.parse_post_processing()


        end_time = time.time()
        total_seconds = end_time - start_time
        minutes = int(total_seconds // 60)
        seconds = round(total_seconds % 60)
        print(f'Время выполнения: {minutes} мин. {seconds} сек.')

    # Архивация всех файлов
    create_archives(
        directory=mr_conf.OUTPUT_DIR_POST_PROCESSING,
        extensions=["json"],
        max_size_mb=80
    )

    # Отправка по почте архивов
    send_archives_via_gmail(
        gmail_email=mr_conf.AUTHENTICATION['GMAIL'],
        gmail_app_password=mr_conf.AUTHENTICATION['PASS_GMAIL'],
        recipient_email=mr_conf.AUTHENTICATION['MAIL_SBER'],
        directory_path=mr_conf.OUTPUT_DIR_POST_PROCESSING,
        subject_prefix="Архив ",
        body_text="Архив: ",
        file_pattern="archive_*.zip",  # Только zip файлы
        sort_files=True  # Сортировать по номеру
    )
