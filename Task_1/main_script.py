"""
Этот модуль отвечает за получение исторических данных о погоде для
города Санкт-Петербург, создание и управление Docker-контейнером с
базой данных PostgreSQL, а также запись полученных данных в базу данных.
"""

from datetime import datetime
import subprocess
import time

import openmeteo_requests
import requests_cache
from retry_requests import retry
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine, text

def launch_container(container_name, db_name, db_user, db_password, db_port, db_port_a):
    """
    Запускает контейнер PostgreSQL с использованием Docker Compose.

    :param container_name: Название Docker контейнера.
    :param db_name: Название базы данных.
    :param db_user: Имя пользователя базы данных.
    :param db_password: Пароль пользователя базы данных.
    :param db_port: Порт, на котором будет доступен контейнер.

    :raises RuntimeError: Если контейнер не был запущен после 3 попыток.
    """
    # Установка переменных
    retry_count = 0

    # Установка параметром контейнейра
    docker_compose_content = f"""
    services:
      postgres:
        image: postgres:16
        restart: always
        shm_size: 1024mb
        container_name: "{container_name}"
        environment:
          POSTGRES_USER: "{db_user}"
          POSTGRES_PASSWORD: "{db_password}"
          POSTGRES_DB: "{db_name}"
        ports:
          - "{db_port_a}:{db_port}"
        volumes:
          - pg_data:/var/lib/postgresql/data

    volumes:
      pg_data:
    """

    # Запись docker-compose.yml файла в вирткальное окружение
    with open("docker-compose.yml", "w", encoding="utf-8") as file:
        file.write(docker_compose_content)

    # Запуск Docker Compose для поднятия контейнера
    print("Запуск контейнера Postgres через Docker Compose...")
    subprocess.run(["docker-compose", "up", "-d"], check=True)

    # Ожидание запуска контейнера
    print("Ожидание запуска контейнера Postgres (примерно 10 секунд)...")
    time.sleep(10)

    # Проверка, что контейнер запущен
    while retry_count <= 2:

        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=postgres_weather", "--format", "{{.Names}}"],
                capture_output=True, text=True, check=True
            )

            # Проверка вывода команды
            if container_name in result.stdout:
                print(f"Контейнер {container_name} запущен.")
                return "Success"
            else:
                print(f"Контейнер {container_name} не запущен. Повторная проверка через 10 секунд.")
                retry_count += 1
                time.sleep(10)
                continue
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при проверке состояния контейнера: {e}")

    # Если контейнер не запустился после трёх попыток, поднимаем исключение
    raise RuntimeError(f"""Контейнер {container_name} не был запущен после 3 попыток.
    Проверьте параметры запуска.
    """)

def get_weather_data(months):
    """
    Получает исторические данные о погоде за указанные месяцы.

    :param months: Количество месяцев, за которые необходимо получить данные о погоде.
    :return: DataFrame с данными о погоде.
    """
    # Поднимаем клиента Open-Meteo API client с кэшированием и перезапуском в случае ощибок
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Определяем переменные
    start_date = (datetime.now() - relativedelta(months=months)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    # Устанавливаем параметры запроса
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 59.9375,
        "longitude": 30.308611,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", \
            "apparent_temperature_max", "apparent_temperature_min", "wind_speed_10m_max"],
        "wind_speed_unit": "ms",
        "timezone": "Europe/Moscow"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Обрабатываем полученные данные
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_apparent_temperature_max = daily.Variables(3).ValuesAsNumpy()
    daily_apparent_temperature_min = daily.Variables(4).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(5).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

    daily_dataframe = pd.DataFrame(data = daily_data)

    return daily_dataframe

def db_connect(db_user, db_password, db_host, db_port, db_name):
    """
    Создает подключение к базе данных PostgreSQL.

    :param db_user: Имя пользователя базы данных.
    :param db_password: Пароль пользователя базы данных.
    :param db_host: Хост базы данных.
    :param db_port: Порт базы данных.
    :param db_name: Название базы данных.
    :return: Объект подключения к базе данных.
    """
    # Создание подключения к базе данных
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    return engine

def create_table(engine, table_name):
    """
    Создает таблицу в базе данных, если она не существует.

    :param engine: Объект подключения к базе данных.
    :param table_name: Название таблицы, которую необходимо создать.

    :raises RuntimeError: Если таблица не была создана.
    """
    # DDL запрос
    ddl_query = text(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE PRIMARY KEY,
            weather_code INTEGER,
            temperature_2m_max FLOAT,
            temperature_2m_min FLOAT,
            apparent_temperature_max FLOAT,
            apparent_temperature_min FLOAT,
            wind_speed_10m_max FLOAT
        );
    ''')

    # Выполнение DDL запроса для создания таблицы
    print("Создание таблицы в базе данных...")
    try:
        with engine.connect() as connection:
            connection.execute(ddl_query)
            connection.commit()
            connection.close()
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")

    # Проверка, что таблица была создана
    with engine.connect() as connection:
        result = connection.execute(text(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
            );
        """))
        connection.commit()
        connection.close()

        # Получение результата проверки
        table_exists = result.scalar()

        if not table_exists:
            raise RuntimeError(f"""Ошибка: Таблица '{table_name}' не была создана.
            Проверьте параметры запроса.
            """)
        else:
            print(f"Таблица '{table_name}' успешно создана.")
            return "Success"

def load_data(daily_dataframe, table_name, engine):
    """
    Загружает данные о погоде в таблицу базы данных.

    :param daily_dataframe: DataFrame с данными о погоде.
    :param table_name: Название таблицы, в которую будут записаны данные.
    :param engine: Объект подключения к базе данных.

    :raises ValueError: Если количество записанных записей не совпадает с ожидаемым.
    """
    print("Запись данных в таблицу daily_weather...")
    daily_dataframe.to_sql(table_name, engine, if_exists='replace', index=False)

    # Проверка, что все данные записались в таблицу
    with engine.connect() as connection:
        # Получение количества строк в таблице после вставки
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
        row_count = result.scalar()  # Получаем одно значение

    # Сравнение количества записей
    if row_count != len(daily_dataframe):
        raise ValueError(f"Ошибка: записано {row_count} записей, ожидается {len(daily_dataframe)}.")

    print(f"Данные успешно записаны в таблицу {table_name}.")
    return "Success"

def ask_user():
    """
    Запрашивает у пользователя, хочет ли он остановить и удалить контейнер.

    Функция циклически запрашивает ввод пользователя, пока не получит допустимый ответ.
    Допустимые ответы: 'y' (да) и 'n' (нет). Если ввод неверный, пользователю
    отображается сообщение об ошибке, и запрос повторяется.

    Returns:
        str: Возвращает 'y', если пользователь хочет остановить и удалить контейнер,
             и 'n', если он предпочитает оставить контейнер запущенным.
    """
    while True:
        answer = input("Остановить и удалить контейнер? (y/n): ").strip().lower()
        if answer in ('y', 'n'):
            return answer
        else:
            print("Ошибка: введите только 'y' или 'n'. Попробуйте снова.")
            continue

def get_dates(daily_dataframe):
    """
    Получает три даты с максимальными значениями температуры и скорости ветра из DataFrame.

    Функция принимает DataFrame с данными о погоде, преобразует столбец дат в формат datetime,
    находит три максимальных значения для температуры и скорости ветра,
    объединяет результаты и возвращает отформатированные даты в виде строки в родительном падеже.

    Параметры:
    ----------
    daily_dataframe : pd.DataFrame
        DataFrame с колонками 'date', 'temperature_2m_max' и 'wind_speed_10m_max',
        где 'date' должен быть в формате строки с часовым поясом.

    Returns:
    -------
    list
        Список строк с отформатированными датами в формате "день месяц год"
        (например, "3 сентября 2024") без скобок и кавычек.
    """
    # Преобразовываем столбец date в datetime
    daily_dataframe['date'] = pd.to_datetime(daily_dataframe['date'])

    # Находим 3 даты с максимальным значением temperature_2m_max
    top_temp_dates = daily_dataframe.nlargest(3, 'temperature_2m_max')[['date', 'temperature_2m_max']]

    # Находим 3 даты с максимальным значением wind_speed_10m_max
    top_wind_dates = daily_dataframe.nlargest(3, 'wind_speed_10m_max')[['date', 'wind_speed_10m_max']]

    # Объединяем результаты
    top_dates = pd.concat([top_temp_dates, top_wind_dates]).drop_duplicates(subset='date').nlargest(3, 'temperature_2m_max')

    # Словарь для замены месяцев
    months = {
        1: "января",
        2: "февраля",
        3: "марта",
        4: "апреля",
        5: "мая",
        6: "июня",
        7: "июля",
        8: "августа",
        9: "сентября",
        10: "октября",
        11: "ноября",
        12: "декабря"
    }

    # Форматируем даты с заменой месяцев
    top_dates['formatted_date'] = top_dates['date'].apply(lambda x: f"{x.day} {months[x.month]} {x.year}")

    # Выводим только отформатированные даты без скобок и кавычек
    formatted_dates = top_dates['formatted_date'].tolist()

    return formatted_dates

if __name__ == "__main__":

    # Определяем переменные
    CONTAINER_NAME = "postgres_weather" # Название Docker контейнера
    DB_NAME = "postgres" # Название базы данных PostgreSQL
    TABLE_NAME = "daily_weather"
    MONTHS = 2 # Количество месяцев для запроса данных о погоде
    DB_HOST = "localhost" # Хост БД
    DB_PORT = "5432" # Порт БД по умоланию
    BD_PORT_A = "5433" # Порт для контейнера, в случае если на компьютере установлен PostgreSQL
    # Установка имени пользователя и пароля пользователем
    DB_USER = "user"
    DB_PASSWORD = "pass"

    # Запрашиваем исторические данные погоды в городе Санкт-Петербург, Россия
    received_daily_dataframe = get_weather_data(MONTHS)
    print("Исторические данные погоды в городе Санкт-Петербург, Россия успешно получены.")
    print(f"Количество месяцев, за которые получены данные - {MONTHS}.")

    # Запускаем контейнер Postgres с помощью Docker
    launch_container(CONTAINER_NAME, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, BD_PORT_A)

    # Подключаемся к базе данных
    db_engine = db_connect(DB_USER, DB_PASSWORD, DB_HOST, BD_PORT_A, DB_NAME)
    print(f"Подключение к базе {DB_NAME} успешно установлено.")

    # Создаём таблицу для записи данных в базе
    create_table(db_engine, TABLE_NAME)

    # Запись данных в таблицу Postgres
    load_data(received_daily_dataframe, TABLE_NAME, db_engine)

    # Считаем количество солнечных дней за период
    count = received_daily_dataframe[received_daily_dataframe['weather_code'].isin([0, 1])].shape[0]
    print(f"Количество солнечных дней (weather_code 0, 1) в запрошенном периоде - {count}")

    # Считаем количество дней, когда температура была выше 20 градусов по Цельсию
    count = received_daily_dataframe[received_daily_dataframe['temperature_2m_max'] >= 20].shape[0]
    print(f"количество дней, когда температура была выше 20 градусов по Цельсию в запрошенном периоде - {count}")

    # Получаем 3 дня, когда была самая высокая температура и самый сильный ветер
    formatted_dates = get_dates(received_daily_dataframe)
    print("3 дня, когда была самая высокая температура и самый сильный ветер - " + ", ".join(formatted_dates))

    # Информируем пользователя о возможности подключиться к базе
    print(f"""
Вы можете подключиться к локальной базе данных PostgreSQL
с помощью любого клиента (например, DBeaver, pgAdmin или командной строки)
с использованием следующих параметров:

    Хост: {DB_HOST}
    Порт: {BD_PORT_A}
    База данных: {DB_NAME}
    Полльзователь: {DB_USER}
    Пароль: {DB_PASSWORD}
    """)

    # Завершение работы. Справщиваем у пользователя нужно ли остановить и удалить контейнер
    user_answer = ask_user()
    if user_answer == 'y':
        print("Остановка и удаление контейнера...")
        subprocess.run(["docker-compose", "down"], check=True)
        print("Контейнер успешно остановлен и удален.")
    else:
        print("Контейнер Postgres оставлен запущенным.")
