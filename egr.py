import requests
import urllib3
import sqlite3
import logging
import time
from urllib3.exceptions import InsecureRequestWarning


# Отключение предупреждений об отсутствии сертификата
urllib3.disable_warnings(InsecureRequestWarning)

# Настройка логгирования
logging.basicConfig(filename='egr_data.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_egr_info(unp):
    """
    Выполняет запрос к API и возвращает данные в формате JSON.

    Args:
        unp (str): Уникальный номер плательщика (УНП).

    Returns:
        dict: Данные в формате JSON или пустой словарь в случае ошибки.
    """
    try:
        response = requests.get(
            f'https://egr.gov.by/api/v2/egr/getAddressByRegNum/{unp}', verify=False)
        if response.status_code == 429:  # Обработка ошибки "слишком много запросов"
            logging.warning(f"""Too many requests for UNP {
                            unp}. Waiting before retrying.""")
            time.sleep(60)  # Подождать одну минуту перед повторным запросом
            return get_egr_info(unp)  # Повторить запрос
        if response.status_code == 204:
            return None
        response.raise_for_status()  # Проверка на успешный запрос
        json_data = response.json()
        return json_data
    except Exception as e:
        logging.error(f"Error getting EGR info for UNP {unp}: {e}")
        return {}


def get_name_company(unp):
    """
    Выполняет запрос к API и возвращает данные в формате JSON.

    Args:
        unp (str): Уникальный номер плательщика (УНП).

    Returns:
        dict: Данные в формате JSON или пустой словарь в случае ошибки.
    """
    try:
        response = requests.get(
            f'https://egr.gov.by/api/v2/egr/getJurNamesByRegNum/{unp}', verify=False)
        if response.status_code == 429:  # Обработка ошибки "слишком много запросов"
            logging.warning(f"""Too many requests for UNP {
                            unp}. Waiting before retrying.""")
            time.sleep(60)  # Подождать одну минуту перед повторным запросом
            return get_name_company(unp)  # Повторить запрос
        if response.status_code == 204:
            return None
        response.raise_for_status()
        json_data = response.json()
        return json_data
    except Exception as e:
        logging.error(f"Error getting company name for UNP {unp}: {e}")
        return {}


def get_activity_company(unp):
    """
    Выполняет запрос к API и возвращает данные в формате JSON.

    Args:
        unp (str): Уникальный номер плательщика (УНП).

    Returns:
        dict: Данные в формате JSON или пустой словарь в случае ошибки.
    """
    try:
        response = requests.get(
            f'https://egr.gov.by/api/v2/egr/getVEDByRegNum/{unp}', verify=False)
        if response.status_code == 429:  # Обработка ошибки "слишком много запросов"
            logging.warning(f"""Too many requests for UNP {
                            unp}. Waiting before retrying.""")
            time.sleep(60)  # Подождать одну минуту перед повторным запросом
            return get_activity_company(unp)  # Повторить запрос
        if response.status_code == 204:
            return None
        response.raise_for_status()
        json_data = response.json()
        return json_data
    except Exception as e:
        logging.error(f"Error getting company activity for UNP {unp}: {e}")
        return {}


def create_table():
    """
    Создает таблицу egr_data в базе данных my_database.db.
    """
    try:
        conn = sqlite3.connect('my_database.db')
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS egr_data (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            Name VARCHAR(255),
                            UNP VARCHAR(255) UNIQUE,
                            Activity VARCHAR(255),
                            PostIndex INT NULL,
                            Address VARCHAR(255),
                            Email VARCHAR(255) NULL,
                            Telefon1 VARCHAR(255) NULL,
                            Telefon2 VARCHAR(255) NULL
                        )''')

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error creating database table: {e}")


def format_data(data: dict):
    try:
        name_info = data['Name'][0].get(
            'vn', data['Name'][0].get('vnaim', 'N/A'))
        unp_info = data['Name'][0].get('ngrn', 'N/A')
        activity_info = data.get('Activity', [{}])[0].get(
            'nsi00114', {}).get('vnvdnp', 'N/A')

        email = data['Info'][0].get('vemail', 'N/A')

        phone_string = data['Info'][0].get('vtels', '')
        phone_numbers = [number.strip() for number in phone_string.split(
            ',')] if phone_string else ['N/A', 'N/A']
        telefon1 = phone_numbers[0] if phone_numbers else 'N/A'
        telefon2 = phone_numbers[1] if len(phone_numbers) > 1 else 'N/A'

        post_index = data['Info'][0].get('nindex', 'N/A')
        city = data['Info'][0].get('vnp', 'N/A')
        vulitsa = data['Info'][0].get('vulitsa', 'N/A')
        vdom = data['Info'][0].get('vdom', data['Info'][0].get('vkorp', 'N/A'))
        vpom = data['Info'][0].get('vpom', 'N/A')
        full_address = f'''г.{city}, ул. {vulitsa} {vdom} {
            vpom}''' if city and vulitsa and vdom and vpom else 'N/A'

        return {
            'name': name_info,
            'unp': unp_info,
            'activity': activity_info,
            'email': email,
            'telefon1': telefon1,
            'telefon2': telefon2,
            'post_index': post_index,
            'full_address': full_address
        }
    except Exception as e:
        logging.error(f"Error formatting data: {e}")
        return {}


def insert_data(data: dict):
    """
    Вставляет данные в таблицу egr_data базы данных my_database.db.

    Args:
        data (dict): Словарь с данными для вставки в таблицу.
    """
    try:
        formatted_data = format_data(data)
        conn = sqlite3.connect('my_database.db')
        cursor = conn.cursor()

        cursor.execute('''INSERT INTO egr_data (Name, UNP, Activity, Email, Telefon1, Telefon2, PostIndex, Address)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (formatted_data['name'], formatted_data['unp'], formatted_data['activity'],
                        formatted_data['email'], formatted_data['telefon1'], formatted_data['telefon2'],
                        formatted_data['post_index'], formatted_data['full_address']))

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")


def generate_numbers(start, end):
    """
    Генерирует номера для запросов к API в указанном диапазоне.

    Args:
        start (int): Начальное значение диапазона номеров.
        end (int): Конечное значение диапазона номеров.

    Yields:
        str: Номер для запроса к API в формате строки с длиной 9 цифр.
    """
    for number in range(start, end + 1):
        yield str(number).zfill(9)


def main():
    try:
        create_table()
        for number in generate_numbers(100000313, 999999999):
            combined_data = {}
            name = get_name_company(number)
            if name is None:
                logging.info(
                    f'Failed to retrieve necessary data for UNP: {number}')
                continue
            activity = get_activity_company(number)
            egr_info = get_egr_info(number)

            combined_data['Name'] = name
            combined_data['Activity'] = activity
            combined_data['Info'] = egr_info

            insert_data(combined_data)

    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')


if __name__ == '__main__':
    main()
else:
    print('MAGICHAL SCUF')
