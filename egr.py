import aiohttp
import asyncio
import aiosqlite
import sqlite3
import logging
from tqdm import tqdm


# Установите желаемое количество одновременных запросов
MAX_CONCURRENT_REQUESTS = 1000
# Через сколько запросов делать паузу
REQUESTS_PAUSE_INTERVAL = 1000
# Длительность паузы в секундах
PAUSE_DURATION = 0

# Настройка логгирования
logging.basicConfig(filename='egr_data.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s %(message)s %(lineno)d')


async def async_get_data(session: aiohttp.ClientSession, url_template: str, unp):
    MAX_RETRIES = 3  # Максимальное количество попыток
    RETRY_DELAY = 5  # Задержка между попытками в секундах

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url_template.format(unp), verify_ssl=False) as response:
                if response.status == 429:
                    logging.warning(
                        f"Too many requests for UNP {unp}. Waiting before retrying.")
                    await asyncio.sleep(5)
                    continue
                if response.status == 204:
                    return None
                response.raise_for_status()
                json_data = await response.json()
                return json_data
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError) as e:
            logging.error(f"Error getting data for UNP {unp}: {e}")

            if isinstance(e, (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError)):
                logging.warning("Connection error. Reconnecting...")
                await asyncio.sleep(RETRY_DELAY)
                continue

        logging.warning(
            f"Retrying ({attempt + 1}/{MAX_RETRIES}) for UNP {unp}...")
        await asyncio.sleep(RETRY_DELAY)

    logging.error(
        f"Failed to get data for UNP {unp} after {MAX_RETRIES} attempts.")
    return {}


async def async_get_name_company(session: aiohttp.ClientSession, unp: str):
    url_template = 'https://egr.gov.by/api/v2/egr/getJurNamesByRegNum/{}'
    return await async_get_data(session, url_template, unp)


async def async_get_activity_company(session: aiohttp.ClientSession, unp: str):
    url_template = 'https://egr.gov.by/api/v2/egr/getVEDByRegNum/{}'
    return await async_get_data(session, url_template, unp)


async def async_get_egr_info(session: aiohttp.ClientSession, unp: str):
    url_template = 'https://egr.gov.by/api/v2/egr/getAddressByRegNum/{}'
    return await async_get_data(session, url_template, unp)


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
                            Telefon VARCHAR(255) NULL
                        )''')

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error creating database table: {e}")


def format_data(data: dict):
    try:
        name_info = data['Name'][0].get(
            'vn', data['Name'][0].get('vnaim', ''))
        unp_info = data['Name'][0].get('ngrn', '')
        activity_info = data.get('Activity', [{}])[0].get(
            'nsi00114', {}).get('vnvdnp', '')

        email = data['Info'][0].get('vemail', '')

        telefon = data['Info'][0].get('vtels', '')

        post_index = data['Info'][0].get('nindex', '')
        city = data['Info'][0].get('vnp', '')
        vulitsa = data['Info'][0].get('vulitsa', '')
        vdom = data['Info'][0].get('vdom', data['Info'][0].get('vkorp', ''))
        vpom = data['Info'][0].get('vpom', '')
        full_address = f'''г.{city}, ул. {vulitsa} {vdom} {
            vpom}'''

        return {
            'name': name_info,
            'unp': unp_info,
            'activity': activity_info,
            'email': email,
            'telefon': telefon,
            'post_index': post_index,
            'full_address': full_address
        }
    except Exception as e:
        logging.error(f"Error formatting data: {e}")
        return {}


async def async_insert_data(data: dict):
    try:
        formatted_data = format_data(data)

        # Используем контекстный менеджер для подключения к базе данных с aiosqlite
        async with aiosqlite.connect('my_database.db') as conn:
            cursor = await conn.cursor()

            await cursor.execute('''INSERT INTO egr_data (Name, UNP, Activity, Email, Telefon, PostIndex, Address)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                 (formatted_data['name'], formatted_data['unp'], formatted_data['activity'],
                                  formatted_data['email'], formatted_data['telefon'],
                                  formatted_data['post_index'], formatted_data['full_address']))

            await conn.commit()
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


async def async_get_combined_data(session: aiohttp.ClientSession, sem: asyncio.Semaphore, unp):
    async with sem:
        combined_data = {}
        name = await async_get_name_company(session, unp)
        if name is None or name == {}:
            logging.info(f'Failed to retrieve necessary data for UNP: {unp}')
            return None
        activity = await async_get_activity_company(session, unp)
        egr_info = await async_get_egr_info(session, unp)

        combined_data['Name'] = name
        combined_data['Activity'] = activity
        combined_data['Info'] = egr_info

        return combined_data


async def main_async():
    create_table()

    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        total_numbers = 999999999 - 100000127 + 1  # Общее количество чисел
        progress_bar = tqdm(total=total_numbers,
                            desc="Processing Numbers", unit="number")

        for index, number in enumerate(generate_numbers(100000127, 999999999), start=1):
            tasks.append(async_get_combined_data(session, sem, number))
            if index % REQUESTS_PAUSE_INTERVAL == 0:
                results = await asyncio.gather(*tasks)
                tasks = []
                progress_bar.update(index - progress_bar.n)
                logging.info(f'Pause for {PAUSE_DURATION} seconds...')
                await asyncio.sleep(PAUSE_DURATION)

                # Вставка данных в базу данных после каждой паузы
                if results and any(result for result in results):
                    for result in results:
                        if result:
                            await async_insert_data(result)

        if tasks:
            results = await asyncio.gather(*tasks)

            # Вставка данных в базу данных после последней паузы
            if results and any(result for result in results):
                for result in results:
                    if result:
                        await async_insert_data(result)

        progress_bar.close()

if __name__ == '__main__':
    asyncio.run(main_async())
else:
    print('MAGICHAL SCUF')
