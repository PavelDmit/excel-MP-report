import aiohttp # type: ignore
from datetime import datetime, timedelta
import logging
from typing import Optional


logger = logging.getLogger(__name__)


async def get_wb_orders(
    session: aiohttp.ClientSession,
    api_key: str,
    date_from: Optional[str] = None,
    flag: int = 0
) -> list:
    """
    Асинхронно получает данные о заказах с Wildberries API.

    Параметры:
        session (aiohttp.ClientSession): Сессия для HTTP-запросов.
        api_key (str): API-ключ Wildberries.
        date_from (str, optional): Дата начала периода в формате RFC3339.
        flag (int): Флаг для фильтрации данных (0 или 1).

    Возвращает:
        list: Список заказов или пустой список при ошибке.
    """
    # Валидация параметров
    if not isinstance(api_key, str):
        logger.error("API-ключ должен быть строкой")
        return []

    if flag not in (0, 1):
        logger.error("Флаг должен быть 0 или 1")
        return []

    # Установка даты по умолчанию (начало прошлой недели)
    if date_from is None:
        now = datetime.now()
        date_from = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%dT00:00:00")

    # Формирование запроса
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": api_key}
    params = {"dateFrom": date_from, "flag": flag}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                logger.warning("Превышен лимит запросов. Попробуйте через 1 минуту")
            else:
                error_data = await response.json()
                logger.error(f"Ошибка {response.status}: {error_data.get('errors', 'Неизвестная ошибка')}")
            return []
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка соединения: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
    return []


async def get_wb_stocks(
    session: aiohttp.ClientSession,
    api_key: str,
    date_from: str = "2019-06-20"
) -> list:
    """
    Асинхронно получает данные об остатках товаров с Wildberries API.

    Параметры:
        session (aiohttp.ClientSession): Сессия для HTTP-запросов.
        api_key (str): API-ключ Wildberries.
        date_from (str): Дата начала периода (по умолчанию "2019-06-20").

    Возвращает:
        list: Список остатков или пустой список при ошибке.
    """
    # Валидация параметров
    if not isinstance(api_key, str):
        logger.error("API-ключ должен быть строкой")
        return []

    # Формирование запроса
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    headers = {"Authorization": api_key}
    params = {"dateFrom": date_from}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                logger.warning("Превышен лимит запросов. Попробуйте через 1 минуту")
            else:
                error_data = await response.json()
                logger.error(f"Ошибка {response.status}: {error_data.get('errors', 'Неизвестная ошибка')}")
            return []
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка соединения: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
    return []
