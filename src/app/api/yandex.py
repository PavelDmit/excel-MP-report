import aiohttp # type: ignore
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


async def get_yandex_offers_stocks(
    session: aiohttp.ClientSession,
    campaign_id: str,
    api_key: str,
    page_token: Optional[str] = None
) -> List[Dict]:
    """
    Получает данные об остатках товаров на складах Yandex Market.

    Параметры:
        session: Сессия aiohttp
        campaign_id: ID кампании в Яндекс.Маркете
        api_key: API-ключ Яндекс.Маркета
        page_token: Токен для пагинации (опционально)

    Возвращает:
        Список словарей с данными об остатках
    """
    url = f"https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks"
    if page_token:
        url += f"?page_token={page_token}"

    headers = {"Api-Key": api_key}
    result = []

    try:
        async with session.post(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                result.extend(data.get("result", {}).get("warehouses", []))

                # Обработка пагинации
                next_page_token = data.get("result", {}).get("paging", {}).get("nextPageToken")
                if next_page_token:
                    next_page = await get_yandex_offers_stocks(session, campaign_id, api_key, next_page_token)
                    result.extend(next_page)
            else:
                error = await response.json()
                logger.error(f"Ошибка {response.status}: {error}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка соединения: {str(e)}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")

    return result


async def get_yandex_orders(
    session: aiohttp.ClientSession,
    campaign_id: str,
    api_key: str,
    page_token: Optional[str] = None,
    fromDate: Optional[str] = None,
    toDate: Optional[str] = None
) -> List[Dict]:
    """
    Получает данные о заказах Yandex Market.

    Параметры:
        session: Сессия aiohttp
        campaign_id: ID кампании в Яндекс.Маркете
        api_key: API-ключ Яндекс.Маркета
        page_token: Токен для пагинации (опционально)

    Возвращает:
        Список словарей с данными о заказах
    """
    now = datetime.now()
    if not fromDate:
        fromDate = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d")
    if not toDate:
        toDate = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")

    url = f"https://api.partner.market.yandex.ru/campaigns/{campaign_id}/orders?fromDate={fromDate}&toDate={toDate}"
    if page_token:
        url += f"&page_token={page_token}"

    headers = {"Api-Key": api_key}
    result = []

    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                result.extend(data.get("orders", []))

                # Обработка пагинации
                next_page_token = data.get("paging", {}).get("nextPageToken")
                if next_page_token:
                    next_page = await get_yandex_orders(session, campaign_id, api_key, next_page_token, fromDate, toDate)
                    result.extend(next_page)
            else:
                error = await response.json()
                logger.error(f"Ошибка {response.status}: {error}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка соединения: {str(e)}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")

    return result


async def get_yandex_offer_mappings(
    session: aiohttp.ClientSession,
    business_id: str,
    api_key: str,
    page_token: Optional[str] = None
) -> List[Dict]:
    """
    Получает информацию о соответствии товаров Yandex Market.

    Параметры:
        session: Сессия aiohttp
        business_id: ID бизнеса в Яндекс.Маркете
        api_key: API-ключ Яндекс.Маркета
        page_token: Токен для пагинации (опционально)

    Возвращает:
        Список словарей с информацией о соответствии товаров
    """
    url = f"https://api.partner.market.yandex.ru/businesses/{business_id}/offer-mappings"
    if page_token:
        url += f"?page_token={page_token}"

    headers = {"Api-Key": api_key}
    result = []

    try:
        async with session.post(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                result.extend(data.get("result", {}).get("offerMappings", []))

                # Обработка пагинации
                next_page_token = data.get("result", {}).get("paging", {}).get("nextPageToken")
                if next_page_token:
                    next_page = await get_yandex_offer_mappings(session, business_id, api_key, next_page_token)
                    result.extend(next_page)
            else:
                error = await response.json()
                logger.error(f"Ошибка {response.status}: {error}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка соединения: {str(e)}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")

    return result


async def get_yandex_warehouses(
    session: aiohttp.ClientSession,
    api_key: str
) -> List[Dict]:
    url = "https://api.partner.market.yandex.ru/warehouses"
    headers = {"Api-Key": api_key}

    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return (await response.json()).get("result", {}).get("warehouses", [])
            logger.error(f"Ошибка {response.status}: {await response.text()}")
            return []
    except Exception as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        return []
