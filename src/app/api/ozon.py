import aiohttp # type: ignore
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional


logger = logging.getLogger(__name__)


async def get_ozon_product_info_attributes(
    session: aiohttp.ClientSession,
    client_id: str,
    api_key: str,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[str]] = None,
    sku: Optional[List[str]] = None
) -> List[Dict]:
    """Получает атрибуты товаров Ozon."""
    url = "https://api-seller.ozon.ru/v4/product/info/attributes"
    headers = {"Client-Id": client_id, "Api-Key": api_key}
    payload = {
        "filter": {
            "offer_id": offer_id or [],
            "product_id": product_id or [],
            "sku": sku or []
        },
        "limit": 1000
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                return (await response.json()).get("result", [])
            logger.error(f"Ошибка {response.status}: {await response.text()}")
            return []
    except Exception as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        return []

async def get_ozon_analytics_stocks(
    session: aiohttp.ClientSession,
    client_id: str,
    api_key: str,
    skus: List[str]
) -> List[Dict]:
    """Получает данные об остатках товаров Ozon."""
    if not skus:
        return []

    # Разбиваем SKU на чанки по 100 элементов
    chunk_size = 100
    results = []
    for i in range(0, len(skus), chunk_size):
        chunk = skus[i:i + chunk_size]
        url = "https://api-seller.ozon.ru/v1/analytics/stocks"
        headers = {"Client-Id": client_id, "Api-Key": api_key}
        payload = {"skus": chunk}

        try:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    results.extend((await response.json()).get("items", []))
                else:
                    logger.error(f"Ошибка {response.status} для чанка {i}:{i+chunk_size}")
        except Exception as e:
            logger.error(f"Ошибка запроса для чанка {i}:{i+chunk_size}: {str(e)}")

    return results


async def get_ozon_posting_fbo_list(
    session: aiohttp.ClientSession,
    client_id: str,
    api_key: str,
    since: Optional[str] = None,
    to: Optional[str] = None
) -> List[Dict]:
    """Получает список FBO-заказов Ozon."""
    # Установка временного диапазона (прошлая неделя)
    now = datetime.now()
    if not since:
        since = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%dT00:00:00.000Z")
    if not to:
        to = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%dT00:00:00.000Z")

    # Разбивка на суточные интервалы
    results = []
    current_since = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_date = datetime.strptime(to, "%Y-%m-%dT%H:%M:%S.%fZ")

    while current_since < end_date:
        next_day = current_since + timedelta(days=1)
        chunk_to = min(next_day, end_date)

        url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
        headers = {"Client-Id": client_id, "Api-Key": api_key}
        payload = {
            "filter": {
                "since": current_since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "to": chunk_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            },
            "limit": 1000
        }

        try:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    results.extend((await response.json()).get("result", []))
                else:
                    logger.error(f"Ошибка {response.status} для периода {current_since}-{chunk_to}")
        except Exception as e:
            logger.error(f"Ошибка запроса для периода {current_since}-{chunk_to}: {str(e)}")

        current_since = next_day

    return results


async def get_ozon_posting_fbs_list(
    session: aiohttp.ClientSession,
    client_id: str,
    api_key: str,
    since: Optional[str] = None,
    to: Optional[str] = None,
    offset: int = 0
) -> List[Dict]:
    """Получает список FBS-заказов Ozon."""
    # Установка временного диапазона (прошлая неделя)
    now = datetime.now()
    if not since:
        since = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%dT04:00:00.000Z")
    if not to:
        to = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%dT04:00:00.000Z")

    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {"Client-Id": client_id, "Api-Key": api_key}
    payload = {
        "filter": {"since": since, "to": to},
        "limit": 1000,
        "offset": offset
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                return (await response.json()).get("result", {}).get("postings", [])
            logger.error(f"Ошибка {response.status}: {await response.text()}")
            return []
    except Exception as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        return []
