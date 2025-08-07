import pandas as pd # type: ignore
import logging
from typing import Dict
import aiohttp # type: ignore

from app.api import yandex


logger = logging.getLogger(__name__)


async def get_orders_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Асинхронно получает и преобразует данные о заказах Яндекс.Маркета в DataFrame.

    Параметры:
        get_yandex_orders: Асинхронная функция для получения заказов
        params: Словарь параметров (должен содержать CAMPAIGN_ID, API_KEY, PA)
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: DataFrame с заказами или пустой DataFrame при ошибке
    """
    try:
        logger.info(f"Начало загрузки заказов Yandex для PA {params['PA']}")

        # Получаем данные
        orders = await yandex.get_yandex_orders(
            session=session,
            campaign_id=params['CAMPAIGN_ID'],
            api_key=params['API_KEY']
        )

        if not orders:
            logger.warning(f"Нет данных о заказах для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for order in orders:
            for item in order.get('items', []):
                data.append({
                    'creationDate': order.get('creationDate', ''),
                    'order_id': order.get('id', None),
                    'item_id': item.get('id', None),
                    'offerId': item.get('offerId', ''),
                    'offerName': item.get('offerName', ''),
                    'price': item.get('price', None),
                    'buyerPrice': item.get('buyerPrice', None),
                    'buyerPriceBeforeDiscount': item.get('buyerPriceBeforeDiscount', None),
                    'vat': item.get('vat', ''),
                    'count': item.get('count', None),
                })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']

        logger.info(f"Успешно загружено {len(df)} заказов для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки заказов Yandex для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()


async def get_info_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Получает информацию о соответствии товаров Яндекс.Маркета.

    Параметры:
        get_yandex_offer_mappings: Функция для получения соответствий
        params: Параметры запроса (BUSINESS_ID, API_KEY, PA)
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: Данные о соответствии товаров или пустой DataFrame
    """
    try:
        logger.info(f"Загрузка информации о товарах Yandex для PA {params['PA']}")

        mappings = await yandex.get_yandex_offer_mappings(
            session=session,
            business_id=params['BUSINESS_ID'],
            api_key=params['API_KEY']
        )

        if not mappings:
            logger.warning(f"Нет данных о соответствии товаров для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for mapping in mappings:
            offer = mapping.get('offer', {})
            data.append({
                'offerId': offer.get('offerId', ''),
                'barcode': offer.get('barcodes', [])[0],
                'name': offer.get('name', ''),
                'vendor': offer.get('vendor', ''),
                'vendorCode': offer.get('vendorCode', '')
            })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']

        logger.info(f"Загружено {len(df)} соответствий товаров для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки информации о товарах Yandex для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()


async def get_stocks_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Получает данные об остатках товаров Яндекс.Маркета.

    Параметры:
        get_yandex_offers_stocks: Функция для получения остатков
        params: Параметры запроса (CAMPAIGN_ID, API_KEY, PA)
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: Данные об остатках или пустой DataFrame
    """
    try:
        logger.info(f"Загрузка остатков Yandex для PA {params['PA']}")

        stocks = await yandex.get_yandex_offers_stocks(
            session=session,
            campaign_id=params['CAMPAIGN_ID'],
            api_key=params['API_KEY']
        )

        if not stocks:
            logger.warning(f"Нет данных об остатках для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for warehouse in stocks:
            for offer in warehouse.get('offers', []):
                for stock in offer.get('stocks', []):
                    data.append({
                        'warehouseId': warehouse.get('warehouseId', ''),
                        'offerId': offer.get('offerId', ''),
                        'updatedAt': offer.get('updatedAt', ''),
                        'type': stock.get('type', ''),
                        'count': stock.get('count', None)
                    })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']

        logger.info(f"Загружено {len(df)} остатков для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки остатков Yandex для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()


async def get_warehouses_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    try:
        logger.info(f"Загрузка списке складов Yandex для PA {params['PA']}")

        warehouses = await yandex.get_yandex_warehouses(
            session=session,
            api_key=params['API_KEY']
        )

        if not warehouses:
            logger.warning(f"Нет данных о списке складов для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for warehouse in warehouses:
            data.append({
                'warehouseId': warehouse.get('id', None),
                'name': warehouse.get('name', '')
            })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']

        logger.info(f"Загружено {len(df)} складов для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки списка складов Yandex для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()
