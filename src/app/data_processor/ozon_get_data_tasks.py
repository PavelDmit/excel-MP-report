import pandas as pd # type: ignore
import logging
from typing import Dict, List
import aiohttp # type: ignore
import asyncio

from app.api import ozon


logger = logging.getLogger(__name__)


async def get_info_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Получает и преобразует информацию о товарах Ozon в DataFrame.

    Параметры:
        get_ozon_product_info_attributes: Асинхронная функция для получения атрибутов
        params: Параметры запроса (должен содержать CLIENT_ID, API_KEY, PA)
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: С данными товаров или пустой DataFrame при ошибке
    """
    try:
        logger.info(f"Загрузка информации о товарах Ozon для PA {params['PA']}")

        # Получаем данные
        attributes = await ozon.get_ozon_product_info_attributes(
            session=session,
            client_id=params['CLIENT_ID'],
            api_key=params['API_KEY']
        )

        if not attributes:
            logger.warning(f"Нет данных о товарах для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for product in attributes:
            data.append({
                'barcode': product.get('barcode', ''),
                'offer_id': product.get('offer_id', ''),
                'sku': product.get('sku', None),
                'product_id': product.get('id', None)
            })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']
        # df['sku'] = pd.to_numeric(df['sku'], errors='coerce')
        # df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')

        logger.info(f"Загружено {len(df)} товаров для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки информации о товарах Ozon для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()


async def get_orders_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Получает и объединяет данные о FBO и FBS заказах Ozon.

    Параметры:
        get_ozon_posting_fbo_list: Функция для получения FBO заказов
        get_ozon_posting_fbs_list: Функция для получения FBS заказов
        params: Параметры запроса (CLIENT_ID, API_KEY, PA)
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: Объединенные данные заказов или пустой DataFrame
    """
    async def process_postings(get_func, posting_type: str) -> pd.DataFrame:
        """Обрабатывает заказы определенного типа (FBO/FBS)"""
        try:
            postings = await get_func(
                session=session,
                client_id=params['CLIENT_ID'],
                api_key=params['API_KEY']
            )

            if not postings:
                logger.warning(f"Нет {posting_type} заказов для PA {params['PA']}")
                return pd.DataFrame()

            data = []
            for posting in postings:
                for product in posting.get('products', []):
                    data.append({
                        'created_at': posting.get('created_at' if posting_type == 'FBO' else 'in_process_at', ''),
                        'order_id': posting.get('order_id', None),
                        'order_number': posting.get('order_number', ''),
                        'posting_number': posting.get('posting_number', ''),
                        'name': product.get('name', ''),
                        'offer_id': product.get('offer_id', ''),
                        'sku': product.get('sku', None),
                        'price': product.get('price', ''),
                        'quantity': product.get('quantity', None)
                    })

            df = pd.DataFrame(data)
            df['PA'] = params['PA']
            return df

        except Exception as e:
            logger.error(f"Ошибка обработки {posting_type} заказов для PA {params['PA']}: {str(e)}")
            return pd.DataFrame()

    # Параллельная загрузка FBO и FBS заказов
    fbo_df, fbs_df = await asyncio.gather(
        process_postings(ozon.get_ozon_posting_fbo_list, "FBO"),
        process_postings(ozon.get_ozon_posting_fbs_list, "FBS")
    )

    return pd.concat([fbo_df, fbs_df], ignore_index=True)


async def get_stocks_df(
    params: Dict,
    skus: List[str],
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Получает данные об остатках товаров Ozon.

    Параметры:
        get_ozon_analytics_stocks: Функция для получения остатков
        params: Параметры запроса (CLIENT_ID, API_KEY, PA)
        skus: Список SKU для запроса
        session: Сессия aiohttp

    Возвращает:
        pd.DataFrame: Данные об остатках или пустой DataFrame
    """
    try:
        logger.info(f"Загрузка остатков Ozon для {len(skus)} SKU (PA {params['PA']})")

        stocks = await ozon.get_ozon_analytics_stocks(
            session=session,
            client_id=params['CLIENT_ID'],
            api_key=params['API_KEY'],
            skus=skus
        )

        if not stocks:
            logger.warning(f"Нет данных об остатках для PA {params['PA']}")
            return pd.DataFrame()

        # Преобразуем в DataFrame
        data = []
        for item in stocks:
            data.append({
                'name': item.get('name', ''),
                'offer_id': item.get('offer_id', ''),
                'sku': item.get('sku', None),
                'available_stock_count': item.get('available_stock_count', None),
                'warehouse_id': item.get('warehouse_id', None),
                'warehouse_name': item.get('warehouse_name', None),
                'cluster_id': item.get('cluster_id', None)
            })

        df = pd.DataFrame(data)
        df['PA'] = params['PA']

        logger.info(f"Загружено {len(df)} остатков для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки остатков Ozon для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()
