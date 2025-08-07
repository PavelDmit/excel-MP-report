import pandas as pd # type: ignore
import logging
from typing import Dict
import aiohttp # type: ignore

from app.api import wb


logger = logging.getLogger(__name__)


async def get_orders_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Асинхронно получает и преобразует данные о заказах Wildberries в DataFrame.

    Параметры:
        get_wb_orders: Асинхронная функция для получения заказов
        params: Словарь параметров (должен содержать 'API_KEY' и 'PA')
        session: Сессия aiohttp для выполнения запросов

    Возвращает:
        pd.DataFrame: DataFrame с заказами или пустой DataFrame при ошибке
    """
    try:
        logger.info(f"Начало загрузки заказов WB для PA {params['PA']}")

        # Получаем сырые данные
        orders_data = await wb.get_wb_orders(
            session=session,
            api_key=params['API_KEY'],
            date_from=params.get('date_from'),
            flag=params.get('flag', 0)
        )

        if not orders_data or not isinstance(orders_data, list):
            logger.warning(f"Получены пустые или некорректные данные для PA {params['PA']}")
            return pd.DataFrame()

        df = pd.DataFrame(orders_data)
        df['PA'] = params['PA']

        # Оптимизация типов данных
        numeric_cols = ['totalPrice', 'discountPercent', 'priceWithDisc']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        date_cols = ['date', 'lastChangeDate']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        logger.info(f"Успешно загружено {len(df)} заказов для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки заказов WB для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()


async def get_stocks_df(
    params: Dict,
    session: aiohttp.ClientSession
) -> pd.DataFrame:
    """
    Асинхронно получает и преобразует данные об остатках Wildberries в DataFrame.

    Параметры:
        get_wb_stocks: Асинхронная функция для получения остатков
        params: Словарь параметров (должен содержать 'API_KEY' и 'PA')
        session: Сессия aiohttp для выполнения запросов

    Возвращает:
        pd.DataFrame: DataFrame с остатками или пустой DataFrame при ошибке
    """
    try:
        logger.info(f"Начало загрузки остатков WB для PA {params['PA']}")

        # Получаем сырые данные
        stocks_data = await wb.get_wb_stocks(
            session=session,
            api_key=params['API_KEY'],
            date_from=params.get('date_from', '2019-06-20')
        )

        if not stocks_data or not isinstance(stocks_data, list):
            logger.warning(f"Получены пустые или некорректные данные для PA {params['PA']}")
            return pd.DataFrame()

        df = pd.DataFrame(stocks_data)
        df['PA'] = params['PA']

        # Оптимизация типов данных
        numeric_cols = ['quantity', 'inWayToClient', 'inWayFromClient']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'lastChangeDate' in df.columns:
            df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'], errors='coerce')

        logger.info(f"Успешно загружено {len(df)} остатков для PA {params['PA']}")
        return df

    except Exception as e:
        logger.error(f"Ошибка обработки остатков WB для PA {params['PA']}: {str(e)}", exc_info=True)
        return pd.DataFrame()
