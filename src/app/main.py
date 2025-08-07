import pandas as pd # type: ignore
import asyncio
import aiohttp # type: ignore
from io import BytesIO
from fastapi import FastAPI, Response # type: ignore
import logging

from app.config import WB_API_CONF_LIST_MAP, OZON_API_CONF_LIST_MAP, YANDEX_API_CONF_LIST_MAP
from app.data_processor import wb_get_data_tasks, ozon_get_data_tasks, yandex_get_data_tasks


app = FastAPI()

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s: %(message)s"
)
logger = logging.getLogger(__name__)


async def get_additional_data(session):
    """Обновляет дополнительные данные (атрибуты товаров и FBY списк складов) из Ozon и Yandex."""
    # Ozon
    ozon_get_info_tasks = [
        ozon_get_data_tasks.get_info_df(params, session)
        for params in OZON_API_CONF_LIST_MAP
    ]
    ozon_info_results = await asyncio.gather(*ozon_get_info_tasks)
    ozon_attributes_info_df = pd.concat(ozon_info_results).drop_duplicates().reset_index(drop=True)

    # Yandex атрибуты товаров
    yandex_get_info_tasks = [
        yandex_get_data_tasks.get_info_df(params, session)
        for params in YANDEX_API_CONF_LIST_MAP
    ]
    yandex_info_results = await asyncio.gather(*yandex_get_info_tasks)
    yandex_attributes_info_df = pd.concat(yandex_info_results).drop_duplicates().reset_index(drop=True)

    # Yandex списк складов FBY
    yandex_get_warehouses_tasks = [
        yandex_get_data_tasks.get_warehouses_df(params, session)
        for params in YANDEX_API_CONF_LIST_MAP
    ]
    yandex_warehouses_results = await asyncio.gather(*yandex_get_warehouses_tasks)
    yandex_warehouses_df = pd.concat(yandex_warehouses_results).drop_duplicates().reset_index(drop=True)

    return {
        'ozon_attributes_info': ozon_attributes_info_df[['barcode', 'sku']],
        'yandex_attributes_info': yandex_attributes_info_df[['offerId', 'barcode']],
        'yandex_warehouses': yandex_warehouses_df
    }


async def get_orders(session, additional_data):
    """Обновляет данные о заказах из Wildberries, Ozon и Yandex."""
    # Wildberries
    wb_get_tasks = [
        wb_get_data_tasks.get_orders_df(params, session)
        for params in WB_API_CONF_LIST_MAP
    ]
    wb_orders_results = await asyncio.gather(*wb_get_tasks)
    wb_orders_result_df = pd.concat(wb_orders_results)

    # Ozon
    ozon_get_tasks = [
        ozon_get_data_tasks.get_orders_df(params, session)
        for params in OZON_API_CONF_LIST_MAP
    ]
    ozon_orders_results = await asyncio.gather(*ozon_get_tasks)
    ozon_orders_df = pd.concat(ozon_orders_results)
    ozon_orders_result_df = ozon_orders_df.merge(
        additional_data['ozon_attributes_info'], on='sku', how='left'
    )

    # Yandex
    yandex_get_tasks = [
        yandex_get_data_tasks.get_orders_df(params, session)
        for params in YANDEX_API_CONF_LIST_MAP
    ]
    yandex_orders_results = await asyncio.gather(*yandex_get_tasks)
    yandex_orders_df = pd.concat(yandex_orders_results)
    yandex_orders_result_df = yandex_orders_df.merge(
        additional_data['yandex_attributes_info'], on='offerId', how='left'
    )

    return {
        'wb_orders': wb_orders_result_df,
        'ozon_orders': ozon_orders_result_df,
        'yandex_orders': yandex_orders_result_df
    }


async def get_stocks(session, additional_data):
    """Обновляет данные об остатках товаров из Wildberries, Ozon и Yandex."""
    # Wildberries
    wb_get_tasks = [
        wb_get_data_tasks.get_stocks_df(params, session)
        for params in WB_API_CONF_LIST_MAP
    ]
    wb_stocks_results = await asyncio.gather(*wb_get_tasks)
    wb_stocks_result_df = pd.concat(wb_stocks_results)

    # Ozon
    ozon_skus = additional_data['ozon_attributes_info']['sku'].astype('str').tolist()
    ozon_get_tasks = [
        ozon_get_data_tasks.get_stocks_df(params, ozon_skus, session)
        for params in OZON_API_CONF_LIST_MAP
    ]
    ozon_stocks_results = await asyncio.gather(*ozon_get_tasks)
    ozon_stocks_df = pd.concat(ozon_stocks_results)
    ozon_stocks_result_df = ozon_stocks_df.merge(
        additional_data['ozon_attributes_info'], on='sku', how='left'
    )

    # Yandex
    yandex_get_tasks = [
        yandex_get_data_tasks.get_stocks_df(params, session)
        for params in YANDEX_API_CONF_LIST_MAP
    ]
    yandex_stocks_results = await asyncio.gather(*yandex_get_tasks)
    yandex_stocks_init_df = pd.concat(yandex_stocks_results)
    yandex_stocks_df = yandex_stocks_init_df.merge(
        additional_data['yandex_attributes_info'], on='offerId', how='left'
    )
    yandex_stocks_result_df = yandex_stocks_df[yandex_stocks_df['warehouseId'].isin(
        additional_data['yandex_warehouses']['warehouseId'].unique()
    )]

    return {
        'wb_stocks': wb_stocks_result_df,
        'ozon_stocks': ozon_stocks_result_df,
        'yandex_stocks': yandex_stocks_result_df
    }


async def generate_excel_file():
    """Генерирует Excel-файл в памяти."""
    async with aiohttp.ClientSession() as session:
        additional_data = await get_additional_data(session)
        orders_data = await get_orders(session, additional_data)
        stocks_data = await get_stocks(session, additional_data)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            orders_data['wb_orders'].to_excel(writer, sheet_name='WB_orders', index=False)
            orders_data['ozon_orders'].to_excel(writer, sheet_name='OZON_orders', index=False)
            orders_data['yandex_orders'].to_excel(writer, sheet_name='YANDEX_orders', index=False)
            stocks_data['wb_stocks'].to_excel(writer, sheet_name='WB_stocks', index=False)
            stocks_data['ozon_stocks'].to_excel(writer, sheet_name='OZON_stocks', index=False)
            stocks_data['yandex_stocks'].to_excel(writer, sheet_name='YANDEX_stocks', index=False)
        output.seek(0)

        return output


@app.get("/v1/download/excel-MP-report")
async def download_report():
    """Эндпоинт для скачивания Excel-файла."""
    try:
        excel_file = await generate_excel_file()
        headers = {
            "Content-Disposition": "attachment; filename=orders_and_stocks.xlsx",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        return Response(content=excel_file.getvalue(), headers=headers)

    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return 'ОШИБКА. Что-то пошло не так. Повторите позже'


if __name__ == "__main__":
    import uvicorn # type: ignore
    uvicorn.run(app, host="0.0.0.0", port=8000)
