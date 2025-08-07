# Excel Marketplace Report Generator

Сервис для автоматического сбора данных о заказах и остатках товаров с маркетплейсов (Wildberries, Ozon, Яндекс.Маркет) и генерации Excel-отчета.

## Возможности

- Получение данных о заказах с Wildberries, Ozon и Яндекс.Маркет
- Получение данных об остатках товаров
- Объединение данных в единый Excel-файл
- Автоматическое обновление данных при запросе

## Технологии

- Python 3.12
- FastAPI
- aiohttp
- pandas
- xlsxwriter
- Docker

## Установка и запуск

### Требования

- Docker и Docker Compose
- Python 3.12 (для разработки)

### Запуск в production

1. Создайте файл `.env` в корне проекта с необходимыми переменными окружения (см. пример ниже)
2. Запустите сервис:
   ```bash
   docker-compose -f docker-compose-prod.yml up -d --build
   ```

### Запуск для разработки

1. Создайте файл `.env` в корне проекта
2. Запустите сервис:
   ```bash
   docker-compose -f docker-compose-dev.yml up -d --build
   ```

Сервис будет доступен по адресу: `http://localhost:8000`

## Использование

Для получения отчета сделайте GET-запрос:
```
GET /v1/download/excel-MP-report
```

В ответ вы получите Excel-файл с шестью листами:
- WB_orders - заказы Wildberries
- OZON_orders - заказы Ozon
- YANDEX_orders - заказы Яндекс.Маркет
- WB_stocks - остатки Wildberries
- OZON_stocks - остатки Ozon
- YANDEX_stocks - остатки Яндекс.Маркет

## Переменные окружения (.env)

Пример файла `.env`:
```ini
# Wildberries
WB_API_KEY_OLTEX=your_api_key
WB_API_KEY_WELLINA=your_api_key
WB_API_KEY_MIOTEX=your_api_key
WB_API_KEY_IE_STEGNO=your_api_key

# Ozon
OZON_API_KEY_OLTEX=your_api_key
OZON_API_KEY_WELLINA=your_api_key
OZON_API_KEY_MIOTEX=your_api_key
OZON_API_KEY_WISTROVA=your_api_key
OZON_API_KEY_IE_STEGNO=your_api_key

OZON_CLIENT_ID_OLTEX=your_client_id
OZON_CLIENT_ID_WELLINA=your_client_id
OZON_CLIENT_ID_MIOTEX=your_client_id
OZON_CLIENT_ID_WISTROVA=your_client_id
OZON_CLIENT_ID_IE_STEGNO=your_client_id

# Яндекс.Маркет
YANDEX_API_KEY_OLTEX=your_api_key
YANDEX_API_KEY_WELLINA=your_api_key
YANDEX_API_KEY_MIOTEX=your_api_key
YANDEX_API_KEY_WISTROVA=your_api_key

YANDEX_API_CAMPAIGN_ID_OLTEX=your_campaign_id
YANDEX_API_CAMPAIGN_ID_WELLINA=your_campaign_id
YANDEX_API_CAMPAIGN_ID_MIOTEX=your_campaign_id
YANDEX_API_CAMPAIGN_ID_WISTROVA=your_campaign_id

YANDEX_API_BUSINESS_ID_OLTEX=your_business_id
YANDEX_API_BUSINESS_ID_WELLINA=your_business_id
YANDEX_API_BUSINESS_ID_MIOTEX=your_business_id
YANDEX_API_BUSINESS_ID_WISTROVA=your_business_id
```

## Структура проекта

```
excel-MP-report/
├── docker-compose-prod.yml    # Конфигурация Docker для production
├── docker-compose-dev.yml     # Конфигурация Docker для разработки
├── src/                       # Исходный код
│   ├── app/                   # Основное приложение
│   │   ├── api/               # API клиенты для маркетплейсов
│   │   ├── config.py          # Конфигурация приложения
│   │   ├── data_processor/    # Обработка данных
│   │   └── main.py            # Основной модуль FastAPI
│   ├── .dockerignore
│   ├── .python-version
│   ├── Dockerfile
│   └── pyproject.toml         # Зависимости Python
└── README.md                  # Этот файл
```
