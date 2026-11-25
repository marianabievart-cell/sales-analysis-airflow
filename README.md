# Система анализа и визуализации продаж

Интернет-магазин: какие товары популярны и какие действия приводят к покупке. Данные — 10 000 действий 50 пользователей над 100 товарами
(`view`, `add_to_cart`, `purchase`), период 20.08.2024 – 15.02.2025. Весь процесс автоматизирован DAG в Airflow.

## Этапы и где они в коде

| Этап | Что делается | Где |
|---|---|---|
| 1 | проверка столбцов и типов, загрузка CSV в SQLite (`output/sales.db`, таблица `user_actions`) | `load_to_sqlite` в `dags/sales_pipeline.py` |
| 2 | SQL: топ-10 товаров по просмотрам, корзине и покупкам | `sql/metrics.sql`, `sql_top_products` |
| 3 | выручка по категориям, конверсия, графики Plotly (HTML), все метрики в один Excel | `analyze_and_visualize` |
| 4 | DAG `sales_analysis_dag`: `load_to_sqlite → sql_top_products → analyze_and_visualize → log_success`, запуск `@daily` | `dags/sales_analysis_dag.py` |

## Результаты (папка `output/`)
- `sales_metrics.xlsx` — 10 листов: сводка, выручка по категориям, популярность, воронка, конверсия по категориям, выручка по месяцам, топы;
- `dashboard.html` — все графики на одной странице (открывается без интернета), плюс отдельные файлы в `output/html/`.

## Как считались метрики
- **Выручка категории** = сумма `price` по строкам `purchase` этой категории. Общая выручка 1 680 380,11; лидер Home (22,0 %).
- **Конверсия** считается по парам «пользователь + товар + категория»: пара «дошла» до шага, если у неё есть это действие.
  Просмотр 3 106 пар → корзина 385 (12,4 %) → покупка 63 (16,4 % от корзины, 2,0 % от просмотров).
  Долю «покупок / просмотров» по числу событий считать нельзя: покупок (3 360) больше, чем просмотров (3 333), то есть действия в данных не идут цепочкой.
- В данных один `product_id` встречается во всех пяти категориях и с разными ценами, поэтому категория и цена берутся из самого события.

## Запуск без Docker
```bash
pip install pandas plotly openpyxl
python dags/sales_pipeline.py
```

## Запуск в Airflow (Docker Desktop)
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
mkdir -p ./logs ./plugins ./config ./output
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
```
Откройте http://localhost:8080, включите `sales_analysis_dag` и нажмите Trigger. Результаты появятся в папке `output/`.
