"""Функции этапов проекта «Система анализа и визуализации продаж».
Каждая функция - один шаг DAG; их можно запускать и без Airflow (см. блок __main__ внизу).
"""
import logging
import os
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

BASE = Path(os.environ.get("SALES_BASE", Path(__file__).resolve().parent.parent))
CSV_PATH = Path(os.environ.get("SALES_CSV", BASE / "data" / "user_actions.csv"))
OUT = Path(os.environ.get("SALES_OUT", BASE / "output"))
DB_PATH = OUT / "sales.db"
EXCEL_PATH = OUT / "sales_metrics.xlsx"
HTML_DIR = OUT / "html"
TOP_N = 10

REQUIRED = {"user_id": "int64", "product_id": "int64", "action": "object", "category": "object", "price": "float64", "timestamp": "object"}
ACTIONS = ["view", "add_to_cart", "purchase"]
STEP_RU = {"view_to_cart_pct": "Просмотр → корзина", "cart_to_purchase_pct": "Корзина → покупка", "view_to_purchase_pct": "Просмотр → покупка"}
ACTION_RU = {"view": "Просмотр", "add_to_cart": "Добавление в корзину", "purchase": "Покупка"}


def _conn():
    OUT.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def load_to_sqlite(**_):
    """Этап 1. Проверяет структуру CSV и загружает его в таблицу user_actions (SQLite)."""
    import pandas as pd
    df = pd.read_csv(CSV_PATH)
    missing = set(REQUIRED) - set(df.columns)
    if missing:
        raise ValueError(f"В файле нет столбцов: {sorted(missing)}")
    for col, dtype in REQUIRED.items():
        if dtype == "object":
            continue
        df[col] = pd.to_numeric(df[col], errors="raise").astype(dtype)   # упадёт, если в столбце есть нечисловые значения
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    bad = set(df["action"].unique()) - set(ACTIONS)
    if bad:
        raise ValueError(f"Неизвестные действия: {bad}")
    if df.isna().any().any():
        raise ValueError("В данных есть пропуски:\n" + str(df.isna().sum()))
    if (df["price"] <= 0).any():
        raise ValueError("Есть неположительные цены")
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # в SQLite даты хранятся текстом ISO
    with _conn() as c:
        df.to_sql("user_actions", c, if_exists="replace", index=False)
        c.execute("CREATE INDEX IF NOT EXISTS ix_action ON user_actions(action, product_id)")
    log.info("Загружено %s строк в %s, пользователей %s, товаров %s, период %s - %s",
             len(df), DB_PATH.name, df.user_id.nunique(), df.product_id.nunique(), df.timestamp.min()[:10], df.timestamp.max()[:10])
    return len(df)


def sql_top_products(**_):
    """Этап 2. SQL: топ товаров по просмотрам, добавлениям в корзину и покупкам."""
    import pandas as pd
    queries = [q.strip() for q in (BASE / "sql" / "metrics.sql").read_text(encoding="utf-8").split(";") if "SELECT" in q]
    names = ["top_views", "top_cart", "top_purchases"]
    with _conn() as c:
        for name, q in zip(names, queries):
            pd.read_sql(q, c, params={"lim": TOP_N}).to_sql(name, c, if_exists="replace", index=False)
        top = pd.read_sql("SELECT * FROM top_purchases", c)
    log.info("Топ-%s товаров по покупкам: %s", TOP_N, dict(zip(top.product_id, top.cnt)))
    return names


def analyze_and_visualize(**_):
    """Этап 3. Выручка по категориям, конверсия, графики Plotly (HTML), метрики в один Excel."""
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    with _conn() as c:
        df = pd.read_sql("SELECT * FROM user_actions", c)
        tops = {n: pd.read_sql(f"SELECT * FROM {n}", c) for n in ("top_views", "top_cart", "top_purchases")}

    # --- выручка по категориям: сумма цен покупок ---
    buy = df[df.action == "purchase"]
    revenue = (buy.groupby("category").agg(purchases=("price", "size"), revenue=("price", "sum"), avg_check=("price", "mean"))
               .round(2).sort_values("revenue", ascending=False).reset_index())
    revenue["revenue_share_pct"] = (100 * revenue.revenue / revenue.revenue.sum()).round(1)

    # --- популярность по категориям: число действий каждого типа ---
    pop = df.pivot_table(index="category", columns="action", values="price", aggfunc="size")[ACTIONS].reset_index()
    pop.columns.name = None

    # --- конверсия: воронка по парам «пользователь - товар» ---
    # Считать долю как «покупок / просмотров» по числу событий нельзя: события независимы, и в данных покупок больше, чем просмотров.
    # Поэтому смотрим, дошла ли конкретная пара (пользователь, товар, категория) до следующего шага. В данных один product_id встречается
    # в разных категориях, поэтому категория входит в ключ пары - так общая воронка равна сумме воронок по категориям.
    n = df.action.value_counts()
    def funnel_of(g):
        acts = g.groupby(["user_id", "product_id", "category"]).action.agg(set)
        v = int(acts.map(lambda s: "view" in s).sum())
        vc = int(acts.map(lambda s: {"view", "add_to_cart"} <= s).sum())
        vcp = int(acts.map(lambda s: {"view", "add_to_cart", "purchase"} <= s).sum())
        return v, vc, vcp
    v, vc, vcp = funnel_of(df)
    funnel = pd.DataFrame({"stage": ["Просмотр", "Просмотр → корзина", "Просмотр → корзина → покупка"], "pairs": [v, vc, vcp]})
    funnel["conv_from_previous_pct"] = (100 * funnel.pairs / funnel.pairs.shift(1)).round(1)
    funnel["conv_from_first_pct"] = (100 * funnel.pairs / v).round(1)
    # по категориям: тот же расчёт внутри каждой категории
    rows = []
    for cat, g in df.groupby("category"):
        cv, cvc, cvcp = funnel_of(g)
        rows.append({"category": cat, "viewed": cv, "view_cart": cvc, "view_cart_purchase": cvcp,
                     "view_to_cart_pct": round(100 * cvc / cv, 1), "cart_to_purchase_pct": round(100 * cvcp / cvc, 1), "view_to_purchase_pct": round(100 * cvcp / cv, 1)})
    cat_conv = pd.DataFrame(rows)
    # справочно: соотношение числа событий (не конверсия)
    ev = pd.DataFrame({"action": [ACTION_RU[a] for a in ACTIONS], "events": [int(n[a]) for a in ACTIONS]})

    # --- динамика по месяцам ---
    df["month"] = pd.to_datetime(df.timestamp).dt.to_period("M").astype(str)
    monthly = (df[df.action == "purchase"].groupby("month").agg(purchases=("price", "size"), revenue=("price", "sum")).round(2).reset_index())

    # --- Excel: все метрики в одном файле ---
    summary = pd.DataFrame({"metric": ["Всего действий", "Пользователей", "Товаров", "Покупок", "Общая выручка", "Конверсия просмотр → корзина → покупка (пары пользователь-товар-категория), %"],
                            "value": [len(df), df.user_id.nunique(), df.product_id.nunique(), int(n["purchase"]), round(float(buy.price.sum()), 2),
                                      float(funnel.conv_from_first_pct.iloc[2])]})
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as xw:
        sheets = {"Сводка": summary, "Выручка по категориям": revenue, "Популярность по категориям": pop,
                  "Воронка (пары)": funnel, "Число событий": ev, "Конверсия по категориям": cat_conv,
                  "Выручка по месяцам": monthly, "Топ просмотров": tops["top_views"], "Топ корзины": tops["top_cart"], "Топ покупок": tops["top_purchases"]}
        for name, t in sheets.items():
            t.to_excel(xw, sheet_name=name[:31], index=False)
            ws = xw.sheets[name[:31]]
            for i, col in enumerate(t.columns, 1):
                ws.column_dimensions[ws.cell(1, i).column_letter].width = max(14, len(str(col)) + 4)

    # --- графики Plotly -> HTML ---
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    figs = {}
    figs["01_revenue_by_category"] = px.bar(revenue, x="category", y="revenue", text="revenue_share_pct", title="Выручка по категориям (подпись - доля, %)",
                                            labels={"category": "Категория", "revenue": "Выручка"})
    long = pop.melt(id_vars="category", var_name="action", value_name="count")
    long["action"] = long.action.map(ACTION_RU)
    figs["02_popularity_by_category"] = px.bar(long, x="category", y="count", color="action", barmode="group",
                                               title="Популярность категорий: число действий", labels={"category": "Категория", "count": "Действий", "action": "Действие"})
    figs["03_funnel"] = go.Figure(go.Funnel(y=list(funnel.stage), x=list(funnel.pairs), textinfo="value+percent previous"))
    figs["03_funnel"].update_layout(title="Воронка: просмотр → корзина → покупка (пары пользователь-товар-категория)")
    figs["04_conversion_by_category"] = px.bar(cat_conv.melt(id_vars="category", value_vars=["view_to_cart_pct", "cart_to_purchase_pct", "view_to_purchase_pct"], var_name="step", value_name="value").replace({"step": STEP_RU}),
                                               x="category", y="value", color="step", barmode="group", title="Конверсия по категориям (пары пользователь-товар-категория), %",
                                               labels={"category": "Категория", "value": "%", "step": "Шаг"})
    tp = tops["top_purchases"].assign(product_id=lambda t: "Товар " + t.product_id.astype(str))
    figs["05_top_products_purchases"] = px.bar(tp, x="cnt", y="product_id", orientation="h", title=f"Топ-{TOP_N} товаров по числу покупок",
                                               labels={"cnt": "Покупок", "product_id": ""}).update_yaxes(autorange="reversed")
    figs["06_revenue_by_month"] = px.line(monthly, x="month", y="revenue", markers=True, title="Выручка по месяцам (первый и последний месяцы в данных неполные)", labels={"month": "Месяц", "revenue": "Выручка"})
    figs["01_revenue_by_category"].update_traces(texttemplate="%{text}%", textposition="outside")
    for name, fig in figs.items():
        fig.update_layout(height=460, margin=dict(t=60, b=50), template="plotly_white")
        fig.write_html(HTML_DIR / f"{name}.html", include_plotlyjs="cdn")
    # единая страница со всеми графиками
    parts = [f.to_html(full_html=False, include_plotlyjs=True if i == 0 else False) for i, f in enumerate(figs.values())]
    (OUT / "dashboard.html").write_text("<html><head><meta charset='utf-8'><title>Анализ продаж</title></head><body style='font-family:sans-serif;max-width:1000px;margin:auto'>"
                                        "<h1>Система анализа и визуализации продаж</h1>" + "".join(parts) + "</body></html>", encoding="utf-8")

    best = revenue.iloc[0]
    log.info("Выручка всего %.2f; лидер категория %s (%.1f%%); воронка (пары пользователь-товар-категория): просмотр→корзина %.1f%%, корзина→покупка %.1f%%",
             buy.price.sum(), best.category, best.revenue_share_pct, funnel.conv_from_previous_pct.iloc[1], funnel.conv_from_previous_pct.iloc[2])
    return {"revenue": float(buy.price.sum()), "top_category": best.category}


def log_success(ti=None, **_):
    """Итоговое сообщение о завершении."""
    files = sorted(p.name for p in OUT.rglob("*") if p.is_file() and p.suffix in {".xlsx", ".html", ".db"})
    log.info("Пайплайн выполнен успешно. Результаты в %s: %s", OUT, ", ".join(files))


if __name__ == "__main__":      # запуск без Airflow: python dags/sales_pipeline.py
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for step in (load_to_sqlite, sql_top_products, analyze_and_visualize, log_success):
        step()
