-- Этап 2. SQL-показатели (SQLite). Параметр :lim - сколько товаров в топе.
-- Топ товаров по просмотрам
SELECT product_id, COUNT(*) AS cnt FROM user_actions WHERE action = 'view'        GROUP BY product_id ORDER BY cnt DESC, product_id LIMIT :lim;
-- Топ товаров по добавлениям в корзину
SELECT product_id, COUNT(*) AS cnt FROM user_actions WHERE action = 'add_to_cart' GROUP BY product_id ORDER BY cnt DESC, product_id LIMIT :lim;
-- Топ товаров по покупкам
SELECT product_id, COUNT(*) AS cnt FROM user_actions WHERE action = 'purchase'    GROUP BY product_id ORDER BY cnt DESC, product_id LIMIT :lim;
