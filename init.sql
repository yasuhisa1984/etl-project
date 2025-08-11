-- 初回起動時に自動実行される初期化SQL
CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name TEXT,
    price INT
);
