-- Для первой задачи буду использовать PG. В целом все это должно работать как в MPP на базе  PG - Vertica, Greenplum, Redshift
-- так и в Apache Spark 3.х. Отличаться может/будет только этап создания объектов и загрузка данных.
-- В условии указано разнесение данных на разные Бд, но в PG нельзя джойнить таблицы из разных баз. Потому буду использовать одну БД и разные схемы.
-- Что даст тот же уровень изоляции данных друг от друга - так как она нужна скорее логическая, для удобства хранения.


-- Создаем БД, схемы и таблицы. Заполняем их синтетическими данными
-- Сначала не добавляем контроль целостности и индексы, чтоб проще было создавать данные.

CREATE DATABASE "home_test";

CREATE SCHEMA "default";
CREATE SCHEMA "billing";

CREATE SCHEMA "orderstat";


DROP TABLE IF EXISTS "home_test"."default"."tb_users";

CREATE TABLE "home_test"."default"."tb_users" (
  uid bigint PRIMARY KEY,
  registration_date timestamp without time zone not null,
  country varchar(100) default 'None'
);

INSERT INTO "home_test"."default"."tb_users"(uid,registration_date,country) VALUES
(1,'2020-09-22 11:11:22','UK'),
(2,'2021-01-03 09:36:55','UK'),
(3,'2021-01-26 10:13:40','USA'),
(4,'2021-01-04 07:07:12','USA'),
(5,'2021-01-18 13:00:16','USA'),
(6,'2021-01-06 10:12:50','RU'),
(7,'2021-01-26 03:48:47','RU'),
(8,'2021-01-26 07:44:56','UK'),
(9,'2021-01-25 08:51:31','RU'),
(10,'2021-01-29 01:41:55','USA')
;


SELECT * FROM "home_test"."default"."tb_users"
;

-----------
CREATE TYPE account_types AS ENUM ('real', 'demo');

DROP TABLE IF EXISTS "home_test"."default"."tb_logins";

CREATE TABLE "home_test"."default"."tb_logins" (
  user_uid bigint not null,
  login bigint PRIMARY KEY,
  account_type account_types default 'demo'
);

INSERT INTO "home_test"."default"."tb_logins"(user_uid,login,account_type) VALUES
(1, 1,'real'),
(2, 2,'real'),
(2, 3,'demo'),
(2, 4,'demo'),
(3, 5,'real'),
(4, 6,'demo'),
(5, 7,'demo'),
(5, 8,'real'),
(5, 9,'real'),
(6, 10,'demo'),
(7, 11,'real'),
(8, 12,'demo'),
(9, 13,'demo'),
(10, 14,'demo')

;


SELECT * FROM "home_test"."default"."tb_logins"
;

--------

CREATE TYPE operation_types AS ENUM ('deposit', 'withdrawal');

DROP TABLE IF EXISTS "home_test"."billing"."tb_operations";

CREATE TABLE "home_test"."billing"."tb_operations" (
  operation_type operation_types default 'deposit',
  operation_date timestamp without time zone not null,
  login bigint,
  amount double precision not null
);

INSERT INTO "home_test"."billing"."tb_operations"(operation_type,operation_date,login, amount) VALUES

  ('deposit', '2020-09-22 11:11:25', 1, 1000)
  , ('deposit', '2021-01-23 12:10:25', 1, 3000)
  , ('withdrawal', '2021-01-30 09:00:25', 1, 2500)

  , ('deposit', '20201-01-03 09:40:00', 3, 1000)
  , ('deposit', '2021-01-08 17:10:00', 3, 3000)
  , ('deposit', '2021-01-20 13:45:36', 2, 5000)

  , ('deposit', '2021-01-26 10:15:40', 5, 1000)
  , ('deposit', '2021-01-27 17:10:00', 5, 500)
  , ('deposit', '2021-01-28 13:45:36', 5, 500)
  , ('deposit', '2021-01-29 13:45:36', 5, 1000)
  , ('withdrawal', '2021-01-30 00:45:36', 5, 3000)

  , ('deposit', '2021-01-04 07:07:56', 6, 3000)
  , ('withdrawal', '2021-01-30 09:00:25', 6, 2500)

  , ('deposit', '2021-01-18 13:01:16', 8, 3000)
  , ('deposit', '2021-01-19 07:07:12', 7, 1000)

  , ('deposit', '2021-01-06 10:13:00', 10, 3000)

  , ('deposit', '2021-01-26 03:48:57', 11, 800)

  , ('deposit', '2021-01-25 08:54:19', 13, 3000)
  , ('deposit', '2021-01-26 08:51:31', 13, 1000)

  , ('deposit', '2021-01-29 01:43:15', 14, 800)

;

SELECT * FROM "home_test"."billing"."tb_operations"
;

-------


DROP TABLE IF EXISTS "home_test"."orderstat"."tb_orders";

CREATE TABLE "home_test"."orderstat"."tb_orders" (
  login bigint,

  order_close_date timestamp without time zone not null
);

INSERT INTO "home_test"."orderstat"."tb_orders"(login, order_close_date) VALUES

  (1, '2020-09-23 10:11:25')
  , (1, '2021-01-24 12:00:05')

  , (3, '20201-01-03 12:40:00')
  , (3, '2021-01-08 18:00:10')
  , (2, '2021-01-20 13:46:36')

  , (5, '2021-01-26 10:17:40')
  , (5, '2021-01-27 18:00:00')
  , (5, '2021-01-28 13:57:09')

  , (6,'2021-01-04 07:08:34')
  , (6,'2021-01-30 09:34:25')

  , (7, '2021-01-19 07:27:10')

  , (10, '2021-01-06 10:14:00')

  , (11,'2021-01-26 03:59:57')
;

SELECT * FROM "home_test"."orderstat"."tb_orders"
;

---- создаем вторичные ключи. Это позволит делать минимальный контроль целостности + например, Tableau умеет тогда делать не очень страшные джойны))
ALTER TABLE "home_test"."default"."tb_logins"
ADD CONSTRAINT user_uid_fkey
FOREIGN KEY (user_uid)
REFERENCES "home_test"."default"."tb_users" (uid);

ALTER TABLE "home_test"."billing"."tb_operations"
ADD CONSTRAINT login_fkey
FOREIGN KEY (login)
REFERENCES "home_test"."default"."tb_logins" (login);

ALTER TABLE "home_test"."orderstat"."tb_orders"
ADD CONSTRAINT login_fkey
FOREIGN KEY (login)
REFERENCES "home_test"."default"."tb_logins" (login);

-- создаем индексы по всем полям джойнов в задачах.
CREATE INDEX tb_users_uid_idx ON "home_test"."default"."tb_users" (uid);

CREATE INDEX tb_logins_user_uid_idx ON "home_test"."default"."tb_logins" (user_uid);
CREATE INDEX tb_logins_login_idx ON "home_test"."default"."tb_logins" (login);

CREATE INDEX tb_operations_login_idx ON "home_test"."billing"."tb_operations" (login);

CREATE INDEX tb_orders_login_idx ON "home_test"."orderstat"."tb_orders" (login);


-- запрос 1. Написать запрос, который отобразит среднее время перехода пользователей между этапами воронки.

WITH logins AS( -- выбираем все реальные счета всех пользователей с датой регистрации за 90 дней
SELECT
  uid
  , login
  , registration_date
  , country
FROM
  "home_test"."default"."tb_logins" AS l
JOIN
  "home_test"."default"."tb_users" AS u
ON
 user_uid = uid
WHERE
  account_type = 'real'
  AND registration_date >= CURRENT_DATE - INTERVAL '90' DAY
)
, fist_deposit_by_login AS( -- выбираем первые внесения депозита за 90 дней
SELECT
  login
  , operation_date
  , amount
FROM (
  SELECT
  *
  , row_number() OVER (PARTITION BY login ORDER BY operation_date) AS rnk
FROM
  "home_test"."billing"."tb_operations"
WHERE
  operation_type = 'deposit'
  AND operation_date >= CURRENT_DATE - INTERVAL '90' DAY
) AS t
WHERE
  rnk =1
)
, orders_by_login AS ( --выбираем первые сделки за 90 дней

SELECT
  login
  , order_close_date
FROM (
  SELECT
    *
    , row_number() OVER (PARTITION BY login ORDER BY order_close_date) AS rnk
  FROM
    "home_test"."orderstat"."tb_orders"
  WHERE
    order_close_date >= CURRENT_DATE - INTERVAL '90' DAY
) AS t
WHERE
  rnk =1
)
, funnel AS ( -- строим воронку и считаем разницу в секундах между событиями
SELECT
  *
  , COALESCE(extract(epoch from operation_date - registration_date), 0) AS registration_to_deposit_duration
  , COALESCE(extract(epoch from order_close_date - operation_date), 0)  AS deposit_to_operation_duration
FROM
  logins AS l
LEFT JOIN
  fist_deposit_by_login AS f
USING (login)
LEFT JOIN
  orders_by_login AS o
USING (login)
)

SELECT -- считаем средние метрики
  country
 , COUNT(DISTINCT uid) AS users_count
 , AVG(registration_to_deposit_duration) AS avg_registration_to_deposit_duration
 , AVG(deposit_to_operation_duration) AS avg_deposit_to_operation_duration
 , percentile_cont(0.5) within group (order by registration_to_deposit_duration) as madian_registration_to_deposit_duration -- в реальной жизне возможно лучше считать медиану, а не avg
 , percentile_cont(0.5) within group (order by deposit_to_operation_duration) as madian_deposit_to_operation_duration
FROM
  funnel
GROUP  BY
  country
ORDER BY
  users_count DESC

;

--- запрос 2. Написать запрос, который отобразит количество всех клиентов по странам, у которых средний депозит >=1000


WITH clients AS ( -- все клиенты
SELECT
  uid
  , country
  , login
FROM
  "home_test"."default"."tb_logins" AS l
JOIN
  "home_test"."default"."tb_users" AS u
ON
  user_uid = uid
)
, clients_1000 AS ( -- клиенты у которых средний депозит >=1000
SELECT
  uid
  , country
  , AVG(amount) AS avg_amount
FROM
  clients AS c
JOIN
  "home_test"."billing"."tb_operations" AS o
ON
  c.login = o.login
WHERE
  operation_type = 'deposit'
GROUP BY
  1, 2
HAVING
  AVG(amount) >= 1000
)

SELECT
  country
  , COUNT(DISTINCT  uid) AS users_1000 -- кол-во клиентов с средним депозит >=1000
  , (SELECT COUNT(DISTINCT uid) FROM clients AS c WHERE c.country = c1000.country) AS total_users -- всего клиентов в стране. можно переписать на LEFT JOIN, но так компактней)))
FROM
 clients_1000 as c1000
GROUP BY
  1
;

--- запрос 3. Написать запрос, который выводит первые 3 депозита каждого клиента.
SELECT -- кажется тут все понятно))
  *
FROM (
  SELECT
     uid
    , l.login
    , operation_date
    , row_number() over (PARTITION BY uid ORDER BY operation_date) AS rnk
  FROM
    "home_test"."default"."tb_logins" AS l
  JOIN
    "home_test"."default"."tb_users" AS u
  ON
    user_uid = uid
  JOIN
    "home_test"."billing"."tb_operations" AS o
  ON
    l.login = o.login
  WHERE
    operation_type = 'deposit'
) AS t
WHERE
  rnk <=3
;
