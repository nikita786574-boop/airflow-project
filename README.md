# Airflow Project

Учебный проект по оркестрации ETL-пайплайнов в Apache Airflow.
Сквозная задача — выгрузка курсов валют с сервиса [exchangerate.host](https://exchangerate.host/documentation) (JSON → CSV → ClickHouse), которая от темы к теме переписывается всё более «правильными» средствами Airflow: от голого Python-скрипта до кастомных хуков, операторов и витрины данных.

## Стек

- **Apache Airflow** — оркестрация пайплайнов
- **ClickHouse** (+ `airflow-clickhouse-plugin`) — хранилище данных
- **PostgreSQL** — примеры работы через SQL-операторы
- **Python**: `pandas`, `requests`, `clickhouse-driver`
- **Telegram** (`apache-airflow-providers-telegram`) — уведомления о падении DAG

## Структура проекта

| № | Папка | Тема |
|---|-------|------|
| 01 | `01_pipeline_without_airflow` | ETL на чистом Python/Jupyter — без оркестратора |
| 02 | `02_pipeline_with_airflow` | Тот же ETL, обёрнутый в DAG с `PythonOperator` |
| 03 | `03_dependencies` | Зависимости между тасками, trigger rules |
| 04 | `04_backfilldag` | Backfill и Jinja-шаблоны (`{{ ds }}`) |
| 05 | `05_pipeline_jinja_backfill` | Идемпотентный пайплайн с backfill |
| 06 | `06_weight_rule` | Приоритеты тасок (`priority_weight`, `weight_rule`, pools) |
| 07 | `07_sql_operator_and_connection` | `SQLExecuteQueryOperator` и Connections |
| 08 | `08_hooks_connections_variables_pipeline` | `BaseHook`, Connections, Variables |
| 09 | `09_Xcom_branchoperator` | XCom + `BranchPythonOperator` |
| 10 | `10_Xcom_difference` | Инкрементальная загрузка через XCom |
| 11 | `11_ClickHouseOperator_pipeline` | `ClickHouseOperator`, табличная функция `url()` |
| 12 | `12_httpsensor_httpoperator` | `HttpOperator` и `HttpSensor` |
| 13 | `13_notification` | Уведомления в Telegram через `on_failure_callback` |
| 14 | `14_pipeline_notification` | Пайплайн + ClickHouseOperator + нотификации |
| 15 | `15_task_generation` | Динамическая генерация тасок из SQL-файлов |
| 16 | `16_custom_hook` | Кастомные hook и operator с нуля |
| 17 | `17_custom_clickhouse_hook_and_operator` | Наследование от `ClickHouseHook`/`ClickHouseOperator` (v1 vs v2) |
| 18 | `18_pipeline_clickhouse_operator_clickhouse_hook` | Пайплайн полностью средствами Airflow-плагина |
| 19 | `19_pipeline_with_data_mart` | Шаблон пайплайна с витриной данных (JOIN курсов и заказов) |

В корне: `dag_config.py` — служебный DAG, печатающий конфигурацию Airflow (executor, `sql_alchemy_conn`, parallelism, celery broker, hostname). Полезен, чтобы понять, на каком окружении реально исполняется таска.

## Запуск

1. Установить зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Положить нужный DAG-файл в папку `dags/` вашего Airflow.
3. В UI Airflow создать Connections: `clickhouse_default`, `postgres_default`, `api_exchange_rate` (host — URL API, password — access key), `report_api`, telegram-подключение для нотификаций.
4. Создать Variable `exchange_rate` (JSON с путями `s_file`, `csv_file`).
5. Локальные секреты — в `.env` (в git не попадает).

---

## Учебный журнал

### 01 — пайплайн без Airflow
ETL в Jupyter: GET-запрос к API валют → парсинг JSON (`quotes` за дату) → `pandas.DataFrame` → CSV → вставка в ClickHouse через `clickhouse-driver`. Работает, но: нет расписания, нет ретраев, нет логов, при падении на середине непонятно, что перезапускать. Это мотивация для оркестратора.

### 02 — тот же пайплайн в Airflow
Логика 01 разбита на три функции — `extract_data`, `transform_data`, `upload_to_clickhouse` — каждая обёрнута в свой `PythonOperator`. Зависимости заданы через `extract_task >> transform_task >> upload_task`. Параметры передаются через `op_kwargs` / `op_args`. Секреты читаются из `.env` через `python-dotenv`. Дата пока захардкожена, таблица дропается перед каждой загрузкой — оба недостатка чинятся в 04–05.

### 03 — зависимости и trigger rules
- `dependencies.ipynb`: сложный граф из `DummyOperator`. Списки работают с обеих сторон: `[t2, t4, t3] >> t6`, `[t4, t6, t5] >> t7` — так собирается «ромб» из веток.
- `trigger_rules.ipynb`: три типа завершения таски — успех, скип (`raise AirflowSkipException`), падение (`raise AirflowFailException`) — и то, как ведут себя следующие таски при разных `trigger_rule` (`all_success` по умолчанию, `one_failed`, `none_failed` и т.д.).

### 04 — backfill и Jinja
DAG с `start_date` и `end_date` в прошлом: Airflow сам догоняет пропущенные интервалы (backfill). В `op_kwargs` можно передавать Jinja-шаблон `'{{ ds }}'` — на каждом запуске он подставляется в дату конкретного DAG run, а не «сегодня». Так одна и та же таска обрабатывает разные даты.

### 05 — идемпотентный пайплайн
Пайплайн из 02, переделанный под backfill:

- Дата больше не константа — берётся из `kwargs['ds']`, URL собирается с `{{ ds }}`.
- Движок сменён с Log на **MergeTree** (`ORDER BY date`), потому что Log не поддерживает DELETE.
- Вместо DROP таблицы — `ALTER TABLE ... DELETE WHERE date='{{ ds }}'` перед INSERT: перезапуск за день перезаписывает только этот день (идемпотентность), нужна дозапись, а не дроп.
- `max_active_runs=1` — DAG runs идут по одному, нет конкуренции за общий файл на диске.

### 06 — weight_rule
Таски кладутся в общий пул `one_task_pool` (одна параллельная), у каждой свой `priority_weight`. `weight_rule='absolute'` — вес таски ровно тот, что задан, и не зависит от тасок до или после (в отличие от `downstream`/`upstream`, где вес суммируется по потомкам/предкам). Так контролируется порядок выхода тасок из очереди.

### 07 — SQL-оператор и Connection
`SQLExecuteQueryOperator` (пришёл на смену `PostgresOperator`) по `conn_id='postgres_default'` подключается к БД и выполняет заданный SQL. Параметры подключения живут в Airflow Connections, а не в коде.

### 08 — hooks, connections, variables
Из кода убраны все секреты:

- Параметры ClickHouse (host, login, password, schema) — из `BaseHook.get_connection('clickhouse_default')`.
- URL и ключ API — из connection `api_exchange_rate` (host + password).
- Пути к файлам — из Variable `exchange_rate` (`Variable.get(..., deserialize_json=True)` возвращает dict).

Connections и Variables создаются в UI Airflow, код становится переносимым между окружениями.

### 09 — XCom и ветвление
XCom — встроенный механизм передачи **маленьких** данных между тасками через базу метаданных Airflow: `ti.xcom_push(key, value)` / `ti.xcom_pull(key)`.
Первая таска пишет случайное число в XCom. Функция-фильтр читает его и возвращает `task_id` следующей таски (`'higher'` или `'lower'`). `BranchPythonOperator` получает эту функцию и передаёт управление только выбранной ветке, остальные скипаются.

### 10 — инкрементальная загрузка через XCom
Вместо полной перезаписи грузим только дельту:

- `xcom_pull(key='files_full', include_prior_dates=True)` — читаем список файлов, записанный **предыдущими** DAG runs (по умолчанию XCom виден только внутри текущего run).
- Сравниваем со свежим списком из API, разницу пушим под ключом `file_difference`, полный список — обратно в `files_full`.
- Вторая таска забирает разницу и дозагружает в ClickHouse только новые файлы (с `ALTER TABLE ... DELETE` по дате файла для идемпотентности).
- Ошибки HTTP поднимаются как `AirflowException`, чтобы таска честно падала.

### 11 — ClickHouseOperator и табличная функция url()
Весь ETL — одним SQL внутри ClickHouse, без Python-обработки:

- `create_table` и `insert_data` — два `ClickHouseOperator` с `clickhouse_conn_id='clickhouse_default'`.
- Данные читаются прямо из API табличной функцией `url(..., LineAsString)`, JSON разбирается функциями ClickHouse (`visitParamExtractRaw`, `splitByChar`, `arrayJoin`).
- Нюанс: SQL уже содержит Jinja (`{{ ds }}`), поэтому подставлять свои параметры через f-строки нельзя — конфликт фигурных скобок. Решение: `str.replace()` для своих плейсхолдеров.

### 12 — HttpOperator и HttpSensor
Паттерн «запусти отчёт и дождись готовности»:

- `HttpOperator` дергает `start_report`, в `response_check` достаёт `report_id` из ответа и кладёт в XCom.
- `HttpSensor` опрашивает `check_report/{{ti.xcom_pull(key='report_id')}}` каждые `poke_interval=2` секунды (mode `poke`, `timeout=60`), пока `response_check` не увидит `message == 'The report is ready!'`.

### 13 — нотификации в Telegram
В `default_args` DAG'а задан `on_failure_callback`. При падении любой таски колбэк создаёт `TelegramOperator` (conn + chat_id) и вызывает его `execute(context)` вручную — сообщение уходит в чат. Падение симулируется таской с `raise AirflowException`.

### 14 — пайплайн + оператор + нотификации
Сборка предыдущих тем: пайплайн из 08, но создание таблицы вынесено в отдельный узел `create_table` на `ClickHouseOperator` (DDL — декларативно в SQL, а не внутри Python-функции), плюс телеграм-нотификации из 13.

### 15 — динамическая генерация тасок
DAG-файл — это обычный Python, поэтому таски можно генерировать в цикле: `os.listdir()` по папке с SQL-файлами, на каждый файл — свой `ClickHouseOperator`, создающий VIEW из содержимого файла. Зависимости тоже в цикле: `tasks[i-1] >> tasks[i]`. Новый SQL-файл в папке = новая таска без правки DAG.

### 16 — кастомные hook и operator
- **Hook** — объект для взаимодействия с внешними системами (API, БД): инкапсулирует логику подключения, обычно читает параметры из Connection и даёт готовые методы. Здесь минимальный `CustomHook(BaseHook)` с одним методом.
- **Operator** — наследник `BaseOperator`; в `__init__` обязателен `super().__init__(**kwargs)`. Обязательный метод `execute(self, context)` — его Airflow вызывает при запуске таски. Внутри `execute` оператор создаёт хук и пользуется его методами.

### 17 — наследование от ClickHouseHook/ClickHouseOperator
`ClickHouseTransferHook` наследуется от `ClickHouseHook`, `ClickHouseTransferOperator` — от `ClickHouseOperator`. Две версии:

- `v1_...py` — **неправильно**: методы кастомного хука сами создают экземпляр родительского класса и вызывают его методы. Излишне — хук уже унаследовал эту логику и может выполнить её сам.
- `v2_...py` — **правильный полиморфизм**: `conn_id` передаётся в конструктор, отрабатывает инициализатор базового класса; `insert_df_to_db` использует унаследованный `execute` от `ClickHouseHook`.

### 18 — пайплайн полностью через плагин
Финальная версия пайплайна валют: `ClickHouseOperator` для SQL-шагов, `ClickHouseHook` для вставки данных из Python. Отдельный клиент `clickhouse-driver` больше не создаётся — все подключения идут через Airflow Connections и плагин. Код короче, секретов в нём нет.

### 19 — витрина данных (шаблон)
Заготовка пайплайна с витриной: помимо загрузки курсов создаётся таблица заказов `new_order_table (date, order_id, purchase_rub, purchase_usd)`, финальный шаг `insert_join` — `ClickHouseOperator` с INSERT ... SELECT, джойнящим заказы с курсами за `{{ ds }}` (пересчёт рублей в доллары). Граф: `extract >> transform >> [create_table, create_new_order_table] >> upload >> insert_join`. Места с `ВАШ КОД` — для самостоятельного заполнения по образцу 18.
