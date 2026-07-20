# Airflow Project

Учебный проект по оркестрации ETL-пайплайнов в Apache Airflow.
Основная задача — выгрузка курсов валют за различные даты с сервиса [exchangerate.host](https://exchangerate.host/documentation), их преобразование и загрузка в ClickHouse.

## Стек

- **Apache Airflow** — оркестрация пайплайнов
- **ClickHouse** (+ `airflow-clickhouse-plugin`) — хранилище данных
- **PostgreSQL** — примеры работы через SQL-операторы
- **Python**: `pandas`, `requests`, `clickhouse-driver`
- **Telegram** — уведомления о падении DAG

## Структура проекта

Каждая папка — отдельная тема, от простого пайплайна без Airflow до кастомных хуков/операторов и витрины данных.

| № | Папка | Тема |
|---|-------|------|
| 01 | `01_pipeline_without_airflow` | Пайплайн на чистом Python/Jupyter — без оркестратора |
| 02 | `02_pipeline_with_airflow` | Тот же пайплайн, обёрнутый в DAG |
| 03 | `03_dependencies` | Зависимости между тасками, trigger rules |
| 04 | `04_backfilldag` | Backfill и Jinja-шаблоны в DAG |
| 05 | `05_pipeline_jinja_backfill` | Пайплайн с Jinja и backfill |
| 06 | `06_weight_rule` | Приоритеты тасок (`weight_rule`) |
| 07 | `07_sql_operator_and_connection` | `SQLExecuteQueryOperator` и Connections |
| 08 | `08_hooks_connections_variables_pipeline` | Hooks, Connections, Variables |
| 09 | `09_Xcom_branchoperator` | XCom + `BranchPythonOperator` |
| 10 | `10_Xcom_difference` | Дельта данных через XCom (`include_prior_dates`) |
| 11 | `11_ClickHouseOperator_pipeline` | `ClickHouseOperator`, табличная функция URL |
| 12 | `12_httpsensor_httpoperator` | `HttpSensor` и `HttpOperator` |
| 13 | `13_notification` | Уведомления в Telegram при падении DAG |
| 14 | `14_pipeline_notification` | Пайплайн + ClickHouse operator + нотификации |
| 15 | `15_task_generation` | Автогенерация тасок |
| 16 | `16_custom_hook` | Кастомные hook и operator |
| 17 | `17_custom_clickhouse_hook_and_operator` | Наследование от `ClickHouseHook`/`ClickHouseOperator` |
| 18 | `18_pipeline_clickhouse_operator_clickhouse_hook` | Пайплайн полностью через Airflow-плагин ClickHouse |
| 19 | `19_pipeline_with_data_mart` | Пайплайн с витриной данных |

Также в корне: `dag_config.py` — служебный DAG для просмотра конфигурации Airflow (executor, parallelism, broker и т.д.).

## Запуск

1. Установить Airflow и зависимости:
   ```bash
   pip install apache-airflow airflow-clickhouse-plugin clickhouse-driver pandas requests
   ```
2. Положить нужный DAG-файл в папку `dags/` вашего Airflow.
3. В UI Airflow создать Connections (`postgres_default`, подключение к ClickHouse) и Variables (в т.ч. `exchange_rate` с параметрами API).
4. Секреты хранятся в `.env` (в git не попадает).

---

## Учебный журнал

### 05
Изменил движок на MergeTree, потому что в Log-движке нельзя делать delete.
Перестал дропать таблицу каждый раз — нужна дозапись, а не дроп.
В DAG указал запуск по одному, чтобы не было конкуренции за запись в файл.

### 06
`weight_rule = absolute` — вес таски только тот, который задан, не зависит от тасок до или после.

### 07
`SQLExecuteQueryOperator` через коннектор `postgres_default` подключается к БД и выполняет указанный SQL-запрос.

### 08
Параметры подключения к ClickHouse (host, port, login, password) получаются через `BaseHook`.
Все переменные скрыты, включая пути к файлам. Переменные создаются в UI Airflow, читаются через `Variable.get()`.

### 09
Для передачи маленьких данных — встроенный XCom: через объект task значение пишется в метаданные Airflow и передаётся между тасками.
Получатель — функция-фильтр, которая по значению из XCom возвращает имя следующей таски. `BranchPythonOperator` передаёт управление таске с этим именем.

### 10
Через XCom находим разницу между метаданными: смотрим дельту и вместо полной перезаписи пишем только новые данные.
`include_prior_dates` позволяет читать XCom-значения, записанные не в текущем запуске DAG (по умолчанию доступны только значения текущего DAG run).

### 11
`ClickHouseOperator` для SQL-запросов. Загрузка данных прямо в ClickHouse через табличную функцию URL.

### 12
`HttpSensor` и `HttpOperator`.

### 13
Уведомления в Telegram при падении DAG.

### 14
Пайплайн выгрузки с ClickHouse operator и нотификациями. Добавился узел `create_table`.

### 15
Автогенерация тасок на основе `ClickHouseOperator` для чтения данных из файлов.

### 16
Кастомный hook и operator. В операторе обязательный метод `execute`, который Airflow вызывает при запуске таски.
Hook — объект для взаимодействия с внешними системами (API, БД): инкапсулирует логику подключения, читает параметры из Connection, даёт готовые методы.

### 17
`ClickHouseTransferHook` наследуется от `ClickHouseHook`, `ClickHouseTransferOperator` — от `ClickHouseOperator`.
В `_v1` наследование реализовано неправильно: в методах кастомного хука всё равно создаётся родительский класс и вызываются его методы, хотя хук наследует эту логику.
В `_v2` — правильный полиморфизм: при создании `ClickHouseTransferHook` передаём `conn_id`, отрабатывает инициализатор базового класса; в `insert_df_to_db` используется унаследованный `execute`.

### 18
Пайплайн выгрузки из API валют, преобразования и загрузки в ClickHouse.
Всё через `ClickHouseOperator` и `ClickHouseHook` — без отдельного подключения через клиент ClickHouse, только Airflow и его плагин.
