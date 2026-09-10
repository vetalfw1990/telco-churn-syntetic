# Промпт для генерації Airflow 3 DAG

Sprint 4 · Homework part 2 (30 балів) · Prompt Engineering

**Завдання:** написати промпт, який змусить AI-чатбот згенерувати DAG, що чекає на
файл `data.csv` у папці `/data` через Sensor, рахує кількість рядків Python-функцією
і передає це число далі через XCom. Вимоги: ідемпотентність задач і TaskFlow API.

**Результат:** файл `csv_row_count_pipeline.py`, перевірений на Apache Airflow 3.1.7
(`import_errors: {}`) і запущений у живому стеку — сенсор дочекався файлу, XCom
передав 100 рядків.

---

## Текст промпта

```
You are a senior data engineer writing production Airflow DAGs.

Write a single Airflow 3 DAG file that implements this pipeline:

1. Wait for a file named `data.csv` to appear in the `/data` folder.
   Use a Sensor for this — do not poll inside a Python task.
2. Once the file exists, run a Python function that counts the number
   of data rows in the CSV (excluding the header row).
3. Pass that count to a downstream task via XCom, and have that task
   print the count to the task log.

Hard requirements:

- Target Apache Airflow 3.1 and author with the stable `airflow.sdk`
  namespace (`from airflow.sdk import dag, task, Param`). Do not use the
  Airflow 2.x paths `airflow.decorators`, `airflow.models.DAG`, or
  `airflow.operators.*` — core operators and sensors now live in
  `apache-airflow-providers-standard`.
- Use the TaskFlow API: define the Python steps as `@task`-decorated
  functions and pass the row count as a return value / function argument
  so XCom is handled implicitly, not via manual `xcom_push` / `xcom_pull`.
- Every task must be idempotent: re-running any task with the same input
  must produce the same result and must not mutate or delete the source
  file. State the reasoning in a short comment on each task.
- Configure the sensor so it does not occupy a worker slot while waiting,
  and give it a finite timeout so a missing file fails the run instead of
  hanging forever.
- Make the file path a DAG-level `Param` with `/data/data.csv` as the
  default, so the DAG can be pointed at another path at trigger time
  without editing code.
- Keep DAG-parse time light: any non-trivial import belongs inside the
  task body, not at module top level.
- Set `schedule=None`, `catchup=False`, an explicit `start_date`, and
  sensible `retries` in `default_args`.

Output format: one complete, runnable `.py` file, ready to drop into the
`dags/` folder, with a short module docstring describing the flow. After
the code, add three sentences explaining how idempotency is achieved and
why the sensor mode you chose is the right one.
```

---

## Чому промпт побудований саме так

**Роль і контекст на початку.** «Senior data engineer writing production DAGs»
зміщує відповідь від навчального прикладу до коду, який не соромно покласти в репо.

**Заборона Airflow 2.x-шляхів.** Без неї моделі майже завжди генерують
`from airflow.decorators import dag, task` — у навчальних даних цього набагато
більше, ніж Airflow 3. Явна заборона конкретних імпортів працює краще, ніж
загальне «use Airflow 3».

**Вимога передавати значення поверненням функції.** Це і є TaskFlow API. Якщо
просто написати «use XCom», модель напише `xcom_push` / `xcom_pull` — формально
XCom, але не той стиль, якого вимагає завдання.

**Прохання обґрунтувати ідемпотентність.** Ключовий прийом: коли модель мусить
пояснити, чому задача ідемпотентна, вона обирає read-only реалізацію. Без цієї
вимоги типовий згенерований код «прочитав файл і перемістив в archive/» —
зручно, але не ідемпотентно.

**Опис поведінки сенсора замість назви режиму.** У промпті написано «не має
займати слот воркера», а не «використай `mode='reschedule'`». Так перевіряється
розуміння моделі, а не її здатність підставити задану константу.

**Явний формат виводу.** Один файл, готовий до `dags/`, плюс три речення
пояснення — це прибирає риторику навколо коду і дає артефакт, який можна
одразу використати.

---

## Пояснення, яке промпт замовляє в кінці

> **Idempotency and sensor mode.** Every task is read-only with respect to the
> source data: the sensor merely checks that the path exists, `count_rows` opens
> the file for reading and never moves, renames or truncates it, and
> `report_row_count` only writes to the task log — so re-running any task on the
> same input yields the same result and leaves the filesystem untouched. The row
> count is returned as a plain value and consumed as a function argument, so XCom
> is managed by the TaskFlow API rather than by manual `xcom_push`/`xcom_pull`
> calls that could leave half-written state behind on a retry. `mode="reschedule"`
> is the right choice here because the wait can last up to an hour: instead of
> occupying a worker slot while sleeping between pokes, the task frees the slot
> and is re-queued at each `poke_interval`, and the finite `timeout` guarantees
> the run fails loudly if the file never arrives rather than hanging indefinitely.

---

## Як перевірити згенерований код

```bash
cd ~/sprint4/airflow-mentor-session

# 1. покласти файл поруч з іншими DAG'ами (папка примонтована в контейнер)
cp csv_row_count_pipeline.py dags/

# 2. з'єднання для FileSensor — в Airflow 3 не завжди створюється саме
docker compose exec airflow airflow connections get fs_default
docker compose exec airflow airflow connections add fs_default \
  --conn-type fs --conn-extra '{"path": "/"}'

# 3. переконатись, що файлу ще немає — інакше не побачите, як сенсор чекає
ls -la data/data.csv

# 4. запустити; у цьому стеку дані змонтовані в /opt/airflow/data, не в /data
docker compose exec airflow airflow dags unpause csv_row_count_pipeline
docker compose exec airflow airflow dags trigger csv_row_count_pipeline \
  --conf '{"csv_path": "/opt/airflow/data/data.csv"}'

# 5. сенсор перейшов в up_for_reschedule — тепер створюємо файл
head -101 data/telco_churn_full.csv > data/data.csv

# 6. через ~30 секунд у логах report_row_count:
#    /opt/airflow/data/data.csv contains 100 data rows.
docker compose exec airflow bash -c \
  "grep -rh 'contains .* data rows' /opt/airflow/logs/"
```
