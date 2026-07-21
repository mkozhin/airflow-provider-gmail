# Резолвер манифестов + полные URI в XCom (2026-07-21)

## Overview

Подготовка провайдера к стыковке с новым `airflow-provider-tablefile`
(и любыми будущими потребителями вложений). Два изменения:

1. **XCom `GmailAttachmentsToS3Operator` — полные URI.** Сейчас `execute()`
   возвращает список голых object keys (`prefix/dt=.../<id>/_manifest.json`)
   без бакета и схемы — потребитель не может обратиться к файлу, не зная
   бакет из сторонних источников. Новый контракт: `s3://<bucket>/<key>`.
   Локальный оператор уже возвращает абсолютные пути — контракт
   выравнивается до «список полных путей к `_manifest.json`».
2. **Публичный резолвер манифестов.** Знание о схеме `_manifest.json`
   остаётся в этом пакете (его владельце): функция `resolve_attachments()`
   + тонкий оператор `GmailResolveAttachmentsOperator` разворачивают список
   манифестов в плоский список полных путей вложений, с выбором
   `pick="latest"` по `internal_date`. Downstream-потребители (tablefile)
   принимают готовые пути и МОГУТ ничего не знать о манифестах. Граница
   слоёв (codex-ревью, решение пользователя 2026-07-21): манифест ОСТАЁТСЯ
   публичным контрактом со слоем 2 — прямое его чтение (как в прод-DAG
   realcombi сегодня) легально и поддерживается; резолвер — официальный
   клиент этого контракта и рекомендуемый путь.

Целевая цепочка в DAG'ах:

```python
download = GmailAttachmentsToS3Operator(...)           # → list[s3://.../_manifest.json]
resolve  = GmailResolveAttachmentsOperator(            # → list[s3://.../report.xlsx]
    task_id="resolve", manifests=download.output, pick="latest")
parse    = TableFileToS3Operator(input_paths=resolve.output, ...)
```

Ломающее изменение XCom допустимо: потребителей контракта ещё нет
(решение пользователя, brainstorm 2026-07-21). Релиз — минорный бамп (0.3.0).

## Context (from discovery)

- `GmailAttachmentsToS3Operator._destination_path` (`operators/gmail.py`)
  возвращает object key через `s3_key(prefix, rel_path)`; `delivered`
  собирается из этих значений → XCom без бакета. Локальный
  `_destination_path` — абсолютный путь. Важно: в общем `execute()`
  базового класса **одно** значение `_destination_path` питает и XCom
  (`delivered`), и `files[].path` манифеста — после изменения эти два
  потребителя должны разойтись (см. Solution Overview, шов).
- Основные тесты общего `execute()` — в `tests/test_operator_base.py`:
  фикстура `_DictOperator._destination_path` возвращает `s3://bucket/...`,
  ассерты проверяют XCom и `files[].path` из одного источника — при
  разведении путей фикстуру и ассерты придётся переработать.
- Схема манифеста и парсинг: `manifest.py` (`Manifest.from_json`,
  `ManifestError`); `files[].path` — «S3 key ИЛИ абсолютный локальный путь»
  (docstring `FileEntry`). Схему манифеста НЕ меняем — это контракт слоя 2
  (ADR-0001, дедуп); бакет для `files[].path` резолвер берёт из URI самого
  манифеста.
- Сенсор (`sensors/gmail.py`) вычисляет ключи манифестов сам через
  `paths.manifest_key` — от XCom оператора не зависит, изменение его
  не затрагивает (проверить тестами, не кодом).
- Паттерны тестов: моки на уровне `googleapiclient`-сервиса и `S3Hook`,
  без сети; фикстуры в `tests/fixtures/`; покрытие ~99%.
- Зависимости: Airflow 2.9.1 (constraint-пин, см. AGENTS.md), extra `s3`.
- `internal_date` в манифесте — ISO-строка с offset; сравнивать как aware
  `datetime`, НЕ лексикографически (разные UTC-offset ломают порядок) —
  находка codex-ревью плана tablefile.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in
  that task — tests are not optional; success + error scenarios
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `pytest` (packaging-маркер деселектится сам)

## Testing Strategy

- **unit tests**: обязательны в каждой задаче; мок `S3Hook` (чтение
  манифестов), реальный `Manifest.from_json` на JSON-фикстурах
- **e2e tests**: в проекте нет — не применимо
- Полный прогон: `pytest`; покрытие:
  `pytest --cov=airflow_provider_gmail --cov-report=term-missing` (держим ~99%)
- Packaging-тесты (`-m packaging`) не трогаем — упаковка не меняется
  (новые модули подхватываются `packages.find`)

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

Ключевые решения (приняты пользователем в brainstorm 2026-07-21):

- **XCom = полные пути.** S3-вариант: `s3://<bucket>/<key>`; локальный —
  абсолютный путь (уже так). Внутренняя запись/чтение продолжают работать
  по ключам — URI только на выходе `execute()`.
- **Разведение XCom и манифеста — через шов наследования (ADR-0006).**
  Новый protected-метод `_xcom_path(rel_path)` на иерархии операторов:
  в базовом классе — дефолт `return self._destination_path(rel_path)`
  (локальный оператор наследует как есть), в S3-наследнике — override
  `s3://{self.bucket}/{s3_key(...)}`. Базовый `execute()` собирает
  `delivered` **только** через `_xcom_path`; `files[].path` манифеста
  продолжает идти через `_destination_path`. Запрещённые варианты:
  менять `_destination_path` S3-оператора на URI (URI утёк бы в манифест,
  сломав контракт слоя 2 / ADR-0001) и `isinstance`/`hasattr`-ветвление
  в базовом `execute()` (нарушение ADR-0006).
- **`resolve.py` — на верхнем уровне пакета (осознанное исключение).**
  Верхнеуровневые модули в проекте «чистые», I/O живёт в `hooks/`/`operators/`;
  `resolve_attachments` делает I/O, но остаётся на верхнем уровне: это
  публичный API-фасад для downstream-потребителей и второй владелец знания
  о манифестах рядом с `manifest.py` (не оператор и не хук); I/O в нём
  минимальный (прочитать байты → `Manifest.from_json`), S3Hook — ленивый
  импорт как в операторе. Не пере-предлагать перенос в `operators/`.
- **`paths.py` — единственный владелец `s3://`-знания** (архитектурный
  грилинг 2026-07-21): `s3_uri(bucket, *segments) -> str` (сборка через
  `s3_key`, гард 1024 байта бесплатно), `split_s3_uri(uri) -> (bucket, key)`
  (строгая: прямое разбиение строки — бакет до первого `/`, остаток — ключ
  как есть, БЕЗ `urlsplit`/`parse_s3_url`; не-S3 вход / пустой бакет /
  пустой ключ → `ValueError`), предикат `is_s3_uri(uri) -> bool`. Литерал
  `s3://` в src существует только в `paths.py`. Round-trip-инвариант
  `split_s3_uri(s3_uri(b, k)) == (b, k)` на ключах с `? # %`, пробелами и
  Unicode — чистые тесты `test_paths.py`. Инвариант гарантируется для
  НОРМАЛИЗОВАННЫХ ключей (финальное ревью 2026-07-21): `s3_uri` идёт через
  `s3_key`/`join_key`, которые схлопывают пустые сегменты и крайние слэши
  (`a//b` → `a/b`) — в проде все ключи уже нормализованы, а тест фиксирует
  поведение на ненормализованном входе явно. Оператор и резолвер только зовут
  эти функции; tablefile — отдельный пакет, его парсер `input_paths`
  остаётся своим, контракт «прямое разбиение» фиксируется документацией.
- **Спецсимволы убиты у источника** (codex-ревью + решение пользователя
  2026-07-21; codex-ревью №2 предлагал убрать это как scope creep —
  пользовательское решение СОХРАНЕНО, не пере-оспаривать):
  `sanitize_filename` расширяется — символы из S3-списка «characters to
  avoid» плюс `?` (набор замены: `? # % { } ^ [ ] < > ~ |`, кавычки `"`
  и `` ` ``) заменяются на `_`. `\` и `/` в набор замены НЕ входят — их
  уже отрезает basename-логика `sanitize_filename` ДО шага замены
  (существующий контракт `a\b\c.xlsx → c.xlsx` сохраняется — фикс
  противоречия из codex-ревью №2). Пробелы и Unicode остаются (безопасны
  и для `urlsplit`, и для aws cli). Ключи URL-безопасны ПО ПОСТРОЕНИЮ →
  сторонний `parse_s3_url` работает корректно, противоречие с ADR-0007
  снято. Единая константа запрещённого набора — в `utils/paths.py`
  (владелец знания о ключах), `mime.py` её импортирует — это НОВАЯ
  межмодульная зависимость `mime → utils.paths` (финальное ревью
  2026-07-21: направление ациклично, `paths.py` остаётся чистым и mime
  не импортирует). `prefix`
  валидируется на тот же набор ТОЛЬКО по отрендеренному значению в начале
  `execute()`/`poke()` — как существующие `date_from`/`date_to`
  (codex-ревью №2: `prefix` — template field, Jinja-эвристика в `__init__`
  хрупка — пропускает `{#…#}`; паттерн проекта — валидация после рендера).
  Коллизии имён после замены разруливает существующий `resolve_collisions`;
  `files[].name` в манифесте хранит оригинальное имя вложения — не
  меняется. `split_s3_uri` остаётся прямым разбиением — это защита в
  глубину (старые/чужие ключи), а не контракт. Это ВТОРОЕ ломающее
  изменение релиза 0.3.0 (новые имена объектов для будущих скачиваний;
  ранее допустимые `prefix` со спецсимволами теперь отвергаются) —
  CHANGELOG обязан назвать его отдельно.
- **Схема манифеста не меняется.** `files[].path` остаётся key/абс.путём;
  резолвер восстанавливает полные URI по бакету из URI манифеста.
- **`pick="latest"` живёт здесь** (логика почтового домена): победитель —
  максимум `internal_date` как aware `datetime`, tie-breaker — `message_id`;
  вложения только победителя. `pick="all"` (дефолт) — вложения всех
  манифестов в порядке входного списка.
- **Два API**: функция `resolve_attachments(manifests, pick, aws_conn_id)`
  для taskflow и оператор-обёртка `GmailResolveAttachmentsOperator` для
  декларативных DAG'ов.
- Пустой вход → пустой выход (не ошибка): `resolve_attachments([]) == []`.
  Уточнение (codex-ревью 2026-07-21): в целевой цепочке этот кейс НЕ
  возникает — при пустом окне download кидает `AirflowSkipException`,
  resolve каскадно skipped и его код не выполняется; `[]` относится к
  прямому вызову функции / ручной сборке списка. Skip-контракт download
  НЕ меняем.

## Implementation Steps

### Task 1a: Чистый фундамент — спецсимволы и URI-хелперы

- [x] ADR-0007 (`docs/adr/0007-xcom-full-paths-manifest-keeps-keys.md`) —
  написан в ходе грилинга 2026-07-21: XCom = полные пути (URI без endpoint,
  пара с `aws_conn_id` потребителя), манифест остаётся на ключах, шов
  `_xcom_path`. Реализация Tasks 1a/1b обязана ему соответствовать.

**Files:**
- Update: `src/airflow_provider_gmail/utils/paths.py`, `tests/test_paths.py`,
  `src/airflow_provider_gmail/utils/mime.py`, `tests/test_mime.py`

- [x] спецсимволы у источника (см. Solution Overview): константа
  запрещённого в ключах набора — в `paths.py` (БЕЗ `\` и `/` — их режет
  basename-логика до шага замены); `sanitize_filename` (`mime.py`)
  заменяет символы набора на `_` после basename-шага (пробелы/Unicode
  не трогает)
- [x] `paths.py`: `s3_uri` / `split_s3_uri` / `is_s3_uri` (см. Solution
  Overview); контракт СИММЕТРИЧЕН (codex-ревью №2): `s3_uri` с пустым
  `bucket` или пустым итоговым ключом → `ValueError` (builder не может
  породить URI, который парный `split_s3_uri` не примет — round-trip
  без дыр); обе стороны пары пишутся в одной задаче
- [x] write tests `test_mime.py`: замена каждого символа набора; пробелы
  и кириллица сохраняются; существующий контракт
  `a\b\c.xlsx → c.xlsx` НЕ ломается (basename до замены); имя ЦЕЛИКОМ из
  спецсимволов: `???` → `___` (замена, НЕ fallback — проверка
  «пусто/только точки» остаётся до шага замены; финальное ревью
  2026-07-21); коллизии после замены идут через `resolve_collisions`
  (тест — рядом с существующими тестами `resolve_collisions`)
- [x] write tests `test_paths.py`: round-trip
  `split_s3_uri(s3_uri(b, k)) == (b, k)` на ключах с `? # %`, пробелами,
  Unicode; error-кейсы СИММЕТРИЧНО для builder и splitter: не-S3 вход,
  `s3:///key` (пустой бакет), `s3://bucket` и `s3://bucket/` (пустой
  ключ) → `ValueError`; `s3_uri("", "key")` / `s3_uri("bucket", "")` →
  `ValueError`; ненормализованный ключ (`a//b`, `/a`) → в URI попадает
  нормализованная форма (`a/b`, `a`) — поведение зафиксировано явно
  (финальное ревью 2026-07-21); `is_s3_uri`: `s3://…` → True, локальный
  путь / `http://…` → False
- [x] run tests - must pass before next task

### Task 1b: Шов _xcom_path и валидация prefix

**Files:**
- Update: `src/airflow_provider_gmail/operators/gmail.py`,
  `src/airflow_provider_gmail/sensors/gmail.py`,
  `tests/test_operator_base.py`, `tests/test_operator_s3.py`,
  `tests/test_operator_local.py`, `tests/test_sensor_s3.py`

- [ ] валидация `prefix` в `GmailAttachmentsToS3Operator` и
  `GmailAttachmentToS3Sensor` — ТОЛЬКО по отрендеренному значению в
  начале `execute()`/`poke()`, как существующие `date_from`/`date_to`
  (codex-ревью №2: `prefix` — template field, Jinja-эвристика в
  `__init__` хрупка; в `__init__` НЕ валидируем); мотивация для сенсора
  (финальное ревью 2026-07-21) — паритет конфиг-валидации с оператором
  (сенсор URI не эмитит, его ключи от спецсимволов в prefix не ломаются;
  ранний фейл единообразен, будущим обзорам не удалять)
- [ ] write tests (prefix): шаблонный `prefix="{{ ds }}"` НЕ падает при
  конструировании оператора/сенсора; невалидный отрендеренный prefix →
  `ValueError` в `execute()`/`poke()`; валидный prefix проходит
- [ ] шов-метод `_xcom_path(rel_path)` (см. Solution Overview): в базовом
  классе дефолт → `_destination_path`; в `GmailAttachmentsToS3Operator`
  override → `s3_uri(self.bucket, self.prefix, rel_path)`; ОБЕ ветки
  `delivered.append(...)` в `execute()` — `DELIVER_ONLY` (строка ~363) и
  `DOWNLOAD_AND_DELIVER` (строка ~398) — переводятся на `_xcom_path`
  (финальное ревью 2026-07-21: иначе легко оставить `DELIVER_ONLY` на
  голом ключе → неконсистентный XCom между ветками); переменная
  `manifest_path` из `_destination_path` в `execute()` больше не
  вычисляется; `_destination_path` и `files[].path` манифеста НЕ трогаем
- [ ] локальный оператор: подтвердить абсолютные пути в XCom (поведение не
  меняется — наследует дефолт `_xcom_path`; только тест-фиксация контракта)
- [ ] докстринги обновить: `_destination_path` базового класса («for the
  manifest / XCom» → теперь только манифест), докстринги классов
  `GmailAttachmentsToS3Operator` и `GmailAttachmentsToLocalOperator`
  (формулировки «manifest paths … returned in XCom»), докстринг нового
  `_xcom_path`
- [ ] `tests/test_operator_base.py`: переработать фикстуру `_DictOperator` —
  `_destination_path` возвращает ГОЛЫЙ ключ, `_xcom_path` — `s3://bucket/...`
  (финальное ревью 2026-07-21: если оба вернут `s3://...`, разведение
  вообще не будет протестировано); развести ассерты — `files[].path` в
  манифесте из `_destination_path` (ключ), XCom из `_xcom_path` (URI),
  включая ветку `DELIVER_ONLY`
- [ ] write tests: S3 — XCom содержит `s3://bucket/...`-URI, а внутренние
  операции и манифест — ровно те же ключи, что раньше; local — абсолютные
  пути; сенсор-тесты зелёные без правок (независимость сенсора от XCom)
- [ ] run tests - must pass before next task

### Task 2a: from_local_iso в dates.py

**Files:**
- Create: `tests/test_dates.py`
- Update: `src/airflow_provider_gmail/dates.py`

- [ ] `dates.py`: `from_local_iso(value: str) -> datetime` (aware) рядом с
  `to_local_iso` — владелец формата `internal_date` парсит его сам, резолвер
  формата не знает (арх. грилинг 2026-07-21, кандидат 3); строка БЕЗ offset
  (naive) → `ValueError` с внятным сообщением прямо в точке парсинга
  (иначе naive проскочит `fromisoformat` и взорвётся невнятным `TypeError`
  на сравнении); в `ManifestError` НЕ оборачивать (он строго про схему
  `from_json`); вызываться будет ТОЛЬКО из `pick="latest"` — режим `all`
  дату не читает и не валидирует (YAGNI + GIGO, codex-ревью 2026-07-21)
- [ ] write tests (НОВЫЙ чистый `tests/test_dates.py`): round-trip
  `from_local_iso(to_local_iso(ms, tz))` == момент `ms` (несколько зон);
  naive-строка → `ValueError`; malformed-строка → `ValueError`;
  существующие date-тесты из `test_operator_base.py` НЕ мигрируем
- [ ] run tests - must pass before next task

### Task 2b: Чистое ядро _resolve

**Files:**
- Create: `src/airflow_provider_gmail/resolve.py`, `tests/test_resolve.py`
- Update: `tests/fixtures/` (JSON-фикстуры манифестов при необходимости)

- [ ] `resolve.py`: приватное ЧИСТОЕ ядро `_resolve(pairs, pick) ->
  list[str]` (арх. грилинг 2026-07-21, кандидат 4); `pairs:
  list[(путь манифеста, Manifest)]` — внутри ядра и выбор победителя, и
  сборка полных путей вложений; ветвление — по `is_s3_uri(путь самого
  манифеста)` (финальное ревью 2026-07-21): S3-манифест →
  `files[].path` → `s3_uri(<бакет из split_s3_uri(пути манифеста)>, key)`;
  локальный манифест → `files[].path` как есть, `split_s3_uri` НЕ
  вызывается (бакета нет); чистые `s3_uri`/`split_s3_uri` из `paths.py`;
  пара нужна, т.к. бакет вложений берётся из пути самого манифеста; ядро
  остаётся приватным (тесты импортируют `_resolve` напрямую — внутренний
  шов)
- [ ] `pick="latest"`: `internal_date` → aware `datetime` (через
  `from_local_iso` из `dates.py` — только в этом режиме), сравнение
  моментов времени, tie-breaker `message_id`; `pick` валидируется
  (`latest|all`), иначе `ValueError`; победитель с пустым `files` → `[]`
  КАК ЕСТЬ, без fallback на следующее по дате письмо (fallback тихо
  доставил бы устаревшую версию отчёта — решение грилинга 2026-07-21)
- [ ] дубликаты во входном списке НЕ дедуплицируются и НЕ валидируются
  (чинить или интерпретировать чужой вход — не задача резолвера, может
  быть и намеренно — решение грилинга 2026-07-21); следствия по режимам
  (codex-ревью): `all` — дубликат на входе → дубликат на выходе;
  `latest` — дубликат лишь повторный кандидат, победитель один и вложения
  возвращаются один раз; квалифицировать в докстринге
- [ ] write tests: ЧИСТАЯ семантическая матрица на `_resolve` с фикстурами
  `Manifest`, БЕЗ моков: local-манифест (путь не `s3://`) — `files[].path`
  возвращаются как есть, `split_s3_uri` не вызывается (финальное ревью
  2026-07-21); all/latest; смесь offset'ов в `internal_date`
  (лексикографический порядок ≠ хронологический — тест именно на это);
  tie-breaker; пустой победитель; naive/malformed `internal_date` при
  `latest` → `ValueError` (и НЕ трогается при `all`); дубликаты в обоих
  режимах (см. выше); неизвестный `pick` → `ValueError`; сборка URI для
  вложений с пробелами и Unicode; defense-in-depth — ключ со «старым»
  спецсимволом (`? # %`): новые ключи их не содержат (sanitization,
  Task 1a), но ядро не должно их портить, если встретит чужой/старый ключ;
  фикстура для этого теста собирается ВРУЧНУЮ (сырой ключ со
  спецсимволами), НЕ через новый `sanitize_filename` — иначе тест
  проверяет пустоту (финальное ревью 2026-07-21)
- [ ] run tests - must pass before next task

### Task 2c: Публичный фасад resolve_attachments (I/O-кромка)

**Files:**
- Update: `src/airflow_provider_gmail/resolve.py`, `tests/test_resolve.py`

- [ ] `resolve_attachments(manifests: list[str], pick: str = "all",
  aws_conn_id: str = "aws_default") -> list[str]` — тонкая I/O-кромка над
  `_resolve`: валидация `pick` — ПЕРВОЙ строкой, ДО любого I/O
  (codex-ревью №2: иначе неизвестный `pick` даст `ClientError` вместо
  `ValueError`; в т.ч. `resolve_attachments([], pick="invalid")` →
  `ValueError`, не `[]`); каждый путь — `is_s3_uri` → `split_s3_uri` (из
  `utils/paths.py`, НЕ `parse_s3_url` — см. Solution Overview),
  `S3Hook.read_key` → `Manifest.from_json`; иначе — локальное чтение
  файла; собранные `pairs` → `_resolve` (который валидирует `pick`
  повторно — защита внутреннего шва)
- [ ] жизненный цикл `S3Hook` (codex-ревью №2, паттерн проекта): ленивый
  импорт; ОДИН hook на вызов функции — создаётся при первом S3-URI,
  переиспользуется для остальных; `aws_conn_id` передаётся в конструктор;
  при входе только из локальных путей Amazon-провайдер НЕ импортируется
  (функция работает без extra `s3`)
- [ ] пустой список `manifests` → `[]`; `None` НЕ маскировать — никаких
  `if not manifests` (проглотит `None` от `xcom_pull` по неверному task_id →
  вечно-зелёный пустой пайплайн); `None` падает естественным `TypeError`
  (решение грилинга 2026-07-21); битый манифест → `ManifestError` наверх
  (не глотать)
- [ ] отсутствующий манифест (URI есть, объекта нет: удалили, ретеншн,
  `aws_conn_id` смотрит не в то хранилище) → естественная ошибка наверх
  без обёртки и без предпроверки `check_for_key` (S3 → ClientError
  NoSuchKey/NoSuchBucket/AccessDenied, local → FileNotFoundError);
  `ManifestError` остаётся строго про содержимое (решение грилинга
  2026-07-21)
- [ ] спецсимволы: после ужесточения `sanitize_filename` (Task 1) новые
  ключи URL-безопасны по построению; разбор всё равно ТОЛЬКО `split_s3_uri`
  из `paths.py` (защита в глубину — старые/чужие ключи могут содержать
  `? # %`); правило согласовано с планом tablefile (его
  `input_paths`-парсер работает прямым разбиением)
- [ ] write tests (I/O-кромка, с моками): s3-чтение (мок `S3Hook`);
  локальное чтение; битый JSON → `ManifestError` наверх; отсутствующий
  манифест — S3: мок кидает ClientError → наверх без обёртки, local:
  `FileNotFoundError`; пустой вход → `[]`; `None` → `TypeError` (не
  маскируется); неизвестный `pick` через ПУБЛИЧНЫЙ API → `ValueError`
  ДО единого обращения к хранилищу (мок не тронут), в т.ч.
  `resolve_attachments([], pick="invalid")` → `ValueError`; жизненный
  цикл hook: один `S3Hook` на вызов при нескольких s3-URI,
  `aws_conn_id` доходит до конструктора, локальный вход не импортирует
  Amazon-провайдер; один сквозной тест с спецсимвольным ключом
  (`?`, `#`, `%`, пробел) как интеграционная страховка
- [ ] run tests - must pass before next task

### Task 3: Оператор GmailResolveAttachmentsOperator

**Files:**
- Create: `src/airflow_provider_gmail/operators/resolve.py`,
  `tests/test_operator_resolve.py`

- [ ] тонкая обёртка над `resolve_attachments`: параметры `manifests`
  (XCom download-оператора), `pick`, `aws_conn_id`;
  `template_fields = ("manifests", "pick")`; return → XCom
  (`get_provider_info()` не трогаем: ключа `python-modules` в нём нет,
  операторы регистрации в provider_info не требуют — импортируются
  пользователем напрямую)
- [ ] write tests (codex-ревью 2026-07-21 — success И error):
  success — execute → список URI (мок `S3Hook`), XCom-структура;
  делегирование — `manifests`/`pick`/`aws_conn_id` доходят до
  `resolve_attachments` ровно как переданы (мок функции); состав
  `template_fields` == `("manifests", "pick")`; пустой вход → `[]` в XCom;
  error — неизвестный `pick` → `ValueError` наверх (таск красный),
  `ManifestError` из `resolve_attachments` → наверх (не глотается)
- [ ] run tests - must pass before next task

### Task 4: [Final] Документация, примеры, changelog

**Files:**
- Update: `README.md`, `README_RU.md`, `CHANGELOG.md`, `CONTEXT.md`,
  `docs/gmail-pipeline-layers-2-3.md`,
  `example_dags/example_gmail_to_s3.py`, `tests/test_example_dags.py`

- [ ] README/README_RU: новый контракт XCom (полные пути), явно: URI
  `s3://bucket/key` адресует объект только в паре с `aws_conn_id`
  потребителя — endpoint живёт в connection, не в URI (решение грилинга
  2026-07-21); раздел про резолвер (функция + оператор, `pick`), целевая
  цепочка с `TableFileToS3Operator` как пример downstream
- [ ] example_dags (конкретный scope, codex-ревью 2026-07-21): обновляется
  ТОЛЬКО `example_gmail_to_s3.py` — добавить resolve-таск (целевая
  цепочка, см. Solution Overview); `example_gmail_to_local.py` и
  `example_gmail_s3_backfill.py` НЕ меняются (минимальный scope: один
  демонстрационный пример резолвера достаточен)
- [ ] CONTEXT.md: уже обновлён опережающе в ходе грилинга 2026-07-21
  (термины Доставка, Резолвер, `pick`) — осознанный порядок работы
  (глоссарий фиксирует целевую модель; codex-ревью №2 отметил
  рассинхрон с кодом как временный); здесь — только сверить с фактической
  реализацией и поправить расхождения, если появились
- [ ] `docs/gmail-pipeline-layers-2-3.md` (граница слоёв, codex-ревью
  2026-07-21): резолвер — рекомендуемый путь получения файлов слоем 2;
  прямое чтение манифеста остаётся поддерживаемым контрактом (realcombi);
  схема манифеста по-прежнему неизменна
- [ ] CHANGELOG.md: секция `## [0.3.0]` — ДВА breaking-пункта (codex-ревью
  №2): (1) XCom keys → full URI; (2) ужесточение `sanitize_filename`
  (спецсимволы → `_`, новые имена объектов для будущих скачиваний) +
  валидация `prefix` (ранее допустимые prefix со спецсимволами теперь
  отвергаются); added (resolver: `resolve_attachments` +
  `GmailResolveAttachmentsOperator`,
  `from_local_iso`, URI-helpers в `paths.py`); ссылки внизу — ПОЛНЫЕ URL
  в формате существующего файла (`https://github.com/mkozhin/...`):
  `[Unreleased]` → `compare/v0.3.0...HEAD`, добавить
  `[0.3.0]: compare/v0.2.0...v0.3.0`
- [ ] write tests: `tests/test_example_dags.py` — DagBag импортируется без
  ошибок; структурная проверка ПОЛНОЙ цепочки примера в
  `example_gmail_to_s3.py` (codex-ревью №2): все task_ids на месте,
  `resolve` стоит downstream от `download` и upstream от следующего таска
  цепочки (если в примере есть parse-заглушка), тип resolve-таска —
  `GmailResolveAttachmentsOperator`
- [ ] run tests (полный прогон) - must pass
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

- релиз `v0.3.0` по тегу (publish workflow уже настроен)
- обновить пин в `airflow-provider-tablefile` (extra `dev`:
  `airflow-provider-gmail>=0.3`) — учтено в его плане
- прод-DAG `example_gmail_to_s3` (realcombi): при миграции на tablefile
  учесть новый формат XCom (полные URI) — мигрируется вместе с внедрением
  tablefile, отдельного шага не нужно
