# Исправления по итогам code review (2026-07-12)

## Overview

Точечные исправления находок глубокого code review провайдера
`airflow-provider-gmail`. Три категории:

1. **Тихая потеря данных в MIME-парсере** — письма/вложения молча выпадают из
   доставки при зелёной задаче (худший вид отказа: невидимый и постоянный).
2. **Ненадёжный dedup и запросы** — исключение обработанных писем через
   `-label:"..."` опирается на недокументированное поведение Gmail-поиска и
   может «отказывать открытым» (дубли доставки); метасимволы в значениях
   запроса парсятся как операторы; S3-сенсор игнорирует `run_id` квитанции
   вопреки ADR-0001.
3. **Документация обещает то, чего нет** — `soft_fail`, отсутствие
   предупреждения о backfill над общим префиксом.

Плюс дешёвый cleanup (константы, единый владелец раскладки, `os.path.splitext`).

Скоуп согласован с пользователем в brainstorm-сессии 2026-07-12; принятые
решения зафиксированы в задачах ниже и пересмотру в ходе реализации не
подлежат (изменение скоупа — только через пользователя).

## Context (from discovery)

- Затронутые модули: `src/airflow_provider_gmail/utils/mime.py`,
  `hooks/gmail.py`, `operators/gmail.py`, `sensors/gmail.py`,
  `utils/paths.py`; тесты `tests/test_mime.py`, `test_hook_query.py`,
  `test_hook_labels.py`, `test_hook_search.py`, `test_operator_*.py`,
  `test_sensor*.py`; доки `CONTEXT.md`, `README.md`, `README_RU.md`,
  `example_dags/`.
- Паттерны: моки на уровне `googleapiclient`-сервиса (`hook.get_conn()`),
  без сети; фикстуры-JSON в `tests/fixtures/gmail/`; канонические термины из
  `CONTEXT.md`; поведенческие контракты — в ADR (`docs/adr/`).
- Зависимости: Airflow 2.9.1 (ставить только с constraint-пином, см.
  AGENTS.md), `google-api-python-client`, extra `s3`.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in
  that task — tests are not optional; success + error scenarios
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `pytest` (packaging-маркер деселектится сам)
- maintain backward compatibility где это не противоречит фиксу (провайдер
  ещё не опубликован, ломающие изменения сигнатур хука допустимы)

## Testing Strategy

- **unit tests**: обязательны в каждой задаче; мок на уровне
  `googleapiclient`-сервиса, без сети (как принято в `tests/`)
- **e2e tests**: в проекте нет — не применимо
- Полный прогон: `pytest`; покрытие:
  `pytest --cov=airflow_provider_gmail --cov-report=term-missing` (проект
  держит ~99%)
- Packaging-тесты (`-m packaging`) в этом плане **не трогаем** — упаковка не
  меняется

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

Ключевые решения (приняты пользователем):

- **Фильтр inline-частей**: отбрасывать только `inline` + `image/*`; ветку
  «есть Content-ID» убрать. Потеря настоящего PDF/XLSX (Apple Mail + шлюзы,
  проставляющие Content-ID на все части) хуже редкой лишней картинки, которую
  и так отсекает `attachment_pattern`. CONTEXT.md привести к обещанию
  «Inline не-картинки (PDF, xlsx) — остаются вложениями».
- **`.eml`-вложения**: часть, подходящая под определение вложения из
  CONTEXT.md (непустой `filename` + источник тела), отдаётся как вложение и
  внутрь **не** рекурсируется — вложения внутреннего письма перестают
  «протекать» под чужим `message_id`.
- **Dedup меток**: уйти от `-label:"..."` в строке запроса (эскейпинг и форма
  метки со слешем у Gmail не документированы; отказ — тихий). Фильтровать в
  коде по `labelIds` письма: ID метки детерминирован (мы сами её
  создаём/находим), сравнение ID с ID тестируется офлайн. Поиск ID перед
  поиском писем — **lookup-only** (без создания метки), чтобы не требовать
  `gmail.modify` на этапе поиска и не плодить метки на пустых прогонах.
- **S3-сенсор**: квитанция (`_manifest.json`) с **текущим** `run_id` — «работа
  осталась» (зеркало `Decision.DELIVER_ONLY` из ADR-0001), сенсор пропускает
  оператор доделать доставку. «Обработано» = квитанция **чужого** рана.
- **`soft_fail`**: дефолт остаётся громким (`False`, таймаут = ошибка +
  алерт); docstring переписывается честно («передайте `soft_fail=True` для
  skipped/зелёного DAG»).

## Technical Details

- `_is_inline_image(mime_type, headers)` → после правки: `inline` и
  `mime_type.startswith("image/")`; переименовывать не нужно, имя снова
  соответствует поведению.
- `_walk(part)`: сначала `_as_attachment(part)`; при не-`None` — yield и
  `return` (не рекурсировать). Обычные контейнеры (`multipart/*`) не имеют
  `filename` → `_as_attachment` вернёт `None` → рекурсия как раньше.
- `_cap_filename_length` / `_first_free_name`: единый способ разбиения имени —
  `os.path.splitext` (корректен для имён без точки и dotfiles).
- Контракт «пустой `attachmentId`»: в `_as_attachment` источник тела — это
  **непустой** `attachmentId` или наличие ключа `data`
  (`bool(body.get("attachmentId")) or ("data" in body)`); в
  `download_attachment` при falsy `attachment_id` и `data is None` — исключение
  (`AirflowException`), а не `b""`.
- `_field_value`: квотировать, если значение НЕ матчится «безопасному»
  паттерну `^[\w.@+-]+$` (Unicode-`\w` сохраняет текущий контракт «кириллица
  одним словом — без кавычек»); `_escape` внутри кавычек остаётся. Известное
  ограничение: экранирование `"`/`\` внутри кавычек у Gmail не документировано
  — для structured-полей это принимаем (документировать в docstring), для
  критичного dedup меток уходим от запроса совсем (см. ниже).
- Новый `GmailHook.find_label_id(name) -> str | None`: `labels.list`, матч по
  полному имени, кэш в `self._label_ids`, `None` при отсутствии; **не**
  создаёт метку (в отличие от `get_or_create_label`).
- `find_messages_with_attachments(query, pattern, exclude_label_id=None)`:
  после `get_message` пропускать письмо, если
  `exclude_label_id in (message.get("labelIds") or [])`, с INFO-логом.
- `build_query(...)`: параметры `filter_processed_label` / `label_name` и терм
  `-label:` удаляются; сигнатура сужается, docstring и упоминания в ADR-0001 /
  CONTEXT.md обновляются.
- Метод-политика `_filter_processed_label()` (база/S3/local, сенсоры)
  сохраняется, но теперь отвечает «фильтровать ли по ID метки processed»
  (S3 → `False`, local/базовый сенсор → `self.mark_processed`); в
  `execute()`/`_find_messages` при `True`:
  `exclude_label_id = hook.find_label_id(resolve_label_name(label_suffix))`.
- `_has_processed_manifest(msg, run_id)`: парсить читаемый манифест и
  возвращать `manifest.run_id != run_id`; `run_id` прокидывается из
  `poke(context)`.
- Раскладка: `rel_dir` в `execute()` собирается через `utils/paths`
  (`message_dir("", dt.isoformat(), message_id)` — `join_key` отбрасывает
  пустые сегменты), литералы `"_manifest.json"` → `MANIFEST_FILENAME`.

## What Goes Where

- **Implementation Steps** (`[ ]`): правки кода, тестов и документации в этом
  репозитории.
- **Post-Completion** (без чекбоксов): ручная проверка на живом Gmail-ящике,
  отложенные работы.

## Отложено (за скоупом, осознанно)

Не включать в этот план; кандидаты на отдельный план:

- рефакторинг дублирования сенсор/оператор (~80 строк поискового контракта,
  lazy `S3Hook`, чтение манифеста);
- efficiency: ленивые импорты `googleapiclient`/`google-auth` (стоимость
  парсинга DAG), metadata-first fetch до `decide()`, батчинг `messages.get`,
  один GET вместо `check_for_key`+`read_key`;
- runtime-warning про `max_active_runs != 1` в S3-операторе;
- гвард `_range_warning_logged`, не переживающий `mode="reschedule"`.

## Implementation Steps

### Task 1: Смягчить фильтр inline-частей (убрать ветку Content-ID)

**Files:**
- Modify: `src/airflow_provider_gmail/utils/mime.py`
- Modify: `CONTEXT.md`
- Modify: `README.md`
- Modify: `README_RU.md`
- Modify: `tests/test_mime.py`

- [x] в `_is_inline_image` (mime.py:180-191) убрать финальный
  `return _find_header(headers, "Content-ID") is not None` — вернуть `False`;
  часть отбрасывается только при `inline` **и** `image/*`
- [x] обновить docstring `iter_attachments` (mime.py:138-140): правило теперь
  «inline и image/*», без Content-ID
- [x] в CONTEXT.md убрать из правила исключения условие «ИЛИ есть Content-ID»,
  оставить обещание «Inline не-картинки (PDF, xlsx) — остаются вложениями»
- [x] обновить описание правила в README.md:247-251 («drops a part only if it
  is inline *and* (`image/` *or* it has a `Content-ID`)») и параллельный
  фрагмент README_RU.md — старое правило описано и там
- [x] write tests: inline-PDF **с** Content-ID остаётся вложением (новый кейс
  рядом с `test_inline_pdf_remains_attachment`); inline `image/*` с Content-ID
  и без — по-прежнему отбрасывается
- [x] проверить/обновить фикстуру `tests/fixtures/gmail/inline_image.json`,
  если она полагалась на Content-ID-ветку (image/png + inline → по-прежнему
  отбрасывается по правилу image/*; изменений не потребовалось)
- [x] run tests — must pass before task 2

### Task 2: Вложенные письма (.eml, message/rfc822) — вложение, а не контейнер

**Files:**
- Modify: `src/airflow_provider_gmail/utils/mime.py`
- Create: `tests/fixtures/gmail/forwarded_eml.json`
- Modify: `tests/test_mime.py`

- [x] переставить `_walk` (mime.py:145-154): сначала `_as_attachment(part)`;
  если часть — вложение (непустой `filename` + источник тела, не inline-
  картинка) — yield и `return`, в `parts` не спускаться; иначе рекурсия как
  раньше (контейнеры `multipart/*` без `filename` не задеваются)
- [x] обновить docstrings `iter_attachments`/`_walk`: часть с `filename` и
  источником тела — вложение, даже если несёт вложенное дерево
  (message/rfc822); вложения внутреннего письма не поднимаются; явно оговорить
  границу гарантии: rfc822-часть с `filename`, но БЕЗ источника тела (только
  `parts`, без `attachmentId`/`data`) вложением не считается — по ней
  по-прежнему идёт рекурсия
- [x] создать фикстуру `forwarded_eml.json`: message/rfc822-часть с
  `filename="forwarded.eml"`, `body.attachmentId` и вложенным `parts[]`,
  внутри которого своё вложение (например `inner.xlsx`)
- [x] write tests: `.eml` выдаётся как вложение; `inner.xlsx` внутреннего
  письма НЕ выдаётся; обычные multipart-фикстуры (`nested_mime.json`) не
  изменили поведение
- [x] run tests — must pass before task 3

### Task 3: Разбиение имени файла через os.path.splitext (пустой stem, dotfiles)

**Files:**
- Modify: `src/airflow_provider_gmail/utils/mime.py`
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `tests/test_mime.py`
- Modify: `tests/test_operator_base.py`

- [x] в `_cap_filename_length` (mime.py:77-83) заменить
  `name.rpartition(".")` на `os.path.splitext(name)` (импортировать `os.path`);
  для имени без точки stem == имя целиком, обрезка больше не даёт `""`
- [x] в `_first_free_name` (operators/gmail.py:97-110) заменить ручной
  `rpartition` на `os.path.splitext` — `.bashrc` при коллизии становится
  `.bashrc_1`, а не `_1.bashrc`
- [x] write tests (mime): `sanitize_filename("а"*130, "attachment_1")`
  возвращает непустое имя ≤247 байт UTF-8 (регресс воспроизведённого бага);
  длинное имя с расширением сохраняет расширение; dotfile проходит без
  искажений
- [x] write tests (operators): коллизия dotfile → `.bashrc_1`; существующие
  тесты `resolve_collisions` проходят без изменений контракта
- [x] run tests — must pass before task 4

### Task 4: Контракт пустого attachmentId (mime ↔ hook)

**Files:**
- Modify: `src/airflow_provider_gmail/utils/mime.py`
- Modify: `src/airflow_provider_gmail/hooks/gmail.py`
- Modify: `tests/test_mime.py`
- Modify: `tests/test_hook_search.py`

- [x] в `_as_attachment` (mime.py:163) источник тела считать по
  `bool(body.get("attachmentId")) or ("data" in body)` — пустой `attachmentId`
  без `data` не делает часть вложением; docstring `iter_attachments` уточнить
  (пустая строка `data == ""` по-прежнему валидный источник — ключ есть)
- [x] в `download_attachment` (hooks/gmail.py:363-365) заменить
  `return _b64url_decode(attachment.data or "")` на: при `data is None` —
  `AirflowException` с внятным текстом (вместо тихого `b""`); поправить
  комментарий «guaranteed present by iter_attachments»
- [x] write tests (mime): часть `{"attachmentId": ""}` без `data` не выдаётся;
  часть с `attachmentId=""` и `data` — выдаётся (тело из `data`); легитимный
  пустой файл (`data == ""`, фикстура `empty_file.json`) — без изменений
- [x] write tests (hook): `download_attachment` с falsy `attachment_id` и
  `data=None` бросает исключение; с `data=""` возвращает `b""` (покрыто
  существующим `test_download_attachment_empty_file_data`)
- [x] run tests — must pass before task 5

### Task 5: Квотирование метасимволов в значениях запроса

**Files:**
- Modify: `src/airflow_provider_gmail/hooks/gmail.py`
- Modify: `tests/test_hook_query.py`

- [x] в `_field_value` (hooks/gmail.py:59-68) сменить условие: значение
  квотируется, если НЕ матчится `^[\w.@+-]+$` (Unicode); `_escape` внутри
  кавычек сохранить; docstring — новое правило + известное ограничение про
  недокументированность эскейпинга у Gmail
- [x] write tests: `subject_contains="re:invoice"` →
  `subject:"re:invoice"`; `"{urgent}"` → в кавычках; `"(a)"` → в кавычках;
  `user@example.com` — без кавычек; кириллица одним словом — без кавычек
  (`test_cyrillic_single_word_not_quoted` остаётся зелёным)
- [x] обновить существующие тесты `test_hook_query.py`, фиксирующие старый
  контракт «квотирование только при пробелах/кавычках/бэкслеше» (существующие
  ассерты остались валидны — метасимвольных значений без кавычек не было)
- [x] run tests — must pass before task 6

### Task 6: Hook — dedup меток по labelIds вместо -label в запросе

**Files:**
- Modify: `src/airflow_provider_gmail/hooks/gmail.py`
- Modify: `tests/test_hook_labels.py`
- Modify: `tests/test_hook_query.py`
- Modify: `tests/test_hook_search.py`

- [ ] добавить `GmailHook.find_label_id(name) -> str | None`: `labels.list`,
  матч по полному имени, кэш в `self._label_ids`, `None` при отсутствии;
  **без** создания метки (не требует `gmail.modify`); `get_or_create_label`
  может переиспользовать lookup-часть; ⚠️ негативный результат НЕ кэшировать
  в `self._label_ids` — иначе последующий `get_or_create_label` того же имени
  пропустит создание
- [ ] в `find_messages_with_attachments` добавить параметр
  `exclude_label_id: str | None = None`: после `get_message` пропускать письмо
  при `exclude_label_id in (message.get("labelIds") or [])` с INFO-логом
  («processed label — skipped»)
- [ ] из `build_query` удалить параметры `filter_processed_label`,
  `label_name` и терм `-label:` (hooks/gmail.py:220-284); обновить docstring;
  docstring `resolve_label_name` (38-51) — имя больше не идёт в запрос, только
  в `get_or_create_label`/`find_label_id`
- [ ] write tests (labels): `find_label_id` — найдена/не найдена/кэш (второй
  вызов без `labels.list`)
- [ ] write tests (search): письмо с `exclude_label_id` в `labelIds`
  пропускается; без — остаётся; `exclude_label_id=None` — фильтра нет
- [ ] переписать тесты `test_hook_query.py` про `-label:` под новую сигнатуру
  `build_query` (терма в запросе больше нет ни при каких аргументах)
- [ ] run tests — must pass before task 7 (⚠️ тесты операторов/сенсоров
  ЗАВЕДОМО упадут до Task 7 — `FakeGmailHook.build_query` в
  `tests/test_sensor.py:47-67` и `tests/test_sensor_s3.py:59-74` зеркалит
  старую сигнатуру и даст `TypeError`; выполнить Task 7 немедленно следом,
  коммит один на обе задачи)

### Task 7: Операторы и сенсоры — проводка exclude_label_id

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `docs/adr/0001-delivery-contract.md` (упоминания -label)
- Modify: `docs/adr/0004-explicit-date-range-backfill.md` (упоминание -label,
  строка ~25)
- Modify: `CONTEXT.md` (если описывает -label-механизм)
- Modify: `README.md`, `README_RU.md` (разделы про -label:
  README.md:208-218, 252-254; README_RU.md:202-243)
- Modify: `tests/test_operator_base.py`, `tests/test_operator_local.py`,
  `tests/test_operator_s3.py`, `tests/test_sensor.py`,
  `tests/test_sensor_s3.py`

- [ ] `execute()` (operators/gmail.py:297-332): вычислить
  `exclude_label_id = self.hook.find_label_id(label_name) if self._filter_processed_label() else None`,
  убрать label-аргументы из вызова `build_query`, передать `exclude_label_id`
  в `find_messages_with_attachments`
- [ ] `_find_messages` сенсора (sensors/gmail.py:166-211): та же проводка
- [ ] обновить docstrings политики `_filter_processed_label` (база, S3, local,
  оба сенсора) и docstring класса local-оператора (пункт 2 «Limitations»,
  operators/gmail.py:541-551): «`-label:` в запросе» → «фильтр по ID метки»
- [ ] обновить упоминания `-label:` в ADR-0001, ADR-0004, CONTEXT.md,
  README.md и README_RU.md (семантика та же — S3 никогда не фильтрует по
  метке, local — opt-in; меняется механизм)
- [ ] обновить фейки под новую сигнатуру: `FakeGmailHook.build_query` в
  `tests/test_sensor.py:47-67` и `tests/test_sensor_s3.py:59-74` (делегируют в
  реальный `build_query` со старыми аргументами)
- [ ] переписать тесты старого механизма: `test_query_parity...`
  (`tests/test_sensor.py:223-257`, ассертит `-label:"..." in built_query`) и
  `test_poke_does_not_filter_search_by_label`
  (`tests/test_sensor_s3.py:304`, ассертит `"-label:" not in built_query`) —
  паритет/политика теперь проверяются через `exclude_label_id`
- [ ] write tests: local-оператор с `mark_processed=True` фильтрует письмо с
  меткой (мок `labels.list` + `labelIds` в письме) и не фильтрует при
  отсутствии метки в ящике (`find_label_id → None`); S3-оператор и
  S3-сенсор не вызывают `find_label_id` (политика `False`); базовый сенсор с
  `mark_processed=True` — фильтрует
- [ ] run tests — must pass before task 8

### Task 8: S3-сенсор — учитывать run_id квитанции (ADR-0001)

**Files:**
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `tests/test_sensor_s3.py`

- [ ] `_has_processed_manifest` (sensors/gmail.py:298-312): принять `run_id`,
  парсить читаемый манифест в переменную и возвращать
  `manifest.run_id != run_id` (квитанция ТЕКУЩЕГО рана = работа осталась,
  `False`); `poke` передаёт `context["run_id"]`
- [ ] обновить docstrings `_has_processed_manifest`, `poke` и класса
  (sensors/gmail.py:218-259): «обработано = квитанция чужого рана», описать
  сценарий clear-всего-рана; поправить неточную фразу про «operator behind it
  honestly skips» (для текущего run_id оператор делает DELIVER_ONLY)
- [ ] переписать `test_poke_manifest_of_any_run_counts_as_processed`
  (tests/test_sensor_s3.py:231-237): манифест ЧУЖОГО рана → processed
  (`poke=False`), манифест ТЕКУЩЕГО рана → работа есть (`poke=True`)
- [ ] обновить существующие тесты, которые новый контракт переворачивает:
  хелпер `_seed_manifest` по умолчанию сеет манифест с ТЕКУЩИМ `run_id`
  (`RUN`) — `test_poke_false_when_message_present_and_valid_manifest`
  (tests/test_sensor_s3.py:199) и `test_poke_false_when_all_messages_have_manifests`
  (:221) начнут падать; для «processed»-кейсов сеять манифест ЧУЖОГО рана
  (или поменять ассерты); в `test_poke_true_when_two_messages_one_has_manifest`
  (:212) поправить премиссу «msg1 processed»
- [ ] write tests: сценарий восстановления — манифест текущего `run_id` у
  единственного письма → `poke() is True`; корректный `ManifestError`-контракт
  не задет (битый манифест по-прежнему валит poke)
- [ ] run tests — must pass before task 9

### Task 9: Честная документация soft_fail

**Files:**
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `README.md`
- Modify: `README_RU.md`
- Modify: `tests/test_sensor.py`

- [ ] переписать предложение docstring (sensors/gmail.py:63-64): дефолт
  Airflow — `soft_fail=False`, таймаут = ошибка + алерт; «передайте
  `soft_fail=True`, чтобы таймаут стал `skipped` (зелёный DAG)»; дефолт в коде
  НЕ менять (решение пользователя)
- [ ] проверить README.md / README_RU.md (разделы про сенсоры) на ту же
  формулировку «inherited» и поправить
- [ ] write tests: тест-фиксация контракта — у инстанса сенсора без явного
  аргумента `soft_fail is False` (защита от случайной смены дефолта)
- [ ] run tests — must pass before task 10

### Task 10: Предупреждение о backfill над общим префиксом

**Files:**
- Modify: `example_dags/example_gmail_s3_backfill.py`
- Modify: `README.md`
- Modify: `README_RU.md`
- Modify: `tests/test_example_dags.py` (если фиксирует docstring/комментарии)

- [ ] в module docstring backfill-примера добавить явное предупреждение:
  `max_active_runs=1` — per-DAG и НЕ сериализует backfill с дневным DAG над
  тем же префиксом; перед backfill **приостановите (pause) дневной DAG**,
  иначе гонка check-then-act: двойная доставка, а overwrite-backfill может
  перезаписать манифест упавшей дневной попытки чужим `run_id` (потеря
  доставки на retry)
- [ ] то же предупреждение в README.md / README_RU.md (раздел про
  backfill/overwrite)
- [ ] write/update tests: `tests/test_example_dags.py` продолжает проходить
  (импорт DAG-ов не сломан)
- [ ] run tests — must pass before task 11

### Task 11: Cleanup — MANIFEST_FILENAME и единый владелец раскладки

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `src/airflow_provider_gmail/utils/paths.py`
- Modify: `tests/test_operator_base.py`, `tests/test_paths.py`

- [ ] operators/gmail.py:277 и :335 — литерал `"_manifest.json"` заменить на
  `MANIFEST_FILENAME` (импорт уже есть, строка 50)
- [ ] operators/gmail.py:334 — `rel_dir = f"dt={dt}/{msg.message_id}"`
  собрать через `utils/paths` (`message_dir("", dt.isoformat(), msg.message_id)`
  либо новый prefix-less helper рядом с `message_dir`) — один владелец схемы
  `dt=`; сенсор уже ходит через `paths.manifest_key`
- [ ] поправить docstring `utils/paths.py` («Pure S3 object-key construction»
  → модуль используется и локальным оператором для относительной раскладки)
- [ ] write tests: раскладка оператора и ключ сенсора совпадают для одного
  `(dt, message_id)` (связывающий тест «operator writes where sensor looks»)
- [ ] run tests — must pass before task 12

### Task 12: Verify acceptance criteria

- [ ] пройтись по Overview/Solution Overview: все 12 согласованных пунктов
  реализованы, решения пользователя не искажены
- [ ] краевые случаи: пустые имена, dotfiles, `data == ""`,
  `attachmentId == ""`, rfc822 без внутренних вложений, письмо без `labelIds`
- [ ] run full test suite: `pytest`
- [ ] coverage: `pytest --cov=airflow_provider_gmail --cov-report=term-missing`
  — не ниже уровня до правок (~99%)
- [ ] `grep -rn -- '-label:' src/ docs/adr/ CONTEXT.md README* example_dags/`
  — упоминаний старого механизма не осталось (исторический план
  `docs/plans/completed/` и сгенерированный `*.egg-info` из проверки
  исключены намеренно)

### Task 13: [Final] Update documentation

- [ ] CHANGELOG.md: раздел с фиксами (English), включая поведенческие
  изменения (inline-фильтр, .eml, labelIds-dedup, sensor run_id)
- [ ] AGENTS.md: обновить «Domain traps», если формулировки про label-dedup
  устарели
- [ ] переместить этот план в `docs/plans/completed/`

## Post-Completion

**Manual verification** (живой Gmail-ящик, вне CI):
- прогнать local-оператор с `mark_processed=True` дважды: второй прогон не
  доставляет повторно (labelIds-фильтр реально работает против живого API);
- запрос с квотированным значением (`subject:"re:invoice"`) возвращает
  ожидаемые письма;
- письмо от Apple Mail с inline-PDF доставляется.

**Отложенные работы** (отдельный план по желанию):
- рефакторинг дублирования сенсор/оператор; efficiency-пакет (ленивые
  импорты, metadata-first fetch, батчинг, один GET на манифест);
  runtime-warning `max_active_runs`; гвард WARNING в reschedule-режиме.
