# Templated `lookback_days` / `overwrite`

## Overview

`lookback_days` сегодня — обычный аргумент `__init__`, который валидируется
(`validate_lookback_days`) в момент парсинга DAG. `overwrite` — тоже обычный
аргумент `__init__`, но сейчас он **вообще никак не валидируется** — принимается
любое python-значение как есть. Ни то, ни другое не входит в `template_fields`,
поэтому параметры Airflow (`dag_run.conf`/Jinja) до них в принципе не долетают —
DAG-автору, которому нужно разово расширить окно или разово прогнать backfill,
приходится редактировать и передеплоивать файл DAG. `date_from`/`date_to` уже
решают ровно этот класс проблемы (ADR-0004): они templated и парсятся *после*
рендеринга, в `execute()`/`poke()`, а не в `__init__`.

Этот план переводит `lookback_days` (оператор + сенсор) и `overwrite` (только
оператор) на ту же схему: templated-поля, каст/валидация в рантайме, со
**строгим** контрактом фолбэка — рендер в пустую строку / `None` / буквальную
строку `"None"` (классическая ловушка `{{ dag_run.conf.get('x') }}`, когда ключа
нет) даёт `ValueError`, а не тихую подстановку дефолта. Ответственность за
дефолт внутри самого Jinja-выражения (`.get('lookback_days', 7)`) лежит на
DAG-авторе. Для `overwrite` этот план дополнительно **вводит валидацию, которой
сегодня нет вообще** — это осознанное ужесточение поведения, а не побочный
эффект шаблонизации, и это должно быть явно отражено в ADR/CHANGELOG (Task 7/Task 11).

Явно вне скоупа (фиксируется как задел в новом ADR, не реализуется здесь):
`timezone`, `has_attachment`, `mark_processed`, `label_suffix`.
`attachment_pattern` остаётся сознательно не templated по ADR-0005 — этот план
то решение не пересматривает.

## Context (from discovery)

- `src/airflow_provider_gmail/dates.py` — чистые хелперы, без импортов Airflow.
  `validate_lookback_days(lookback_days: int) -> None` живёт здесь сегодня,
  вызывается из `__init__` и оператора, и сенсора. `parse_date_range` — уже
  существующий образец-сосед, которому нужно следовать (парсинг/валидация
  *после* рендера, в `execute()`/`poke()`). Заключительная фраза докстринга
  модуля — «...the sensor→operator coupling that would otherwise exist purely
  to share **two** pure functions» — устареет, как только в модуле окажется
  четыре публичных хелпера вместо двух; её нужно переформулировать, а не
  просто дописать рядом.
- `src/airflow_provider_gmail/operators/gmail.py` —
  `GmailAttachmentsBaseOperator` (`template_fields`, `__init__`, `_run()`),
  `GmailAttachmentsToS3Operator` (свой параметр `overwrite`, прокидывает в
  базовый класс), `GmailAttachmentsToLocalOperator` (`overwrite` **не входит в
  его собственную сигнатуру**, дефолт `lookback_days=0`). В обоих `__init__`
  (базовый + сенсор) есть инлайн-комментарий — `operators/gmail.py:185-186` —
  «Reject negative lookback and an unknown timezone eagerly so bad config
  fails at DAG parse, not deep inside a run» — который становится наполовину
  ложным после переноса валидации `lookback_days` в рантайм, и его нужно
  исправить, а не оставить как есть.
  - **Проверено (не предположение):** хотя `GmailAttachmentsToLocalOperator.__init__`
    не объявляет `overwrite` как явный параметр, он прокидывает `**kwargs` в
    `super().__init__(lookback_days=lookback_days, **kwargs)`, а базовый
    `__init__` `overwrite` **принимает**. Подтверждено прямой инстанциацией:
    `GmailAttachmentsToLocalOperator(task_id="t", source="x", path="/tmp",
    overwrite=True).overwrite` равно `True`. То есть фраза докстринга класса
    «There is no user-facing `overwrite` argument» уже сегодня верна только
    для *объявленной сигнатуры*, а не для фактического поведения — это
    существующий пробел, не привнесённый этим планом. После того как
    `overwrite` станет templated на базовом классе, DAG-автор, обнаруживший
    этот недокументированный проброс через kwargs, сможет передать туда и
    Jinja-строку для локального оператора. Этот план **не** закрывает этот
    пробел (вне скоупа — это не часть шаблонизации), но Task 4 обязан описать
    это точно вместо утверждения, что `overwrite` «недоступен»/«всегда
    `False`» для этого класса, и отметить единственное функциональное
    следствие: `_run()` всё равно безусловно вызывает
    `resolve_overwrite(self.overwrite)`, поэтому невалидное отрендеренное
    значение, переданное таким путём, всё равно вызовет исключение, хотя
    `_read_manifest()` для этого класса всегда возвращает `None` независимо от
    значения `overwrite` (т.е. каст может упасть, даже когда его результат
    никогда бы не повлиял на поведение — тот же класс компромисса, что уже
    задокументирован для `lookback_days` в режиме явного диапазона).
- `src/airflow_provider_gmail/sensors/gmail.py` — `GmailAttachmentSensor`
  (`template_fields`, `__init__`, `_find_messages()` — вызывается на каждый
  `poke()`), `GmailAttachmentToS3Sensor` (про `overwrite` вообще ничего не
  знает, не переопределяет `_find_messages()`, его `template_fields` строится
  распаковкой родительского кортежа). Параллельный инлайн-комментарий — на
  `sensors/gmail.py:124-125`.
- `src/airflow_provider_gmail/window.py` — сигнатура `Window.resolve(...,
  lookback_days: int, ...)` не меняется; вызывающий код обязан передавать уже
  скастованный `int`. `Window.resolve` в режиме явного диапазона
  (`date_from`/`date_to` заданы) `lookback_days` полностью игнорирует — но
  каст/валидация в `_run()`/`_find_messages()` происходит *безусловно*, ещё до
  того, как известна эта ветка, так что шаблонизированный `lookback_days`,
  отрендерившийся в мусор, всё равно упадёт, даже если значение было бы
  отброшено. Это осознанное следствие каста один раз в начале метода
  (простота важнее точности), и это нужно зафиксировать в ADR-0009, а не
  тихо принять как сюрприз.
- **Существующие тесты — проверено по актуальному содержимому файлов, не
  предположение:**
  - `tests/test_dates.py` — сейчас тестирует `from_local_iso` (докстринг файла
    так и говорит); `to_local_iso` встречается в нём только как вспомогательный
    построитель round-trip фикстур, а не как отдельный предмет проверки. Это
    правильный дом для юнит-тестов `resolve_lookback_days`/`resolve_overwrite`.
    `grep -rn validate_lookback_days tests/` **не находит прямых тестов** этой
    функции сегодня — она проверяется только косвенно через
    `tests/test_operator_base.py:236` (`test_init_negative_lookback_days_raises`)
    и `tests/test_sensor.py:168` (`test_negative_lookback_days_fails_at_init`).
    Мигрировать из `tests/test_window.py` нечего (этот файл тестирует только
    `Window.resolve`) — Task 1 пишет тесты с нуля.
  - `tests/test_operator_base.py` — `test_template_fields_expected_set`
    (использует subset-проверку `<=`, строка ~211),
    `test_init_negative_lookback_days_raises` (строка 236), два WARNING-теста
    `test_execute_nondefault_lookback_with_range_warns` (строка 580) и
    `test_execute_default_lookback_with_range_does_not_warn` (строка 591 —
    вообще **не** передаёт `lookback_days`; проверяет дефолт класса, и её
    НЕЛЬЗЯ переделывать на передачу строки).
  - `tests/test_sensor.py` — `test_template_fields_match_base_operator`
    (строка 130, использует **строгое равенство** `tf == {...}`, а не subset —
    её ожидаемый набор придётся обновить, и её *посылка* меняется: после этого
    плана `template_fields` базового оператора получает и `lookback_days`, и
    `overwrite`, а сенсор — только `lookback_days`, так что «совпадает с
    базовым оператором» больше не буквально верно, и имя/комментарий теста
    должны это отражать). `test_sensor_has_no_overwrite_parameter` (строка
    183) — существующий guard для этой асимметрии, ему стоит добавить явный
    assert `"overwrite" not in GmailAttachmentSensor.template_fields`.
    `test_negative_lookback_days_fails_at_init` (строка 168).
    `test_range_override_warning_logged_once_across_pokes` (строка 194) — это
    **единственный** WARNING-тест в этом файле. `test_query_parity_with_local_operator`
    (строка 233) и `test_query_parity_with_explicit_range` (строка 280) — это
    **не** WARNING-тесты — они передают `lookback_days: 3` в общий словарь
    параметров, чтобы доказать, что сенсор и оператор ищут одно и то же окно;
    это отдельная, ценная цель (см. Task 5a) для проверки, что сам каст
    остаётся идентичным между сенсором и оператором, когда `lookback_days`
    приходит строкой.
  - `tests/test_operator_local.py` — `test_default_lookback_days_is_zero`
    (строка 146) и `test_overwrite_attribute_false_but_not_in_public_signature`
    (строка 155) проверяют *поведение с дефолтными аргументами*, а не
    аннотацию — правка только аннотации (`int` → `int | str`) не требует
    правки ни того, ни другого (оба продолжают проходить без изменений:
    `_make_op()` в этом файле никогда не передаёт `overwrite=`, так что
    `op.overwrite is False` остаётся верным).
  - `tests/test_operator_s3.py` — поведенческие тесты `overwrite=True/False`
    уже существуют (например
    `test_overwrite_true_forces_download_despite_current_manifest`, строка
    405); ни один сейчас не рендерит `overwrite` из шаблонной строки.
  - `tests/test_sensor_s3.py` — правильный дом для любого теста, специфичного
    для `GmailAttachmentToS3Sensor` (23 существующие тестовые функции, свои
    хелперы `_make_sensor`/`_poke`). **Не** `tests/test_sensor.py`.
  - `README.md` / `README_RU.md` документируют `lookback_days` (README.md:138)
    и `overwrite` (README.md:151) как аргументы конструктора с плоским типом в
    таблице параметров базового оператора, и templated-параметры в этой же
    таблице уже несут явную фразу «Templated.» (`query`, `date_from`/`date_to`,
    `prefix`, `path`) — `lookback_days`/`overwrite` сейчас нет. Раздел про
    сенсоры (README.md:162-170) — это **проза**, а не отдельная таблица
    параметров со своими строками — там сказано, что сенсоры «mirror the
    operator's filter parameters exactly», так что обновления строки
    `lookback_days` в таблице оператора достаточно; отдельной строки
    `lookback_days` для сенсора искать и править не нужно.
  - `grep -rln render_template tests/` **не находит совпадений** — в
    репозитории нигде нет существующего хелпера для рендеринга
    `template_fields` через настоящий проход Jinja. `tests/test_example_dags.py`
    только строит `DagBag` и проверяет структурные свойства
    (`dag.max_active_runs`, `isinstance`, `upstream_task_ids`); шаблоны он не
    рендерит. `tests/conftest.py` пока не существует. Task 2 создаёт его и
    добавляет туда общий хелпер рендеринга (см. Technical Details) — это не
    требует конструирования `DAG`/`DagBag`: `AbstractOperator.get_template_env()`
    откатывается на голый `SandboxedEnvironment(cache_size=0)`, когда у
    оператора нет привязанного `dag`, что подтверждено напрямую по
    установленному в локальном venv Airflow 2.9.3 в
    `.venv/lib/python3.10/site-packages/airflow/template/templater.py:64-73`
    (CI по `.github/workflows/tests.yml` использует 2.9.1 — патч-версии,
    поведение рендеринга в этой части между ними не отличается), и
    подтверждено фактическим вызовом `render_template_fields()` на «голом»
    экземпляре оператора с `context={"dag_run": SimpleNamespace(conf={...})}`.
    Рендер **всегда даёт строку** (например, значение conf `5` рендерится как
    `"5"`, а отсутствующий ключ без Jinja-дефолта рендерится в буквальную
    строку `"None"`) *если только* в DAG не включён
    `render_template_as_native_obj=True` — тогда нативное окружение Jinja
    возвращает настоящий python-тип (`int`, `bool`, `None`, ...) вместо строки.
    Отдельный нюанс механизма (`AbstractOperator._do_render_template_fields`):
    поле пропускается целиком, если его **дорендерное** значение falsy
    (`if not value: continue`) — поэтому литеральные `overwrite=False` и
    `lookback_days=0` у локального оператора никогда не попадают в Jinja
    (безвредно, они и не должны рендериться), а вот Jinja-строка, отрендерившаяся
    в `""`, **уже присвоена** к моменту этой проверки — она не «пропускается»,
    так что заявленное в Overview «рендер в пустую строку → `ValueError`»
    по-прежнему верно и не требует отдельной защиты от этого механизма.
    Оба резолвера должны обрабатывать оба случая — см. примечание про
    нативные значения у `resolve_overwrite` ниже.
- Дизайн зафиксирован в предыдущей брейнсторм-сессии (здесь не пересматривается):
  `resolve_lookback_days`/`resolve_overwrite` живут в `dates.py`; каст
  происходит в локальных переменных внутри `_run()`/`_find_messages()`, `self`
  никогда не мутируется; фолбэк строгий (ответственность DAG-автора, без
  тихого дефолта). Название `resolve_` (а не `parse_`, как у глагола в
  `parse_date_range`) выбрано осознанно: в отличие от `parse_date_range`,
  которая лишь парсит ISO-текст в `date`, эти две функции дополнительно
  применяют *политику* (отклонить отрицательное / отклонить неоднозначную
  истинность) — ближе к тому, как `Window.resolve` и `resolve_label_name` уже
  используют «resolve» в смысле «превратить сырой вход в единственное
  конкретное значение, которым дальше пользуется код». Это обоснование
  фиксируется в ADR-0009, а не решается заново здесь.
- **Ограничение по порядку (определяет порядок Task 1/2/5a/5b ниже):**
  `validate_lookback_days` нельзя просто удалить в Task 1 — и
  `operators/gmail.py:44`, и `sensors/gmail.py:30` всё ещё её `import`, пока не
  выполнены Task 2 и Task 5a. Удаление в Task 1 при живых импортах ломает
  **сбор** тестов для всего набора (`ImportError` на этапе импорта модуля), а
  не только часть проверок. Поэтому `validate_lookback_days` остаётся в
  `dates.py` рядом с двумя новыми функциями, пока Task 2 и Task 5a не уберут
  оба импорта-потребителя; саму функцию убирает отдельная задача Task 5b —
  небольшая и чисто про `dates.py`, не смешанная с миграцией сенсора.

## Development Approach

- **testing approach**: Regular (сначала код, потом тесты, потом прогон) —
  соответствует соглашению `docs/plans/completed/20260710-airflow-provider-gmail.md`.
- завершать каждую задачу полностью, прежде чем переходить к следующей
- делать небольшие, сфокусированные изменения
- **КРИТИЧНО: каждая задача ОБЯЗАНА включать новые/обновлённые тесты** для
  изменений кода в этой задаче — включая *обновление* перечисленных выше
  существующих тестов, которые проверяют валидацию в момент `__init__`,
  поскольку это поведение переносится в рантайм
- **КРИТИЧНО: все тесты проходят, прежде чем начинать следующую задачу** — без исключений
- **КРИТИЧНО: обновлять этот файл плана, если по ходу меняется объём работ**
- прогонять тесты после каждого изменения
- сохранять обратную совместимость для **валидных** значений: DAG, передающий
  обычный `int`/`bool` (без Jinja), сегодня должен продолжать работать без
  изменений. DAG, передающий **невалидный** литерал (например,
  `lookback_days=-1`), меняет поведение осознанно: сегодня это падает в момент
  парсинга DAG (`__init__`); после этого плана падает в рантайме
  (`execute()`/первый `poke()`), потому что теперь один и тот же код
  обслуживает и литерал, и шаблон. `overwrite` аналогично переходит от
  **полного отсутствия валидации** сегодня к строгой валидации в рантайме — в
  том числе для `overwrite=None`: сегодня это принимается молча и работает как
  `False` (`None if self.overwrite else ...` — `None` тоже falsy), а после
  этого плана `resolve_overwrite(None)` кидает `ValueError`. Это реальный,
  пусть и краевой, случай обратной несовместимости: DAG, который сегодня
  пишет `overwrite=some_dict.get("overwrite")` без Jinja вообще (обычный
  python `dict.get`, возвращающий `None` при отсутствии ключа), перестанет
  работать. Это осознанные, задокументированные изменения поведения (см.
  Task 7/ADR-0009, Task 10, Task 11/CHANGELOG), а не недосмотр.

## Testing Strategy

- **unit tests**: обязательны в каждой задаче, согласно Development Approach выше
- **e2e tests**: в проекте их нет (нет UI); `pytest -m packaging` — отдельная
  тема, этим планом не затрагивается
- Нигде в репозитории нет существующего хелпера, рендерящего `template_fields`
  через Jinja, и `tests/conftest.py` пока не существует (проверено — см.
  Context). Task 2 создаёт `tests/conftest.py` с общим хелпером (Technical
  Details), чтобы каждый тестовый модуль (`test_operator_base.py`,
  `test_operator_s3.py`, `test_operator_local.py`, `test_sensor.py`,
  `test_sensor_s3.py`) мог использовать его одной строкой `from conftest
  import render_fields` — без пере-объявления в каждом файле. Это обычная
  функция, а не pytest-фикстура: `conftest.py` автообнаруживается pytest'ом
  только для фикстур/хуков, для простого импортируемого хелпера всё равно
  нужен явный `import` в каждом тестовом модуле (`tests/` без `__init__.py`,
  так что это top-level импорт, а не относительный).

## Progress Tracking

- отмечать выполненное `[x]` сразу
- новые задачи добавлять с префиксом ➕
- блокеры/проблемы помечать префиксом ⚠️
- обновлять план, если реализация отклоняется от исходного скоупа
- держать план синхронизированным с фактически сделанной работой

## Solution Overview

1. `dates.py`: добавить `resolve_lookback_days(value: int | str | None) -> int`
   (каст + отклонение отрицательных) и `resolve_overwrite(value: bool | str |
   int | None) -> bool` (нативный `bool`/`int` `0`/`1` проходит насквозь;
   `"true"/"1"` / `"false"/"0"` регистронезависимо строками; всё остальное —
   исключение) **рядом** с существующей `validate_lookback_days`, которая
   удаляется отдельной задачей (Task 5b) только после того, как оба её
   вызывающих (оператор в Task 2, сенсор в Task 5a) перестанут её
   импортировать — см. ограничение по порядку выше. Deprecated-обёртка не
   оставляется после итогового удаления — единственные вызывающие внутри
   этого пакета.
2. `template_fields`: добавить `"lookback_days"` в `GmailAttachmentsBaseOperator`
   и `GmailAttachmentSensor`; добавить `"overwrite"` только в
   `GmailAttachmentsBaseOperator` (S3-оператор наследует оба). Локальный
   оператор тоже наследует базовый кортеж (включая `"overwrite"`), хотя не
   объявляет `overwrite` в собственной сигнатуре — см. проверенное примечание
   про проброс через kwargs выше; это описывается точно, а не как no-op.
3. Сигнатуры `__init__`: `lookback_days: int = 7` → `int | str = 7` (`0` для
   локального оператора), `overwrite: bool = False` → `bool | str = False`
   (базовый + S3-оператор). Каста в `__init__` нет; механизм
   template-рендеринга Airflow подставляет отрендеренное значение на инстанс
   до `execute()`/первого `poke()`.
4. Точка каста в рантайме: начало `_run()` (оператор) и начало
   `_find_messages()` (сенсор) — локальные переменные `lookback_days =
   resolve_lookback_days(self.lookback_days)` / `overwrite =
   resolve_overwrite(self.overwrite)`, используются в остальной части метода
   (сравнение для WARNING, `Window.resolve(...)`, `decide(...)`). Каст
   безусловный — выполняется даже в режиме явного диапазона, где
   `lookback_days` в итоге не используется `Window.resolve` (задокументированный
   компромисс, см. Context).
5. Отдельный регрессионный тест (Task 6, единственный владелец — Task 2/5
   сознательно не трогают существующие int-тесты WARNING) фиксирует, что
   сравнение в WARNING «explicit range overrides a non-default lookback_days»
   сравнивает `int` с `int` после каста — это баг, который весь этот план мог
   бы тихо внести, если бы шаблонизированный `lookback_days` сравнивался с
   `default_lookback_days`, оставаясь строкой. Поскольку отформатированный `%s`
   текст лога рендерит `3` и `"3"` одинаково, этот тест проверяет захваченный
   `LogRecord.args` (фактический python-тип), а не отформатированную строку.
6. Новый ADR (`0009`) фиксирует решение о шаблонизации + строгом фолбэке,
   изменение точки отказа (parse-time → runtime) для невалидных литералов
   `lookback_days`, вновь введённую валидацию `overwrite`, компромисс с
   безусловным кастом в режиме явного диапазона, обоснование названия
   `resolve_` и осознанный вне-скоуп (timezone/has_attachment/mark_processed/
   label_suffix как задел, attachment_pattern как уже решённая не-цель по ADR-0005).

## Technical Details

```python
# dates.py
def resolve_lookback_days(value: int | str | None) -> int:
    if isinstance(value, bool):
        # int(True) == 1 would otherwise silently turn a native-Jinja-rendered
        # `true` (render_template_as_native_obj=True) into a 1-day window.
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(check the DAG's Jinja expression renders a number, not a boolean)"
        )
    if isinstance(value, float):
        # int(1.9) == 1 would otherwise silently narrow the window instead of
        # rejecting a fractional day count; native Jinja rendering
        # (render_template_as_native_obj=True) can hand back a float from an
        # arithmetic expression in dag_run.conf.
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(a fractional value is not a valid number of days)"
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(check the DAG's Jinja expression renders a number)"
        ) from exc
    if parsed < 0:
        raise ValueError(f"lookback_days must be >= 0 (0 means 'today only'), got {parsed}")
    return parsed


def resolve_overwrite(value: bool | str | int | None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1"):
            return True
        if low in ("false", "0"):
            return False
    elif isinstance(value, int):
        # Покрывает нативный рендеринг Jinja (render_template_as_native_obj=True),
        # где значение conf 0/1 приходит настоящим int, а не "0"/"1".
        if value in (0, 1):
            return bool(value)
    raise ValueError(
        f"overwrite must render to true/false, got {value!r} "
        "(check the DAG's Jinja expression)"
    )
```

Важно: проверка `isinstance(value, bool)` обязана идти **до** ветки
`isinstance(value, int)` — в Python `bool` является подклассом `int`, так что
порядок важен (голый `True`/`False` должен сработать раньше ветки `0`/`1`,
даже если результат случайно совпал бы в обоих случаях).

`_run()` (оператор, делегат `execute()`):

```python
lookback_days = resolve_lookback_days(self.lookback_days)
overwrite = resolve_overwrite(self.overwrite)
...
if (date_from is not None or date_to is not None) and (
    lookback_days != self.default_lookback_days
):
    self.log.warning(
        "An explicit date_from/date_to range was given; the non-default "
        "lookback_days=%s is ignored.",
        lookback_days,
    )

window = Window.resolve(ref_day, self.timezone, lookback_days, date_from, date_to)
...
manifest = None if overwrite else self._read_manifest(rel_dir)
decision = decide(manifest, run_id, overwrite)
```

`_find_messages()` (сенсор): та же схема, `lookback_days =
resolve_lookback_days(self.lookback_days)` в начале, используется в сравнении
для WARNING и в `Window.resolve(...)`.

**Общий хелпер для тестов рендеринга** (вводится в Task 2 как
`tests/conftest.py`, переиспользуется в Task 3/4/5/6). **Важно:** pytest
автоматически подхватывает из `conftest.py` только фикстуры и хуки — обычная
функция там **не** становится доступной в других тестовых модулях сама по
себе, её нужно явно импортировать. Поскольку в `tests/` нет `__init__.py`,
pytest вставляет саму директорию `tests/` в `sys.path` (rootdir-based import
без пакета), так что обычный `from conftest import render_fields` в начале
каждого тестового модуля работает как штатный top-level импорт — фикстурой
она не оформляется, это осознанный выбор простоты (не нужен параметр
фикстуры в сигнатуре каждого теста):

```python
# tests/conftest.py
from types import SimpleNamespace


def render_fields(op, **conf) -> None:
    """Рендерит templated-поля ``op``, используя ``conf`` как `dag_run.conf`.

    DAG/DagBag не конструируется: ``op`` — обычный, не привязанный к DAG
    экземпляр оператора, а ``AbstractOperator.get_template_env()``
    откатывается на голый ``SandboxedEnvironment(cache_size=0)``, когда
    ``op.dag`` — ``None`` — без обращения к БД Airflow, без сериализации.
    """
    op.render_template_fields(context={"dag_run": SimpleNamespace(conf=conf)})
```

Пример использования в тесте: `from conftest import render_fields` в начале
файла, затем `op = _make_operator(lookback_days="{{
dag_run.conf.get('lookback_days', 7) }}")`, `render_fields(op,
lookback_days=14)`, затем вызвать `op.execute(...)`/`op._run(...)`.

**Паттерн проверки аргумента лога** (Task 6 — доказывает, что резолвенное
значение — скастованный `int`, поскольку отформатированный `%s`-текст не может
отличить `3` от `"3"`):

```python
record = next(r for r in caplog.records if "lookback_days" in r.getMessage())
assert record.args == (3,)  # int 3, а не строка "3"
```

## What Goes Where

- **Implementation Steps**: изменения `dates.py`, оператора/сенсора, ADR,
  AGENTS.md, обновления README, все правки тестов — всё достижимо внутри
  этого репозитория.
- **Post-Completion**: ничего внешнего — это изменение библиотеки без
  необходимости миграции в потребляющих проектах (обратно совместимо для
  валидных значений).

## Implementation Steps

### Task 1: `dates.py` — добавить `resolve_lookback_days` / `resolve_overwrite`

**Files:**
- Modify: `src/airflow_provider_gmail/dates.py`
- Modify: `tests/test_dates.py`

- [x] добавить `resolve_lookback_days(value: int | str | None) -> int` по Technical Details
- [x] добавить `resolve_overwrite(value: bool | str | int | None) -> bool` по
      Technical Details (`bool` проверяется до `int`, поскольку `bool` —
      подкласс `int`)
- [x] **пока оставить `validate_lookback_days` на месте** — не удалять её в
      этой задаче (см. ограничение по порядку в Context); у неё ещё два живых
      вызывающих (`operators/gmail.py`, `sensors/gmail.py`), пока не выполнены
      Task 2 и Task 5a, и удаление здесь сломает сбор тестов для всего набора
- [x] обновить докстринг модуля в двух местах, не только в одном: открывающая
      фраза «Pure date/time helpers shared by the operators and the sensors»
      тоже становится неточной, поскольку `resolve_overwrite` — это не
      дата/время, а политика приведения типа; переформулировать её вместе с
      заключительной фразой («...purely to share two pure functions»,
      устаревающей, поскольку в модуле теперь больше двух публичных хелперов),
      не пересматривая при этом, почему модуль в принципе не импортирует
      Airflow. Заодно добавить `resolve_lookback_days`/`resolve_overwrite` в
      идущий следом список-перечисление хелперов модуля (`parse_date_range`,
      `to_local_date`/`to_local_iso`, `from_local_iso`) — иначе переформулировка
      без правки списка будет выглядеть недоделанной
- [x] обновить докстринг `tests/test_dates.py` (сейчас — «Tests for
      :func:`...dates.from_local_iso`», т.е. привязан к одной функции) так,
      чтобы отражать, что модуль тестирует несколько независимых хелперов
      `dates.py`, а не только `from_local_iso`
- [x] написать тесты для `resolve_lookback_days` в `tests/test_dates.py`:
      строка `"14"` → `14`, нативный `int` проходит насквозь, отрицательное →
      `ValueError`, нативный `bool` (`True`/`False`) → `ValueError` (иначе
      `int(True) == 1` тихо превратил бы нативно отрендеренный `true` в
      однодневное окно), нативный `float` (`1.9` **и** `3.0`) → `ValueError`
      (иначе `int(1.9) == 1` тихо сузил бы окно вместо явного отказа —
      нативный Jinja может вернуть float из арифметического выражения в
      `dag_run.conf`; отклоняется даже «целый» `3.0`, без исключений для
      круглых значений — та же строгая политика, что у `bool`),
      `""`/`"None"`/`"abc"`/нативный `None` → `ValueError` с сообщением,
      упоминающим Jinja-выражение (нативный `None` важен, потому что
      `{{ dag_run.conf.get('x') }}` даёт python `None` напрямую при
      `render_template_as_native_obj=True`, а не только строку `"None"`)
- [x] написать тесты для `resolve_overwrite` в `tests/test_dates.py`:
      `"true"`/`"True"`/`"1"` → `True`, `"false"`/`"0"` → `False`, нативный
      `bool` проходит насквозь (и `True`, и `False`), нативный `int` `1` →
      `True`, нативный `int` `0` → `False`, нативный `int` `2` → `ValueError`,
      `""`/`"None"`/нативный `None`/`"yes"`/мусор → `ValueError`
- [x] прогнать тесты — должны пройти перед task 2

### Task 2: Базовый оператор — templated `lookback_days` + `overwrite`

**Files:**
- Create: `tests/conftest.py`
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `tests/test_operator_base.py`

- [x] в `operators/gmail.py` заменить импорт `validate_lookback_days` на импорт
      `resolve_lookback_days`, `resolve_overwrite` из
      `airflow_provider_gmail.dates` — импорт `validate_lookback_days` в этом
      файле можно убрать сразу, поскольку её вызов в `__init__` убирается
      следующим пунктом; сама функция остаётся определена в `dates.py`, пока
      Task 5a не уберёт отдельный (пока ещё живой) импорт сенсора, и Task 5b
      не удалит саму функцию; `validate_timezone` оставить как есть
- [x] добавить `"lookback_days"`, `"overwrite"` в
      `GmailAttachmentsBaseOperator.template_fields`
- [x] изменить аннотации `__init__` на `lookback_days: int | str = 7`,
      `overwrite: bool | str = False`; убрать вызов
      `validate_lookback_days(lookback_days)` из `__init__`; присваивание
      оставить как есть (`self.lookback_days = lookback_days`,
      `self.overwrite = overwrite`)
- [x] исправить ставший наполовину ложным инлайн-комментарий на
      `operators/gmail.py:185-186` («Reject negative lookback ... fails at DAG
      parse») — верна только половина про timezone; переформулировать так,
      чтобы отражать, что `lookback_days` теперь валидируется в рантайме
- [x] в `_run()`: добавить `lookback_days = resolve_lookback_days(self.lookback_days)`
      и `overwrite = resolve_overwrite(self.overwrite)` в начале (после
      `run_id`/`ref_day`), заменить все последующие обращения к
      `self.lookback_days`/`self.overwrite` в методе (сравнение для WARNING,
      `Window.resolve(...)`, строку `manifest = None if ...`,
      `decide(manifest, run_id, ...)`) на локальные переменные
- [x] обновить докстринг класса: `lookback_days`/`overwrite` теперь templated и
      парсятся в рантайме, по аналогии с `date_from`/`date_to` (докстринга у
      `__init__` сегодня нет — контракт параметров живёт в докстринге класса и
      README, это покрыто в Task 9)
- [x] создать `tests/conftest.py` с общим хелпером `render_fields(op, **conf)`
      по Technical Details — без конструирования `DAG`/`DagBag`
- [x] обновить `test_template_fields_expected_set`, чтобы требовать наличие
      `lookback_days` и `overwrite` в
      `GmailAttachmentsBaseOperator.template_fields` (этот тест уже использует
      subset-проверку — менять посылку не нужно)
- [x] заменить `test_init_negative_lookback_days_raises` (сейчас проверяет
      исключение в момент `__init__`) на тест на исключение в рантайме:
      создать оператор с `lookback_days=-1`, убедиться, что `__init__` **не**
      падает, затем убедиться, что `execute()`/`_run()` падает с `ValueError`
- [x] добавить тест на рендер с помощью `render_fields`:
      `lookback_days="{{ dag_run.conf.get('lookback_days', 7) }}"`, рендер с
      `lookback_days=14`, затем `execute()`/`_run()` и проверка, что итоговое
      окно `Window` использовало 14 дней
- [x] добавить негативный тест на рендер: та же настройка со значением conf
      `-1`, убедиться, что `execute()` падает с `ValueError` (не проглатывается)
- [x] добавить один сквозной тест на **нативный** рендеринг
      (`render_template_as_native_obj=True`): создать реальный `DAG(...,
      render_template_as_native_obj=True)`, привязать к нему оператор с
      `lookback_days` из шаблона (например, `"{{ dag_run.conf['lookback_days'] }}"`),
      выполнить рендер с нативным `int` в conf (не строкой) и убедиться, что
      `execute()`/`_run()` строит окно на нужное число дней. Ранее в плане
      нативный путь проверялся только прямыми юнит-вызовами резолверов
      (Task 1) — без этого теста нет автоматического доказательства, что
      нативно отрендеренный `int`/`bool` действительно доходит до
      `self.lookback_days`/`self.overwrite` через реальный `render_template_fields()`,
      а не только что резолверы умеют такие типы принимать
- [x] обновить докстринг модуля `tests/test_operator_base.py` (сейчас:
      «...the `template_fields` set and `__init__` validation») — валидация
      `lookback_days` для этого файла больше не `__init__`-валидация, а
      рантайм-каст; переформулировать, не переписывая остальное
- [x] **не** менять `test_execute_nondefault_lookback_with_range_warns` (строка
      580) и `test_execute_default_lookback_with_range_does_not_warn` (строка
      591) в этой задаче — оба уже проходят с обычным `int`/без аргумента
      `lookback_days` и остаются такими; строковая версия этой регрессии — в
      исключительном владении Task 6, здесь не дублируется
- [x] прогнать тесты — должны пройти перед task 3

### Task 3: S3-оператор — покрытие рендера шаблона для `overwrite`

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py` (`GmailAttachmentsToS3Operator`: аннотация `__init__` + докстринг класса)
- Modify: `tests/test_operator_s3.py`

- [x] изменить аннотацию `overwrite: bool = False` в
      `GmailAttachmentsToS3Operator.__init__` на `overwrite: bool | str = False`
      (проброс в базовый класс уже корректен, логика не меняется)
- [x] обновить докстринг класса: отметить, что `overwrite` теперь templated
      (унаследовано от базового класса) и сочетается с существующей заметкой
      про backfill через `date_from`/`date_to` (ADR-0004) — backfill,
      управляемый через `dag_run.conf`, теперь может переключать `overwrite`
      без передеплоя DAG; также отметить, что `overwrite` раньше был
      **невалидируемым**, а теперь проходит через `resolve_overwrite`; отдельным
      предложением отметить, что шаблонизация обостряет уже существующую
      ловушку «`overwrite` несовместим с `GmailAttachmentToS3Sensor`»
      (README.md:230 и докстринги S3-оператора/S3-сенсора) — DAG с этим
      сенсором и templated `overwrite` теперь может тихо встать в deadlock от
      простого изменения значения в `conf`, без правки самого DAG-файла
- [x] добавить тест на рендер (`render_fields` из `tests/conftest.py`):
      `overwrite="{{ dag_run.conf.get('overwrite', 'false') }}"`, рендер с
      `overwrite="true"`, затем запустить через существующие хелперы
      `_make_op`/`_run` плюс фикстуру манифеста, которая иначе привела бы к
      `SKIP`/отсутствию повторной загрузки, и убедиться, что срабатывает путь
      принудительной перезагрузки (по аналогии с
      `test_overwrite_true_forces_download_despite_current_manifest`, строка
      405, но со templated-значением)
- [x] добавить негативный тест на рендер: значение conf `"maybe"` →
      `execute()` падает с `ValueError`
- [x] добавить тест на рендер значения `"false"` **с существующим манифестом
      текущего run'а** (тот же фикстурный сетап, что использует
      `test_overwrite_true_forces_download_despite_current_manifest`, но
      наоборот): убедиться, что путь принудительной перезагрузки НЕ
      срабатывает (обычное `DELIVER_ONLY`/`SKIP`-поведение). Тест только на
      `"true"` и на невалидное `"maybe"` доказывает лишь, что резолвер был
      вызван — реализация, которая считает `overwrite =
      resolve_overwrite(self.overwrite)`, но по ошибке продолжает читать
      `self.overwrite` в местах принятия решения (`operators/gmail.py`, где
      сравнивается `manifest = None if overwrite else ...` и
      `decide(manifest, run_id, overwrite)`), пройдёт оба существующих теста
      и всё равно трактует `"false"` как `True` по «правдивости» непустой
      строки — только явный тест на `"false"` это ловит
- [x] прогнать тесты — должны пройти перед task 4

### Task 4: Локальный оператор — аннотации + точная документация `overwrite`

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py` (`GmailAttachmentsToLocalOperator`: аннотация `__init__` + докстринг класса + инлайн-комментарий в `__init__`)
- Modify: `tests/test_operator_local.py`

- [ ] изменить аннотацию `lookback_days: int = 0` в
      `GmailAttachmentsToLocalOperator.__init__` на `int | str = 0`
- [ ] исправить в докстринге класса утверждение «There is no user-facing
      `overwrite` argument» на точное, а не желаемое: `overwrite` не входит в
      *явную* сигнатуру `__init__` этого класса, но **всё равно** достижим
      через проброс `**kwargs` в базовый `__init__` (проверено — см. Context) —
      включая, после этого плана, Jinja-строку, поскольку `overwrite`
      наследуется в `template_fields`. Явно указать, что этот план **не**
      закрывает этот существующий пробел (вне скоупа), и что передача
      невалидного отрендеренного значения этим путём всё равно вызовет
      исключение через `resolve_overwrite`, хотя `_read_manifest()` для этого
      класса всегда возвращает `None`, так что результат каста никогда не
      меняет поведение здесь
- [ ] исправить **ещё одно** место с той же неточной формулировкой — инлайн-
      комментарий в `GmailAttachmentsToLocalOperator.__init__`
      (`operators/gmail.py:717-718`): «No public ``overwrite`` argument: it is
      fixed at the base default (False) and never exposed here (see the class
      docstring)». Это утверждение хуже, чем докстринг класса, который правит
      предыдущий пункт: оно ссылается на тот самый докстринг, который теперь
      будет говорить обратное, и фраза «fixed at the base default (False)»
      прямо опровергнута проверкой выше. Переформулировать в духе
      исправленного докстринга класса, а не оставить это внутреннее
      противоречие
- [ ] `test_default_lookback_days_is_zero` (строка 146) и
      `test_overwrite_attribute_false_but_not_in_public_signature` (строка
      155) проверяют поведение с *дефолтными* аргументами, на что правка
      только аннотации не влияет — убедиться, что оба продолжают проходить
      без изменений, а не редактировать их
- [ ] добавить тест на рендер (`render_fields`):
      `lookback_days="{{ dag_run.conf.get('lookback_days', 0) }}"`, рендер с
      `lookback_days=5` → окно построено на 5 дней
- [ ] добавить один тест, документирующий проверенный проброс через kwargs —
      через `execute()`/`_run()`, а НЕ прямым вызовом `resolve_overwrite(op.overwrite)`
      (прямой вызов резолвера доказывает только, что сама функция работает, а
      не то, что рантайм локального оператора реально её применяет): создать
      `GmailAttachmentsToLocalOperator(...,
      overwrite="{{ dag_run.conf.get('overwrite', 'false') }}")`, рендер с
      `overwrite="true"`, выполнить `execute()`/`_run()` через существующие
      хелперы `_make_op`/`_run` этого файла и убедиться, что запуск проходит
      без исключения; рендер с невалидным значением (например, `"maybe"`) →
      `execute()`/`_run()` падает с `ValueError` — это тест-документация
      (подтверждает исправленное утверждение докстринга и комментария), не
      изменение поведения. Назвать тест и снабдить его комментарием явно как
      **characterization test** признанного, но не исправляемого этим планом
      пробела (со ссылкой на пункт ADR-0009 «не исправлен этим планом» —
      Task 7), чтобы будущая правка, закрывающая этот пробел, не восприняла
      тест как гарантию контракта, которую нельзя менять
- [ ] прогнать тесты — должны пройти перед task 5a

### Task 5a: Базовый сенсор — templated `lookback_days`

**Files:**
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `tests/test_sensor.py`
- Modify: `tests/test_sensor_s3.py`

- [ ] импортировать `resolve_lookback_days` из `airflow_provider_gmail.dates`,
      убрать импорт `validate_lookback_days` из этого файла (её вызов
      убирается следующим пунктом; сама функция в `dates.py` остаётся до
      Task 5b — это последний внутрипакетный потребитель); `validate_timezone`
      оставить как есть
- [ ] добавить `"lookback_days"` в `GmailAttachmentSensor.template_fields`
- [ ] изменить аннотацию `lookback_days: int = 7` в `__init__` на
      `int | str = 7`, убрать вызов `validate_lookback_days(lookback_days)`,
      присваивание оставить как есть
- [ ] исправить ставший наполовину ложным инлайн-комментарий на
      `sensors/gmail.py:124-125` (парный операторскому — верна только половина
      про timezone)
- [ ] в `_find_messages()`: добавить `lookback_days =
      resolve_lookback_days(self.lookback_days)` в начале, заменить
      последующие обращения к `self.lookback_days` (сравнение для WARNING,
      `Window.resolve(...)`) на локальную переменную
- [ ] обновить докстринг класса: `lookback_days` теперь templated, по аналогии
      с `date_from`/`date_to`; расширить существующую заметку «Pairing with
      the local operator: match `lookback_days`» указанием, что это верно
      независимо от того, обычный ли это int или отрендеренный шаблон
- [ ] обновить `test_template_fields_match_base_operator` (строка 130, сейчас
      **строгое равенство**): её посылка меняется — базовый оператор теперь
      имеет и `lookback_days`, и `overwrite`, сенсор — только
      `lookback_days` — переименовать (например,
      `test_sensor_template_fields_are_operator_subset_minus_overwrite`) и
      проверять новый точный набор плюс комментарий, фиксирующий осознанную
      асимметрию
- [ ] добавить `assert "overwrite" not in GmailAttachmentSensor.template_fields`
      в `test_sensor_has_no_overwrite_parameter` (строка 183) — guard, который
      поймает случайный copy-paste `overwrite` в сенсор
- [ ] заменить `test_negative_lookback_days_fails_at_init` на тест на
      исключение в рантайме: `__init__` с `lookback_days=-1` не падает,
      `poke()` падает (`ValueError`)
- [ ] добавить тест на рендер (`render_fields` из `tests/conftest.py`):
      `lookback_days="{{ dag_run.conf.get('lookback_days', 7) }}"`,
      `lookback_days=14` → `poke()` ищет в 14-дневном окне
- [ ] **не** менять `test_range_override_warning_logged_once_across_pokes`
      (строка 194, единственный WARNING-тест сенсора) в этой задаче — она
      остаётся с обычным `int`; строковая версия — в исключительном владении Task 6
- [ ] расширить `test_query_parity_with_local_operator` (строка 233) и/или
      `test_query_parity_with_explicit_range` (строка 280) вариантом, где
      `lookback_days` передаётся **строкой** и сенсору, и оператору под
      тестом — доказывает, что сам каст остаётся идентичным между ними, а
      именно от этого зависит инвариант «query parity» теперь, когда
      `lookback_days` может быть templated
- [ ] добавить один тест на рендер для `GmailAttachmentToS3Sensor` в
      `tests/test_sensor_s3.py`, подтверждающий, что он наследует
      шаблонизированное поведение `lookback_days` без изменений через
      `_find_messages()` — для этого подкласса изменений продакшн-кода не
      ожидается (он не переопределяет `_find_messages()` ни обработку
      templated-полей в `__init__`); эта единственная проверка существует,
      чтобы доказать, что подкласс не был случайно упущен
- [ ] прогнать тесты — должны пройти перед task 5b

### Task 5b: Вывод из эксплуатации `validate_lookback_days`

**Files:**
- Modify: `src/airflow_provider_gmail/dates.py`

- [ ] **теперь, когда оба потребителя мигрированы** (оператор в Task 2, сенсор
      в Task 5a), полностью удалить `validate_lookback_days` из
      `src/airflow_provider_gmail/dates.py` (deprecated-обёртка не
      оставляется); отдельных тестов на неё нет (подтверждено в Task 1 —
      `tests/test_dates.py` их не создавал), поэтому в тестах удалять нечего
- [ ] прогнать тесты — должны пройти перед task 6
- [ ] убедиться, что `grep -rn validate_lookback_days src/ tests/` теперь не
      находит ничего — именно в этой точке плана это становится верным

### Task 6: Регрессионный тест — сравнение в WARNING после каста

**Files:**
- Modify: `tests/test_operator_base.py`
- Modify: `tests/test_sensor.py`

- [ ] добавить `test_explicit_range_with_string_lookback_days_still_warns`
      (оператор): заданы `date_from`/`date_to`, `lookback_days` передан
      **нестандартной строкой** (например, `"3"` при дефолте `7`,
      отрендерено через `render_fields`), убедиться, что WARNING срабатывает
      ровно один раз, и проверить захваченный `LogRecord.args` (например,
      `record.args == (3,)`), чтобы доказать, что резолвенное значение —
      именно скастованный `int`, а не просто что отформатированный текст
      `%s` содержит `"3"` — это прошло бы одинаково и для `3`, и для `"3"`
- [ ] добавить зеркальный не-варнящий случай для оператора: `lookback_days`
      передан строкой, равной *дефолту* (например, `"7"`), с явным диапазоном
      → WARNING НЕ должен сработать
- [ ] добавить те же два случая (варнит / не варнит на строковом
      `lookback_days`, с проверкой `LogRecord.args`) для `_find_messages()` сенсора
- [ ] эта задача — **единственный владелец** проверок WARNING со строковым
      типом — Task 2 и Task 5a сознательно оставляют существующие int-тесты
      WARNING нетронутыми (см. примечания в этих задачах), так что
      дублирования нет, согласовывать нечего
- [ ] прогнать тесты — должны пройти перед task 7

### Task 7: ADR-0009 — задокументировать решение

**Files:**
- Create: `docs/adr/0009-lookback-days-overwrite-templated.md`

- [ ] написать ADR в существующем стиле (см. ADR-0004/0005): Context
      (симметрия с `date_from`/`date_to`, ADR-0004; контраст с
      `attachment_pattern`, остающимся не templated по ADR-0005 — не
      пересматривается), Decision (`lookback_days` + `overwrite` templated,
      `resolve_lookback_days`/`resolve_overwrite` в `dates.py`, строгий
      фолбэк — без тихого дефолта, явное упоминание ловушки
      `{{ dag_run.conf.get('x') }}` → буквальная строка `"None"` / нативный
      `None`, обработка нативных `int` `0`/`1` (и отклонение нативного
      `bool`) для `render_template_as_native_obj=True`, и обоснование
      названия `resolve_` против `parse_`), Consequences:
      - в скоупе: `lookback_days` + `overwrite`; задел: `timezone`/
        `has_attachment`/`mark_processed`/`label_suffix`; `attachment_pattern`
        сознательно исключён по ADR-0005
      - **изменение поведения (lookback_days)**: невалидный литерал (например,
        `lookback_days=-1`) теперь падает на `execute()`/первом `poke()`
        вместо момента парсинга DAG, потому что теперь один и тот же
        рантайм-путь обслуживает и литерал, и шаблон
      - **изменение поведения (overwrite)**: раньше вообще не валидировался;
        теперь строго валидируется через `resolve_overwrite`. Отдельно
        отметить краевой случай обратной несовместимости: `overwrite=None`
        сегодня молча работает как `False` (падает в обеих точках
        использования на `falsy`-проверку), а после этого плана
        `resolve_overwrite(None)` кидает `ValueError` — реалистичный сценарий
        это DAG, пишущий `overwrite=some_dict.get("overwrite")` без всякого
        Jinja. Решение — не подстраивать `None` под тихий `False` (это
        подорвало бы весь смысл строгого фолбэка для templated-случая, где
        `None` — самый частый мусорный рендер), а принять эту несовместимость
        осознанно и явно задокументировать
      - **безусловный каст**: `resolve_lookback_days`/`resolve_overwrite`
        выполняются в начале `_run()`/`_find_messages()` даже в режиме явного
        диапазона, где `Window.resolve` в итоге игнорирует `lookback_days` —
        шаблонизированный `lookback_days`, отрендерившийся в мусор, всё равно
        падает, даже когда его значение семантически не имеет значения для
        этого конкретного запуска; то же самое касается `overwrite` для
        локального оператора, где результат каста никогда не влияет на
        `_read_manifest()` (всегда `None`), но каст всё равно должен пройти успешно
      - существующий пробел с проброс `overwrite` через kwargs у локального
        оператора (задокументирован, не исправлен этим планом — см. Task 4)
      - шаблонизация `overwrite` обостряет существующую ловушку
        «несовместим с `GmailAttachmentToS3Sensor`» (см. Task 3) — теперь это
        может сработать через одно изменение в `dag_run.conf`, без правки DAG
      - **удаление `validate_lookback_days` без deprecated-обёртки — технически
        breaking change для внешнего кода, который мог импортировать её
        напрямую** (`from airflow_provider_gmail.dates import
        validate_lookback_days`) — репозиторий не может доказать grep'ом
        отсутствие таких внешних потребителей, только отсутствие внутренних.
        Явно зафиксировать обоснование, почему это принято без shim'а: у
        `operators/gmail.py` уже есть курируемый список ре-экспорта
        `__all__`, который явно включает `parse_date_range`, `to_local_date`,
        `to_local_iso` из `dates.py`, но никогда не включал
        `validate_lookback_days`/`validate_timezone` — то есть эти две функции
        уже были осознанно исключены из курируемой публичной поверхности
        пакета, а не просто «случайно публичны» из-за отсутствия `_`-префикса.
        Отметить удаление в CHANGELOG как потенциально breaking для прямых
        импортов, не как внутренний рефакторинг (см. Task 11)
- [ ] тесты не требуются — задача только про документацию

### Task 8: `AGENTS.md` — доменная ловушка

**Files:**
- Modify: `AGENTS.md`

- [ ] добавить пункт в «Domain traps» (рядом с существующим про `pick="all"`):
      `lookback_days`/`overwrite` templated и на обоих операторах, и (для
      `lookback_days`) на обоих сенсорах; отрендеренное пустое/`None`/`"None"`
      значение — это жёсткий `ValueError`, а не тихий откат к дефолту
      класса — DAG-автор обязан задать дефолт внутри самого Jinja-выражения;
      невалидный литерал `lookback_days` теперь падает в рантайме, а не в
      момент парсинга DAG; `overwrite`, ранее не валидировавшийся, теперь
      тоже строго валидируется
- [ ] тесты не требуются — задача только про документацию

### Task 9: `README.md` / `README_RU.md` — обновления таблицы параметров

**Files:**
- Modify: `README.md`
- Modify: `README_RU.md`

- [ ] обновить тип строки `lookback_days` на `int \| str` и добавить
      «Templated.» к описанию, в таблице параметров базового оператора
      (README.md:138, плюс русский эквивалент) — раздел про сенсоры
      (README.md:162-170) — это проза, утверждающая, что сенсоры полностью
      повторяют параметры оператора, так что отдельной правки строки
      `lookback_days` для сенсора не требуется
- [ ] обновить тип строки `overwrite` на `bool \| str` и добавить «Templated.»
      к описанию (README.md:151, плюс русский эквивалент); также отметить, что
      раньше он не валидировался, а теперь строго валидируется. Эта строка
      физически находится в таблице «`GmailAttachmentsToS3Operator` adds:» —
      это единственная объявленная публичная сигнатура `overwrite`, так что
      строку менять именно там правильно; но поскольку `template_fields`
      `overwrite` теперь наследуется из **базового** класса (Task 2), явно
      добавить рядом одну фразу, что технически поле шаблонизируется на
      базовом уровне, а не только для S3 — иначе таблица создаст впечатление,
      что `overwrite` templated исключительно для S3-оператора
- [ ] исправить **третье** место с тем же неточным утверждением «There is no
      public `overwrite` argument» — заметку под таблицей локального оператора
      (README.md:159-160: «there is **no public `overwrite`** argument — the
      local operator always overwrites», и её русский эквивалент
      README_RU.md:155-156) в соответствии с исправленным докстрингом класса
      (Task 4): `overwrite` не в явной сигнатуре, но достижим через `**kwargs`
      и (после этого плана) может быть шаблонной строкой. Без этой правки
      получится, что докстринг класса в коде говорит одно, а README — прямо
      противоположное
- [ ] добавить одно предложение в прозу вокруг таблицы, формулирующее строгое
      правило фолбэка: пустое/незаданное отрендеренное значение — исключение,
      а не тихий откат к дефолту класса — дефолт внутри самого Jinja-выражения
      задаёт DAG-автор
- [ ] явно перечислить в прозе принимаемые отрендеренные формы `overwrite`:
      строки `"true"`/`"false"` (регистронезависимо), `"1"`/`"0"`, нативные
      `bool`/`int` `0`/`1` при `render_template_as_native_obj=True`; добавить
      один конкретный пример Jinja-выражения с дефолтом
      (`"{{ dag_run.conf.get('overwrite', 'false') }}"`), чтобы не оставлять
      это только в докстринге кода
- [ ] прогнать существующий набор тестов (изменений тестов здесь не
      ожидается, но это трогает пользовательскую документацию, на которую
      косвенно ссылаются другие тесты — убедиться, что ничего не сломалось):
      `pytest`
- [ ] прогнать тесты — должны пройти перед task 10

### Task 10: Проверить критерии приёмки

- [ ] убедиться, что `lookback_days` templated и кастуется в рантайме на
      обоих классах операторов и обоих классах сенсоров; `overwrite` — на
      базовом + S3-операторе, и (через наследование `template_fields`, без
      собственной публичной сигнатуры) технически тоже на локальном операторе
      — см. Task 4
- [ ] убедиться, что `validate_lookback_days` больше нигде не существует в
      `src/` или `tests/` (`grep -rn validate_lookback_days src/ tests/`
      ничего не находит)
- [ ] убедиться, что WARNING «explicit range overrides non-default
      lookback_days» корректно срабатывает со строковым `lookback_days` и на
      операторе, и на сенсоре (тесты Task 6 проходят)
- [ ] убедиться, что обычный (не-templated) DAG, использующий
      `lookback_days=14`/`overwrite=True` как литеральные python-значения,
      продолжает работать без изменений (обратная совместимость для валидных
      значений)
- [ ] убедиться, что обычный `lookback_days=-1` теперь падает на
      `execute()`/первом `poke()`, а не на `__init__` (задокументированное,
      осознанное изменение поведения — Task 7/ADR-0009)
- [ ] прогнать полный набор тестов: `pytest`
- [ ] прогнать покрытие: `pytest --cov=airflow_provider_gmail
      --cov-report=term-missing` и убедиться в отсутствии регрессии
      относительно текущих 99%
- [ ] `pytest -m packaging` вне скоупа этого изменения (правок в области
      упаковки нет) — пропустить, если не были затронуты
      `pyproject.toml`/entry points

### Task 11: [Final] Обновить документацию

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `example_dags/` (опционально — см. ниже)

- [ ] обновить `CHANGELOG.md` (английский). Сейчас файл не использует секцию
      `## [Unreleased]` — записи добавляются сразу под уже выпущенным
      заголовком версии (последний — `## [0.4.0] - 2026-07-30`, версия берётся
      из git-тега через setuptools-scm). Поскольку тега для этого изменения
      ещё нет, добавить новую секцию `## [Unreleased]` НАД существующим
      `## [0.4.0]` (стандартная практика Keep a Changelog, даже если раньше в
      этом файле так не делали). Внести туда:
      - **headline-запись «Added»**: `lookback_days` (оператор + сенсор) и
        `overwrite` (оператор) теперь `template_fields` — управляются через
        `dag_run.conf`/Jinja так же, как `date_from`/`date_to` (ADR-0004) — а
        не только изменения поведения ниже; иначе CHANGELOG расскажет только
        о regression-рисках, но не о том, что вообще стало возможно
      - изменение точки отказа parse-time → runtime для `lookback_days` и
        вновь введённую валидацию `overwrite` как записи «Changed», отдельно
        упомянув, что `overwrite=None` (сегодня тихо работающий как `False`)
        теперь падает с `ValueError`
      - **удаление `validate_lookback_days`** как отдельную запись «Removed» с
        пометкой, что это потенциально breaking для кода, импортировавшего её
        напрямую (обоснование — см. Task 7/ADR-0009)
      - (README/доменная ловушка уже покрыты в Task 8/9, здесь не повторяются)
- [ ] опционально добавить пример, управляемый `dag_run.conf`, для
      `lookback_days`/`overwrite` в существующий backfill example DAG в
      `example_dags/` (только если это не усложнит существующие фикстуры
      `tests/test_example_dags.py`, в частности assert
      `operators[0].overwrite is True` на строке 94, который сломается, если
      `overwrite` этого DAG станет шаблонной строкой вместо литерала `True`,
      который проверяется сегодня — это nice-to-have, не обязательно)
- [ ] прогнать тесты — должны пройти (актуально, если `example_dags/` был
      затронут): `pytest tests/test_example_dags.py`
- [ ] перенести этот план в `docs/plans/completed/`

## Post-Completion

*Внешних/ручных шагов не выявлено — это изменение библиотеки без
необходимости миграции в потребляющих проектах для валидной конфигурации; DAG,
передающие уже невалидный литерал `lookback_days`, начнут падать в рантайме
вместо момента парсинга, а DAG, передающие уже невалидное значение
`overwrite` (ранее никак не проверявшееся), начнут падать там, где раньше
тихо вели себя неправильно — оба случая задокументированы, это не миграция.*
