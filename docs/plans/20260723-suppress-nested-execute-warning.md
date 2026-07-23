# Suppress `execute cannot be called outside TaskInstance!` warning

## Overview

Каждый запуск таска `download_to_s3` (`GmailAttachmentsToS3Operator`) логирует:

```
{baseoperator.py:399} WARNING - GmailAttachmentsToS3Operator.execute cannot be called outside TaskInstance!
```

Это **косметический шум** — таск отрабатывает штатно (манифесты пишутся, XCom и
downstream корректны). Полная диагностика — в `todo.md` (авторитетна).

Причина: `GmailAttachmentsToS3Operator` — единственный подкласс, который
переопределяет `execute` и внутри делает `return super().execute(context)`
(`src/airflow_provider_gmail/operators/gmail.py:493-504`). В Airflow 2.9+
`BaseOperatorMeta.__new__` оборачивает `execute` каждого подкласса декоратором
`ExecutorSafeguard`; вложенный `super().execute()` не несёт sentinel, который
`TaskInstance` передаёт только внешнему вызову → срабатывает `log.warning`.

Решение (**вариант A**): убрать переопределение `execute` в S3-операторе, а
валидацию отрендеренного `prefix` перенести в `pre_execute` — метод, который
`ExecutorSafeguard` **не** оборачивает и который `TaskInstance` вызывает уже
**после** рендеринга шаблонов. Вложенного `super().execute()` не остаётся →
предупреждение исчезает; штатное выполнение через `TaskInstance` не меняется.

Общий инвариант, на который опирается фикс (одинаков на 2.9.1 и 2.11.2, детали
реализации safeguard между версиями отличаются — см. Context): (1) `pre_execute`
не оборачивается сейфгардом; (2) порядок `render_templates → pre_execute →
execute`; (3) `TaskInstance` передаёт sentinel внешнему вызову `execute`, а
вложенный `super().execute()` его теряет.

**Ключевой бенефит:** чистые логи без ложного WARNING на каждом запуске, при
сохранении контракта штатного выполнения оператора (XCom, манифесты, dedup,
retries).

## Context (from discovery)

- Файлы:
  - `src/airflow_provider_gmail/operators/gmail.py` — `GmailAttachmentsToS3Operator`
    (класс со стр. 436; override `execute` стр. 493-504; `validate_prefix` уже
    импортирован, стр. 55). База `GmailAttachmentsBaseOperator.execute` — стр. 308.
  - `tests/test_operator_s3.py` — тесты S3-оператора; хелпер `_run` (стр. 176-178)
    зовёт `op.execute()` напрямую; тесты валидации `prefix` — стр. 216-238.
  - `CHANGELOG.md` — верхняя версия `[0.3.0] - 2026-07-22` (Keep-a-Changelog стиль).
- Провалидировано по **установленному** Airflow (в `.venv` — **2.11.2**; рантайм-
  target провайдера — 2.9.1 через constraints; номера строк ниже — из 2.11.2).
  **Важно:** реализация safeguard между версиями различается (в 2.9.1 sentinel
  извлекается напрямую по имени конкретного класса; 2.11.2 дополнительно
  использует thread-local bookkeeping) — но **общий инвариант**, на который
  опирается фикс, одинаков (см. Overview). Поэтому локальный прогон на 2.11.2 —
  необходимое, но не достаточное доказательство; acceptance-gate на 2.9.1 см.
  Task 3.
  - `models/baseoperator.py:528-531` — `ExecutorSafeguard` навешивается только на
    `namespace["execute"]`; `pre_execute` (стр. 1356) не оборачивается.
  - `models/taskinstance.py` — порядок `render_templates` (стр. 3141) →
    `pre_execute` (стр. 3164) → `execute`; sentinel кладётся по ключу
    `task.__class__.__name__` (стр. 733-735), т.е. после удаления override базовый
    `execute` на S3-инстансе получит корректный sentinel → тихо. Порядок и
    обработка sentinel в 2.9.1 подтверждают то же решение (baseoperator.py
    L376-400, taskinstance.py L2617-2675 в теге 2.9.1).
  - `ExecutorSafeguard.test_mode = conf.getboolean("core","unit_test_mode")`; в
    этом репо эффективное значение — **`False`** (проверено: нет `conftest.py`/env,
    `.venv/bin/python` → `unit_test_mode = False`). Следствие для тестов: см.
    Testing Strategy — голый `execute()` без sentinel логирует WARNING **всегда**
    (и до, и после фикса), поэтому тест на отсутствие WARNING обязан передавать
    sentinel, имитируя `TaskInstance`.
- Local-оператор (`GmailAttachmentsToLocalOperator`, стр. 578) и
  `GmailResolveAttachmentsOperator` `super().execute()` **не** зовут — правкам не
  подлежат.

## Development Approach

- **testing approach**: Regular (правка кода → обновление/добавление тестов в той
  же задаче).
- Правка кода и её тесты — **одна атомарная задача** (Task 1): между шагами репо
  не должно оставаться в непроверяемом состоянии.
- **CRITICAL: тесты обязательны** — проверить и что WARNING больше не логируется,
  и что валидация отрендеренного `prefix` сохранена (теперь через `pre_execute`),
  и что пользовательский `pre_execute`-хук сохранён и вызывается до валидации.
- **CRITICAL: все тесты зелёные** перед завершением задачи.
- Обратная совместимость: контракт **штатного выполнения через `TaskInstance`**
  сохраняется (сигнатура `execute`, возвращаемое значение, `AirflowSkipException`,
  XCom/манифесты/dedup). Оговорка: прямой ручной вызов `execute()` вне lifecycle
  больше не валидирует `prefix` (это делает `pre_execute`) и сам по себе помечается
  Airflow'ом как WARNING — такой путь не поддерживается.

## Testing Strategy

- **unit tests** (`pytest`): обязательны в каждой задаче.
  - **Предпосылка:** `unit_test_mode = False` (проверено). При `True`
    `ExecutorSafeguard` вообще не проверяет sentinel → тест на WARNING стал бы
    вакуумным (зелёным и на сломанном коде). Тест ниже устроен так, что остаётся
    осмысленным именно при `False`, и самопроверяется.
  - **Новый тест на отсутствие WARNING — обязан имитировать `TaskInstance`,
    передавая sentinel** (голый `execute()`/`_run` для этого НЕ годятся — они
    логируют WARNING всегда, независимо от фикса). Схема:
    ```python
    from airflow.models.base import _sentinel

    sentinel_kw = {f"{type(op).__name__}__sentinel": _sentinel}
    # (a) sanity/negative-control: без sentinel WARNING ЕСТЬ
    #     (доказывает, что сейфгард активен и caplog его видит; ловит unit_test_mode=True)
    op.pre_execute(ctx); op.execute(ctx)                     # → WARNING присутствует
    assert "cannot be called outside TaskInstance!" in caplog.text
    caplog.clear()   # ОБЯЗАТЕЛЬНО: caplog.records накапливает записи между ветками
    # (b) как из TaskInstance: с sentinel WARNING НЕТ (это и есть проверка фикса)
    op2.pre_execute(ctx); op2.execute(ctx, **sentinel_kw)    # → WARNING отсутствует
    assert "cannot be called outside TaskInstance!" not in caplog.text
    ```
    Ассертить по `caplog` на уровне WARNING наличие/отсутствие подстроки
    `"cannot be called outside TaskInstance!"`. **Без `caplog.clear()` между
    ветками** проверка (b) увидит запись из (a) и упадёт даже на правильном коде
    (false negative). На сломанной форме (override + вложенный `super().execute()`)
    ветка (b) дала бы WARNING из вложенного вызова — тест отличает починенный код
    от сломанного.
    **Важно по setup:** сейфгард логирует WARNING и **затем всё равно зовёт**
    обёрнутый `execute`, т.е. запускается полная база. Поэтому в обеих ветках надо
    задать `op.hook = FakeGmailHook([...])`, доставляющий сообщение (иначе
    `KeyError`/`AirflowSkipException("no new messages")`), — или обернуть вызов в
    `pytest.raises(AirflowSkipException)`, чтобы проверялся именно `caplog`, а не
    падало исполнение базы.
  - **Тест сохранения пользовательского `pre_execute`-хука.** `super().pre_execute()`
    — ключевое решение; отдельным тестом проверить, что хук, переданный параметром
    `pre_execute=<callable>`, действительно вызывается, и что он вызывается **до**
    `validate_prefix` (напр. хук с побочным эффектом/спаем + кривой `prefix`:
    убедиться, что хук отработал до `ValueError`). Опционально: исключение из хука
    останавливает дальнейшее выполнение.
  - Обновить хелпер `_run` так, чтобы он повторял порядок `TaskInstance`
    (`pre_execute(ctx)` → `execute(ctx)`), иначе валидация `prefix` перестанет
    срабатывать в тестах, дергающих `execute` напрямую. (Это отдельная забота от
    теста на WARNING — `_run` sentinel не передаёт и на WARNING не завязан.)
  - Существующие тесты валидации `prefix` (переименовать в
    `test_pre_execute_invalid_rendered_prefix_raises` /
    `test_pre_execute_valid_prefix_passes`, т.к. они теперь гоняют lifecycle-хелпер
    с `pre_execute`, а не голый `execute`; `test_templated_prefix_does_not_fail_at_construction`
    оставить) должны остаться зелёными после правки хелпера/комментариев.
- **e2e tests**: в проекте нет UI/e2e — неприменимо.
- Регресс всего пакета: `pytest` (маркер `packaging` deselected по умолчанию).

## Progress Tracking

- отмечать `[x]` сразу по завершении пункта;
- новые задачи — с префиксом ➕; блокеры — с ⚠️;
- держать план синхронным с фактической работой.

## Solution Overview

Убираем двухслойность `execute` в S3-операторе — источник ловушки
`ExecutorSafeguard`. Валидация отрендеренного `prefix` (fail-fast до реальной
оркестрации, паритет с `date_from`/`date_to`, ADR-0004/0007) переезжает в
`pre_execute`, который вызывается `TaskInstance` строго после рендеринга шаблонов
и не оборачивается сейфгардом. `GmailAttachmentsToS3Operator` перестаёт
переопределять `execute` — `TaskInstance` вызывает унаследованный базовый
`execute` прямо на S3-инстансе (с корректным sentinel).

Ключевое решение: сохранить пользовательский `pre_execute`-хук `BaseOperator` —
наш override первым делом зовёт `super().pre_execute(context)`, затем
`validate_prefix(self.prefix)`.

## Technical Details

- Удаляемое (`src/airflow_provider_gmail/operators/gmail.py:493-504`):
  ```python
  def execute(self, context: Any) -> list[str]:
      """..."""
      validate_prefix(self.prefix)
      return super().execute(context)
  ```
- Добавляемое (в тот же класс, до `_s3_hook`):
  ```python
  def pre_execute(self, context: Any) -> None:
      """Validate the rendered ``prefix`` before the base orchestration runs.

      ``prefix`` is a template field, so it is validated on its **rendered**
      value (parity with ``date_from``/``date_to``, ADR-0004) — not in
      ``__init__`` where a ``{{ ds }}`` template would trip the ``{``/``}``
      check. ``TaskInstance`` calls ``pre_execute`` after template rendering and
      before ``execute``, so the rendered ``prefix`` is available and validation
      still fails fast (ValueError) with a clear message, keeping produced object
      keys URL-safe by construction (ADR-0007). Validation lives here — not in an
      overriding ``execute`` calling ``super().execute()`` — because that nested
      call trips ``ExecutorSafeguard`` and logs a spurious
      "... .execute cannot be called outside TaskInstance!" warning.
      """
      super().pre_execute(context)
      validate_prefix(self.prefix)
  ```
- Путь исполнения (`execution_timeout`, `retries`, XCom, манифесты, dedup) не
  меняется — уходит только вложенный `super().execute()`.

## What Goes Where

- **Implementation Steps** (`[ ]`): правка оператора + связанных docstring
  (`utils/paths.py`, `sensors/gmail.py`) + тесты (одна атомарная задача), CHANGELOG,
  верификация (вкл. 2.9.1-gate), закрытие плана.
- **Post-Completion** (без чекбоксов): ручная проверка на реальном Airflow 2.9,
  что WARNING исчез из логов `download_to_s3` (в дополнение к CI-gate из Task 3).

## Implementation Steps

### Task 1: Перенести валидацию `prefix` в `pre_execute` + тесты + связанные docstring (атомарно)

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `src/airflow_provider_gmail/utils/paths.py`
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `tests/test_operator_s3.py`

Реализация:
- [x] удалить метод `execute` (стр. ~493-504) в `GmailAttachmentsToS3Operator`
- [x] добавить `pre_execute(self, context)` в тот же класс: сначала
      `super().pre_execute(context)`, затем `validate_prefix(self.prefix)`;
      перенести docstring-обоснование (rendered `prefix`, паритет с
      `date_from`/`date_to`, ADR-0004/0007, причина: ExecutorSafeguard/вложенный
      вызов) в docstring `pre_execute`
- [x] убедиться, что `validate_prefix` остаётся импортированным/используемым
      (стр. 55); лишних импортов не осталось
- [x] `utils/paths.py:70` (docstring `validate_prefix`): «at the top of
      `execute()`/`poke()`» → отразить новое место (S3-оператор валидирует в
      `pre_execute()`, сенсор — в `poke()`)
- [x] `sensors/gmail.py:359` (docstring `poke`): «parity with the S3 operator's
      `execute()`» → «... `pre_execute()`»

Тесты (в этой же задаче):
- [x] обновить хелпер `_run` (стр. 176-178): `op.pre_execute(context)` перед
      `op.execute(context)` — повторить порядок `TaskInstance`
- [x] актуализировать комментарии про «валидацию в `execute()`» (напр. стр. ~218)
      → «в `pre_execute()`»
- [x] переименовать `test_execute_invalid_rendered_prefix_raises` /
      `test_execute_valid_prefix_passes` →
      `test_pre_execute_invalid_rendered_prefix_raises` /
      `test_pre_execute_valid_prefix_passes`; убедиться, что проходят (ошибка
      валидации теперь из `pre_execute`)
- [x] добавить тест на WARNING **с передачей sentinel** и **`caplog.clear()` между
      ветками** (см. Testing Strategy): ветка без sentinel → WARNING есть
      (negative-control), ветка с `**{f"{type(op).__name__}__sentinel": _sentinel}`
      → WARNING нет
- [x] добавить тест сохранения пользовательского `pre_execute`-хука: хук из
      параметра `pre_execute=` вызывается и вызывается **до** `validate_prefix`
- [x] запустить `pytest tests/test_operator_s3.py` — **должно быть зелёным**
      перед переходом к Task 2

### Task 2: Запись в CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`

- [ ] `[0.3.0]` уже зафиксирован (`b5f07e2`, дата `2026-07-22`) — **не трогать
      его**. Добавить **новый верхний** раздел `## [Unreleased]` с `### Fixed`
      (именно `[Unreleased]`, не `[0.3.1]` — версию задаёт git-тег, не changelog)
- [ ] текст записи (English, Keep-a-Changelog): устранён ложный WARNING
      `GmailAttachmentsToS3Operator.execute cannot be called outside TaskInstance!`
      на каждом запуске; валидация `prefix` перенесена из `execute()` в
      `pre_execute()`, штатное поведение оператора не изменилось (Airflow 2.9+/
      `ExecutorSafeguard`). Явно указать смену места валидации `execute()` →
      `pre_execute()` (блок 0.3.0 «ValueError at the top of `execute()`» оставляем
      как есть, новая запись уточняет)
- [ ] (документация только-changelog — отдельных тестов не требует)

### Task 3: Verify acceptance criteria
- [ ] проверить, что реализованы все требования из Overview (WARNING устранён,
      штатное поведение не изменилось)
- [ ] проверить, что больше нет сайтов `super().execute()`: Local-оператор и
      `GmailResolveAttachmentsOperator` (`resolve.py:64`) переопределяют `execute`
      без `super().execute()`, а сенсоры (`sensors/gmail.py:226,347`) переопределяют
      `poke`, а не `execute` — чинить больше нечего
- [ ] запустить полный набор: `pytest` (маркер `packaging` deselected)
- [ ] запустить с покрытием: `pytest --cov=airflow_provider_gmail
      --cov-report=term-missing` — не ниже текущего уровня (99%)
- [ ] убедиться, что `pre_execute`/новая ветка покрыты тестами
- [ ] **acceptance-gate на целевом 2.9.1** (механика safeguard в 2.9.1 отличается
      от локальной 2.11.2, см. Context): дождаться зелёного прогона CI-матрицы
      `.github/workflows/tests.yml` (она ставит constraints Airflow 2.9.1), либо
      прогнать локально в constrained-окружении по инструкции из AGENTS.md

### Task 4: [Final] Закрыть план
- [ ] удалить `todo.md` из корня (диагностика перенесена в этот план/CHANGELOG)
- [ ] переместить этот план в `docs/plans/completed/`
- [ ] обновлять `AGENTS.md`/`CLAUDE.md` **не требуется** — разовая особенность
      `ExecutorSafeguard` не является новым общепроектным соглашением (заметку
      вносить только если по ходу реально выявится repo-wide правило)

## Post-Completion
*Требует ручного действия или внешних систем — без чекбоксов, информационно*

**Manual verification:**
- Прогнать `download_to_s3` на реальном Airflow 2.9 и убедиться, что строка
  `cannot be called outside TaskInstance!` больше не появляется в логах таска, а
  манифесты/XCom/downstream остаются корректными.
