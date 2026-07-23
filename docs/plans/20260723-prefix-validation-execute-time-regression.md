# Регресс валидации prefix: валидировать на execute-времени через seam `_run` (вариант B)

## Обзор

Продолжение плана `20260723-suppress-nested-execute-warning.md` (уже влит в эту
ветку). Прошлый фикс убрал ложный WARNING
`GmailAttachmentsToS3Operator.execute cannot be called outside TaskInstance!`,
перенеся валидацию отрендеренного `prefix` из переопределённого `execute()` в
`pre_execute()`. Адверсариальное ревью codex нашло, что это внесло узкий, но
реальный **регресс**.

**Регресс.** Порядок lifecycle в `TaskInstance` (проверено по `taskinstance.py`,
2.11.2):

```
render_templates (3141) → pre_execute (3164) → on_execute_callback (3167) → execute (3185)
```

До прошлого фикса `validate_prefix` работал в **начале `execute()`** (3185) —
т.е. **после** `on_execute_callback`. После него валидация в `pre_execute` (3164)
— **до** `on_execute_callback`. Значит `on_execute_callback`, изменивший
`self.prefix` на URL-небезопасное значение, теперь **проскакивает валидацию**:
`execute()` пишет URL-небезопасные S3-ключи и возвращает некорректный URI, молча
ослабляя гарантию «ключи URL-safe by construction» (ADR-0007). codex воспроизвёл:
callback, ставящий `prefix = "gmail/a#b"` после `pre_execute`, дал ключи вида
`gmail/a#b/dt=.../a.xlsx`.

**Фикс (вариант B).** Восстановить валидацию на execute-времени, не возвращая
вложенный `super().execute()`, который и вызывал WARNING: вынести тело базовой
оркестрации в protected-seam `_run(context)`; базовый `execute` становится тонким
`return self._run(context)`; `GmailAttachmentsToS3Operator` переопределяет
`execute` на `validate_prefix(self.prefix)` затем `return self._run(context)`.
Валидация снова на execute-времени (после `on_execute_callback`), вложенного
вызова нет (сейфгард молчит — `_run` им не оборачивается), а `pre_execute`
убирается. Это ровно «вариант B», рассматривавшийся в исходном брейншторме;
находка codex его оправдывает.

**Ключевой бенефит:** инвариант ADR-0007 восстановлен до до-регрессной силы **и**
ложный WARNING остаётся устранён.

## Context (from discovery)

- Файлы:
  - `src/airflow_provider_gmail/operators/gmail.py` — базовый
    `GmailAttachmentsBaseOperator.execute` (сейчас вся оркестрация, стр. 308–~433);
    `GmailAttachmentsToS3Operator.pre_execute` (493–508, добавлен прошлым фиксом);
    базовый `execute` также наследуется `GmailAttachmentsToLocalOperator` (тот его
    не переопределяет).
  - `src/airflow_provider_gmail/utils/paths.py:70` — docstring `validate_prefix`,
    изменён прошлым фиксом на `pre_execute()`; вернуть на `execute()`/`poke()`.
  - `src/airflow_provider_gmail/sensors/gmail.py:360` — строка паритета в docstring
    `poke`, изменена на `pre_execute()`; вернуть на `execute()`.
  - `tests/test_operator_s3.py` — тестовые изменения прошлого фикса (хелпер `_run`,
    переименования, hook-тест, sentinel-warning-тест).
  - `CHANGELOG.md` — запись `[Unreleased] / Fixed` (стр. 3–24) описывает уже
    отменённый перенос `execute()`→`pre_execute()`; переписать.
- Факты lifecycle (установлен Airflow 2.11.2, target 2.9.1 через constraints):
  `on_execute_callback` выполняется между `pre_execute` и `execute`
  (`taskinstance.py:3164 → 3167 → 3185`). `ExecutorSafeguard` оборачивает только
  методы с именем `execute` в namespace класса (`BaseOperatorMeta.__new__`), значит
  protected `_run` не оборачивается никогда, а `execute` подкласса, зовущий
  `self._run(...)` (не `super().execute(...)`), несёт sentinel от `TaskInstance` и
  молчит. `_run` работает на execute-времени, после callback'а — то же окно, что и
  исходная валидация в начале `execute`.
- У local-оператора нет `prefix` и нет валидации — он просто наследует тонкий
  базовый `execute` → `_run`. Сенсор валидирует в `poke()` (не меняется).

## Development Approach

- **testing approach**: Regular (правка кода → обновление/добавление тестов в той
  же атомарной задаче).
- Правка кода + все её тесты + CHANGELOG — **одна атомарная задача** (Task 1): репо
  не должно оставаться в непроверяемом состоянии между шагами.
- **CRITICAL: тесты обязательны** — проверить: (a) отсутствие ложного WARNING
  (паритет с гарантией прошлого фикса); (b) отрендеренный `prefix` всё ещё
  валидируется fail-fast; (c) **регресс закрыт** — `prefix`, изменённый после
  `pre_execute` (как это сделал бы `on_execute_callback`), ловится в `execute`.
- **CRITICAL: все тесты зелёные** перед завершением задачи.
- Обратная совместимость: контракт штатного выполнения через `TaskInstance` не
  меняется (сигнатура/возврат `execute`, `AirflowSkipException`,
  XCom/манифесты/dedup). Тело базовой оркестрации переезжает в `_run` дословно —
  логика не меняется.

## Testing Strategy

- **unit tests** (`pytest` через `.venv` проекта): обязательны в Task 1.
  - **Регресс-тест (суть изменения):** сконструировать
    `GmailAttachmentsToS3Operator` с **валидным** `prefix`, задать
    `op.hook = FakeGmailHook([...])` (детерминизм — без реального подключения),
    вызвать `op.pre_execute(ctx)` (теперь no-op из `BaseOperator` — делает сценарий
    «мутация **после** pre_execute» точным и соответствующим имени теста), затем
    изменить `op.prefix = "gmail/a#b"` (имитация `on_execute_callback`, меняющего
    его после `pre_execute`), затем ассертить `op.execute(...)` →
    `ValueError(match="prefix")`. Падает на до-фиксной (валидация-в-`pre_execute`)
    форме и проходит после варианта B — mutation-sensitive.
  - **Тест на отсутствие WARNING** (`test_execute_logs_no_warning_when_invoked_with_sentinel`):
    сохранить negative-control (голый `execute` без sentinel → WARNING есть),
    `caplog.clear()`, затем sentinel-ветка (`execute(ctx, **{f"{type(op).__name__}__sentinel": _sentinel}`) → WARNING нет). Убрать устаревшие вызовы
    `op.pre_execute(...)` (S3 больше не переопределяет `pre_execute`; валидация в
    `execute`). Предпосылка: `unit_test_mode = False` (проверено) — тест не
    вакуумный.
  - **Тесты валидации prefix:** переименовать обратно
    `test_pre_execute_invalid_rendered_prefix_raises` /
    `test_pre_execute_valid_prefix_passes` →
    `test_execute_invalid_rendered_prefix_raises` /
    `test_execute_valid_prefix_passes`; вернуть тестовый хелпер `_run` к прямому
    вызову `op.execute(...)` (валидация снова в `execute`); вернуть комментарий в
    `test_templated_prefix_does_not_fail_at_construction` на `execute()`.
  - **Удалить** `test_user_pre_execute_hook_runs_before_prefix_validation` — он
    проверял свойство варианта A (валидация внутри override `pre_execute` после
    `super().pre_execute()`); вариант B `pre_execute` не переопределяет, тест
    больше не описывает реальное поведение.
- **e2e tests**: в проекте нет — неприменимо.
- Регресс всего пакета: `pytest` + покрытие ≥ 99%.

## Progress Tracking

- отмечать `[x]` сразу; новые задачи — `➕`; блокеры — `⚠️`.

## Solution Overview

Ввести protected orchestration-seam, чтобы storage-подкласс мог валидировать свой
отрендеренный конфиг **на execute-времени** без override `execute`, зовущего
`super().execute()`:

- `GmailAttachmentsBaseOperator`: переименовать текущее тело `execute` в
  `_run(self, context)`; добавить тонкий `execute(self, context) -> list[str]`,
  возвращающий `self._run(context)`.
- `GmailAttachmentsToS3Operator`: заменить `pre_execute` на override `execute`,
  зовущий `validate_prefix(self.prefix)` затем `return self._run(context)`.
- `GmailAttachmentsToLocalOperator`: без изменений (наследует тонкий базовый
  `execute`).

Почему это чинит обе проблемы: `_run` не назван `execute`, поэтому
`ExecutorSafeguard` его не оборачивает → нет ложного WARNING от внутреннего вызова;
а валидация стоит в начале `execute` (execute-время, после `on_execute_callback`)
→ гарантия ADR-0007 восстановлена. `_run` — намеренно protected seam (зовётся
подклассом), а не строго private.

## Technical Details

- База (`operators/gmail.py`), заменить заголовок `execute` на стр. 308:
  ```python
  def execute(self, context: Any) -> list[str]:
      """Тонкий делегат к :meth:`_run`.

      Оставлен однострочником, чтобы storage-подкласс мог валидировать свой
      отрендеренный конфиг *перед той же оркестрацией*, переопределив ``execute``
      и вызвав :meth:`_run` — НЕ ``super().execute()``. Вложенный
      ``super().execute()`` не несёт sentinel'а ``ExecutorSafeguard`` и логирует
      ложный "... .execute cannot be called outside TaskInstance!" warning
      (Airflow 2.9+). ``_run`` сейфгардом не оборачивается, поэтому внутренний
      вызов молчит.
      """
      return self._run(context)

  def _run(self, context: Any) -> list[str]:
      """<существующий docstring execute + тело дословно>"""
      ...
  ```
- S3 (`operators/gmail.py`), заменить `pre_execute` (493–508) на:
  ```python
  def execute(self, context: Any) -> list[str]:
      """Валидируем отрендеренный ``prefix``, затем базовая оркестрация.

      ``prefix`` — template-поле, валидируется на **отрендеренном** значении
      (паритет с ``date_from``/``date_to``, ADR-0004) — не в ``__init__``, где
      шаблон ``{{ ds }}`` споткнётся о проверку ``{``/``}``. Валидация на
      ``execute``-времени — после ``pre_execute`` и ``on_execute_callback`` —
      поэтому поздняя мутация ``prefix`` всё ещё ловится, ключи остаются
      URL-safe by construction (ADR-0007). Зовёт :meth:`_run` (не
      ``super().execute()``), поэтому вложенный вызов не роняет
      ``ExecutorSafeguard`` (Airflow 2.9+) ложным warning'ом.
      """
      validate_prefix(self.prefix)
      return self._run(context)
  ```
- Тело оркестрации не меняется — переезжает в `_run` дословно.

## What Goes Where

- **Implementation Steps** (`[ ]`): рефактор оператора + override S3 + откат
  docstring'ов + тесты + CHANGELOG (одна атомарная задача), затем верификация и
  закрытие плана (не-implementation задачи, тестов не требуют).
- **Post-Completion** (без чекбоксов): ручная проверка на реальном Airflow 2.9;
  обязательная зелёная CI-матрица 2.9.1 (authoritative acceptance gate).

## Implementation Steps

### Task 1: Вынести базовый `_run`, валидировать `prefix` в `execute` S3, откатить docstring'и, обновить тесты и CHANGELOG (атомарно)

**Files:**
- Modify: `src/airflow_provider_gmail/operators/gmail.py`
- Modify: `src/airflow_provider_gmail/utils/paths.py`
- Modify: `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `tests/test_operator_s3.py`
- Modify: `CHANGELOG.md`

Реализация:
- [ ] В `GmailAttachmentsBaseOperator`: переименовать тело текущего `execute`
      (стр. 308) в `def _run(self, context)` (docstring + тело дословно); добавить
      тонкий `def execute(self, context) -> list[str]: return self._run(context)` с
      docstring-делегатом (см. Technical Details)
- [ ] В `GmailAttachmentsToS3Operator`: заменить `pre_execute` (493–508) на override
      `execute`: `validate_prefix(self.prefix)` затем `return self._run(context)`
      (docstring по Technical Details)
- [ ] Проверить, что `validate_prefix` остаётся импортированным/используемым;
      `GmailAttachmentsToLocalOperator` по-прежнему наследует тонкий базовый
      `execute` (правок там не нужно)
- [ ] `utils/paths.py:70` — вернуть docstring `validate_prefix` к «at the top of
      `execute()` / `poke()`»
- [ ] `sensors/gmail.py:360` — вернуть строку паритета `poke` к «parity with the S3
      operator's `execute()`»

Тесты (в этой же задаче):
- [ ] Вернуть тестовый хелпер `_run` (сейчас `pre_execute` → `execute`) к
      `return op.execute(context or _context())`, и **убрать его устаревший
      комментарий** про «Mirror the TaskInstance lifecycle order
      (pre_execute → execute)…» (tests/test_operator_s3.py:181-182)
- [ ] Переименовать `test_pre_execute_invalid_rendered_prefix_raises` /
      `test_pre_execute_valid_prefix_passes` →
      `test_execute_invalid_rendered_prefix_raises` /
      `test_execute_valid_prefix_passes`; вернуть комментарий в
      `test_templated_prefix_does_not_fail_at_construction` на `execute()`
- [ ] Удалить `test_user_pre_execute_hook_runs_before_prefix_validation` (проверял
      свойство только варианта A; реального поведения больше не описывает)
- [ ] Обновить `test_execute_logs_no_warning_when_invoked_with_sentinel`: убрать
      вызовы `op.pre_execute(...)` (S3 больше не переопределяет `pre_execute`);
      сохранить negative-control + `caplog.clear()` + sentinel-ветку
- [ ] **Добавить регресс-тест** `test_execute_revalidates_prefix_mutated_after_pre_execute`:
      op с валидным `prefix` + `op.hook = FakeGmailHook([...])`, затем
      `op.pre_execute(ctx)` (no-op из BaseOperator), затем `op.prefix = "gmail/a#b"`,
      ассертить `op.execute(...)` → `ValueError(match="prefix")` (закрывает находку
      codex — имитирует мутацию из `on_execute_callback` после `pre_execute`)
- [ ] CHANGELOG (`[Unreleased] / ### Fixed`, стр. 3–24): переписать запись —
      ложный WARNING `execute cannot be called outside TaskInstance!` убран
      вынесением базовой оркестрации в protected `_run`, который S3-оператор зовёт
      напрямую (вместо `super().execute()`); валидация `prefix` **остаётся на
      `execute`-времени** (после `on_execute_callback`), так что гарантия
      URL-safe-ключей ADR-0007 не изменилась относительно поведения `0.3.0`. НЕ
      ссылаться на перенос в `pre_execute()` (тот промежуточный подход отменён).
      Блок `[0.3.0]` не трогать; English / Keep-a-Changelog
- [ ] Запустить `.venv/bin/python -m pytest tests/test_operator_s3.py
      tests/test_operator_base.py tests/test_operator_local.py` — должно быть
      зелёным перед Task 2 (base/local прогоняют перенесённое тело оркестрации через
      унаследованный тонкий `execute` → `_run`, поэтому должны остаться зелёными;
      полный набор — в Task 2)

### Task 2: Проверка приёмочных критериев (verification)
- [ ] Grep `src/` на **исполняемый** вызов `return super().execute(` (или
      `super().execute(` вне docstring'ов) — ожидать ZERO вызовов в коде.
      Текстовые упоминания `super().execute()` внутри docstring'ов допустимы и
      ожидаемы — не считать их за находку
- [ ] Подтвердить, что `pre_execute` больше не определён в
      `GmailAttachmentsToS3Operator`
- [ ] Полный набор: `.venv/bin/python -m pytest` — все проходят
- [ ] Покрытие: `.venv/bin/python -m pytest --cov=airflow_provider_gmail
      --cov-report=term-missing` — не ниже 99%; новый seam `_run`/`execute` и
      регресс-тест покрыты
- [ ] **2.9.1 acceptance-gate** — механика сейфгарда в 2.9.1 отличается от
      локальной 2.11.2, поэтому authoritative-проверка — только зелёная CI-матрица
      `.github/workflows/tests.yml` (ставит constraints Airflow 2.9.1). Локально
      `.venv` = 2.11.2, downgrade не делать. Пока CI не зелёная, ставить этому
      пункту `⚠️` (не `[x]`) — это открытый блокер до мержа

### Task 3: [Final] Закрыть план
- [ ] Переместить план в `docs/plans/completed/` (делает харнесс при exec-прогоне;
      иначе — закрывающим коммитом). Отражает локальную готовность; ветку **не
      мержить**, пока `⚠️` 2.9.1-gate (Task 2) не станет зелёным на CI
- [ ] Обновлять `AGENTS.md`/`CLAUDE.md` **не требуется** — `_run`-seam это локальный
      выбор структуры оператора, а не общепроектное соглашение

## Post-Completion
*Ручное / внешнее — без чекбоксов*

**Ручная проверка:**
- На реальном Airflow 2.9 убедиться, что `download_to_s3` не логирует
  `cannot be called outside TaskInstance!`, и что DAG с `on_execute_callback`,
  меняющим `prefix` на URL-небезопасное, теперь падает fail-fast с `ValueError`
  (а не пишет небезопасные ключи).

**Внешнее (блокер мержа):**
- Дождаться **зелёной** CI-матрицы 2.9.1 (`.github/workflows/tests.yml`) после
  пуша — это authoritative acceptance gate (internals сейфгарда в 2.9.1 и локальной
  2.11.2 различаются). До этого ветку не мержить.
