# Кросс-версионная чистка sentinel: не падать на Airflow 2.9.1

## Обзор

Финальное независимое codex-ревью ветки нашло **MAJOR**, ломающий целевую CI.

Регресс-раунд варианта B добавил в `test_execute_logs_no_warning_when_invoked_with_sentinel`
(`tests/test_operator_s3.py:289`) чистку thread-local утечки:

```python
ExecutorSafeguard._sentinel.callers.pop(f"{type(op_b).__name__}__sentinel", None)
```

Эта чистка корректна на локальном Airflow **2.11.2**, где `ExecutorSafeguard`
ведёт thread-local `_sentinel.callers`. Но на **целевом 2.9.1** (constraints, CI-
матрица `.github/workflows/tests.yml`) у `ExecutorSafeguard` **нет атрибута
`_sentinel`**: там sentinel просто извлекается из kwargs по ключу
`{ClassName}__sentinel` и сравнивается с модульным `_sentinel` — никакого
thread-local-учёта. Следствие: строка 289 на 2.9.1 бросит `AttributeError` и
**завалит весь тест-раунд на всех трёх Python в CI-матрице**.

Ирония: правка под 2.11-утечку сломала 2.9.1 — ровно тот версионный разрыв,
из-за которого 2.9.1-gate держали открытым (`⚠️`).

**Фикс.** Сделать чистку **защищённой**: обращаться к `_sentinel.callers` через
`getattr`, чистить только когда механика присутствует. На 2.9.1 — no-op (утечки
там нет вовсе), на 2.11 — реальная чистка. Один и тот же тест-файл проходит на
обеих версиях.

**Бенефит:** снимается CI-блокер `⚠️` 2.9.1-gate — тест перестаёт зависеть от
версионно-специфичного атрибута, оставаясь осмысленным на обеих версиях.

## Context (from discovery)

- `tests/test_operator_s3.py`:
  - строка 21: `from airflow.models.baseoperator import ExecutorSafeguard` (нужен
    для `getattr`-guard, остаётся).
  - строка 20: `from airflow.models.base import _sentinel` (модульный sentinel —
    существует и в 2.9.1, и в 2.11; остаётся).
  - строки 285-289: комментарий + падающая на 2.9.1 строка чистки.
- Механика по версиям (подтверждено codex по исходникам 2.9.1 и прогоном на 2.11.2):
  - 2.9.1 `ExecutorSafeguard.decorator`: `kwargs.pop(f"{self.__class__.__name__}__sentinel", None)`
    → сравнение с `_sentinel`; **нет** `cls._sentinel` / `callers`. Утечки нет.
  - 2.10+/2.11: добавлен thread-local `_sentinel = local()` c `.callers` для nested-
    operators; sentinel-вызов оставляет запись, которую не pop'ает — отсюда утечка,
    ради которой чистка и добавлена.
  - Прочие части no-warning-теста (импорт `_sentinel`, kwarg `{ClassName}__sentinel`,
    negative-control) валидны на обеих версиях — их не трогаем.
- Локально `.venv` = 2.11.2; 2.9.1 локально не поставить без возмущения окружения
  (см. AGENTS.md). Поведение на 2.9.1 проверяется конструкцией фикса (getattr → no-op)
  + зелёной CI-матрицей.

## Development Approach

- **testing approach**: Regular (правка + проверка в той же задаче).
- Изменение микроскопическое (одна тестовая строка → защищённый блок); держать
  минимальным, без правок production-кода.
- **CRITICAL: полный `pytest` зелёный** на локальном 2.11.2 (чистка по-прежнему
  срабатывает, поведение теста не меняется).
- Обратная совместимость: production-код не трогаем; меняется только внутритестовая
  чистка, чтобы не падать там, где механики нет.

## Testing Strategy

- **unit tests** (`pytest` через `.venv` 2.11.2):
  - Полный прогон `tests/test_operator_s3.py` — `test_execute_logs_no_warning_when_invoked_with_sentinel`
    и весь файл зелёные (на 2.11 guard истинен → чистка выполняется как прежде).
  - **Обязательный** микротест версионной безопасности хелпера (единственный
    локально-исполнимый прокси для 2.9.1-пути, чья непроверяемость и пропустила
    исходный MAJOR): под `monkeypatch.delattr(ExecutorSafeguard, "_sentinel", raising=False)`
    вызвать `_clear_safeguard_sentinel(...)` и убедиться, что он **не бросает**
    (имитирует 2.9.1 без thread-local механики). Тест зовёт реальный хелпер, поэтому
    защищает от будущего рефактора, который снова введёт незащищённое обращение к
    `_sentinel`. Важно: тестировать **только хелпер**, не полный `execute()` — под
    снятым `_sentinel` декоратор самого Airflow 2.11 упал бы внутри себя, а не в
    нашем коде.
- **e2e**: нет — неприменимо.
- Полный `pytest` + покрытие ≥ 99%.
- **Целевой 2.9.1**: authoritative-проверка — зелёная CI-матрица; локально не
  воспроизводится.

## Progress Tracking

- `[x]` сразу; новые задачи — `➕`; блокеры — `⚠️`.

## Solution Overview

Заменить прямое обращение `ExecutorSafeguard._sentinel.callers.pop(...)` на
защищённое через `getattr`, чтобы отсутствие thread-local механики (2.9.1) было
безопасным no-op, а её наличие (2.11) — реальной чисткой. Логика no-warning-теста
в остальном не меняется.

## Technical Details

Вынести guard в **единый module-level хелпер** (чтобы защищённое обращение жило в
одном месте, а обязательный микротест проверял реальный код, а не копию выражения):
```python
def _clear_safeguard_sentinel(op):
    # ExecutorSafeguard keeps thread-local `callers` bookkeeping only on Airflow
    # 2.10+ (2.11 here); on the target 2.9.1 it has no `_sentinel` attribute and
    # stores nothing, so there is no leak to clear. Clear only where the mechanism
    # exists — a bare `ExecutorSafeguard._sentinel` access would AttributeError on
    # 2.9.1 and break the CI matrix. On 2.11 a later bare S3 execute() would
    # otherwise consume this leftover sentinel and be wrongly silenced.
    callers = getattr(getattr(ExecutorSafeguard, "_sentinel", None), "callers", None)
    if callers is not None:
        callers.pop(f"{type(op).__name__}__sentinel", None)
```
В `test_execute_logs_no_warning_when_invoked_with_sentinel` заменить инлайновую
чистку (строки 285-289) на вызов `_clear_safeguard_sentinel(op_b)` (короткий
комментарий-ссылку на хелпер оставить).
- Импорты (строки 20-21) не меняются. Production-код не трогается.

## What Goes Where

- **Implementation Steps** (`[ ]`): защитить строку чистки, прогнать тесты; затем
  verification и закрытие плана.
- **Post-Completion** (без чекбоксов): зелёная 2.9.1 CI-матрица (снимает открытый
  `⚠️`-gate предыдущего плана).

## Implementation Steps

### Task 1: Защитить чистку sentinel через getattr (2.9.1-совместимость)

**Files:**
- Modify: `tests/test_operator_s3.py`

- [x] Добавить module-level хелпер `_clear_safeguard_sentinel(op)` с getattr-guard
      (см. Technical Details); заменить инлайновую чистку (строки 285-289) на вызов
      `_clear_safeguard_sentinel(op_b)`
- [x] Убедиться, что импорты `_sentinel` (стр. 20) и `ExecutorSafeguard` (стр. 21)
      остаются (хелпер использует `ExecutorSafeguard`); production-код не тронут
- [x] Добавить **обязательный** микротест
      `test_clear_safeguard_sentinel_is_noop_without_thread_local(monkeypatch)`:
      `monkeypatch.delattr(ExecutorSafeguard, "_sentinel", raising=False)`, затем
      `_clear_safeguard_sentinel(_make_op({}))` — не должно бросать (имитация 2.9.1)
- [x] Запустить `.venv/bin/python -m pytest tests/test_operator_s3.py` — зелёный
      (на 2.11 guard истинен → чистка выполняется, поведение теста прежнее; микротест
      проходит)

### Task 2: Verify acceptance criteria
- [ ] Grep: в `tests/test_operator_s3.py` нет незащищённого `ExecutorSafeguard._sentinel`
      (единственное обращение — через `getattr(..., "_sentinel", None)` внутри
      `_clear_safeguard_sentinel`)
- [ ] Полный набор: `.venv/bin/python -m pytest` — все проходят
- [ ] Покрытие: `.venv/bin/python -m pytest --cov=airflow_provider_gmail
      --cov-report=term-missing` — не ниже 99%
- [ ] **2.9.1 CI-gate**: authoritative только на CI (`.github/workflows/tests.yml`).
      Локально не воспроизвести (`.venv`=2.11.2, downgrade не делаем). Отметить
      `⚠️` до зелёной CI — это снимаемый пушем блокер

### Task 3: [Final] Закрыть план
- [ ] Переместить план в `docs/plans/completed/` (харнесс при exec; иначе —
      закрывающим коммитом). Ветку не мержить, пока 2.9.1 CI не зелёная
- [ ] `AGENTS.md`/`CLAUDE.md` — обновление не требуется (правка только в тест-файле,
      не новое общепроектное соглашение)
- [ ] `CHANGELOG.md` — запись **не требуется**: правка только в тест-файле
      (`tests/test_operator_s3.py`), production-код не затронут и пользовательское
      поведение не меняется; фикс варнинга уже описан в `[Unreleased]`

## Post-Completion
*Ручное / внешнее — без чекбоксов*

**Внешнее (снятие блокера):**
- После пуша дождаться **зелёной** CI-матрицы 2.9.1 (`.github/workflows/tests.yml`,
  Python 3.10/3.11/3.12). Именно этот прогон подтверждает, что `AttributeError`
  устранён и весь фикс (варианты + эта совместимость) корректен на целевом рантайме.
  Это закрывает открытый `⚠️` 2.9.1-gate из плана
  `20260723-prefix-validation-execute-time-regression.md`.
