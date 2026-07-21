# Фикс: `from_local_iso` не читает суффикс `Z` (Zulu/UTC) на Python 3.10

## Overview

`from_local_iso` (`src/airflow_provider_gmail/dates.py`) парсит манифестный
`internal_date` обратно в aware `datetime` для сравнения по моменту времени в
режиме `pick="latest"` резолвера. Парсинг идёт через `datetime.fromisoformat`,
который **до Python 3.11 не принимает суффикс `Z`** (ISO 8601 Zulu = UTC). Проект
поддерживает `requires-python = ">=3.10"`, поэтому на 3.10 значение вида
`2026-07-10T09:00:00Z` — полностью timezone-aware UTC-момент — падает в
`pick="latest"` с вводящим в заблуждение `ValueError` «internal_date must be
timezone-aware … naive», хотя вход НЕ naive.

**Проблема латентная, а не текущая:** собственный писатель манифеста
`to_local_iso` (`.isoformat()` на zoneinfo-дате) всегда эмитит числовой offset
(`+03:00`, `+00:00`) и **никогда** не пишет `Z` — поэтому все манифесты,
созданные этим провайдером, безопасны на любом Python. Экспозиция — только
чужой/рукописный/будущий манифест с `Z`, прочитанный резолвером (который
позиционируется как «официальный клиент контракта манифеста»). Находка
`/code-review` (2026-07-21, замечание #1); прошлые фазы ревью и codex её
пропустили.

**Решение (вариант A, выбран пользователем 2026-07-21):** нормализовать хвостовой
`Z` в `+00:00` перед `fromisoformat`. Резолвер начинает честно читать оба
написания UTC на всех поддерживаемых версиях Python, а неверное сообщение «naive»
для реального UTC-момента исчезает. Философию «этот модуль владеет форматом
`internal_date`» решение не нарушает — просто делает чтение устойчивым к
стандартному синониму UTC.

## Context (from discovery)

- Файл-владелец формата: `src/airflow_provider_gmail/dates.py` —
  `to_local_iso` (писатель, offset-форма) и `from_local_iso` (читатель,
  добавлен в 0.3.0). Модуль намеренно «чистый»: только `datetime`/`zoneinfo`,
  без Airflow/S3/Gmail.
- Единственный вызов `from_local_iso` — в `resolve.py::_resolve`, ветка
  `pick="latest"` (внутри `max(..., key=lambda ...)`), режим `pick="all"` дату
  не читает. Поведение остальных вызовов не меняется — правка изолирована в
  `from_local_iso`.
- Тесты: `tests/test_dates.py` (чистые, без моков; round-trip, naive, malformed).
- Контракт `to_local_iso`/`from_local_iso` — обратная пара; round-trip
  инвариант уже покрыт тестом.
- Релиз: код `from_local_iso` — новый в **0.3.0**, ветка `manifest-resolver-uri-xcom`
  ещё не смержена и не зарелижена, тег `v0.3.0` не выставлен. Это фикс
  нового-в-0.3.0 кода ДО релиза, а не регрессия выпущенного поведения.

## Development Approach

- **testing approach**: Regular (код фикса, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: каждая задача с изменением кода ОБЯЗАНА содержать новые/обновлённые
  тесты** — success и error сценарии
- **CRITICAL: все тесты зелёные перед следующей задачей**
- **CRITICAL: обновлять этот план при изменении объёма**
- прогон после изменения: `pytest` (packaging-маркер деселектится сам)

## Testing Strategy

- **unit tests**: обязательны; чистый `tests/test_dates.py`, без моков,
  реальный `from_local_iso`/`to_local_iso` на строковых входах
- **e2e tests**: в проекте нет — не применимо
- Полный прогон: `pytest`; покрытие:
  `pytest --cov=airflow_provider_gmail --cov-report=term-missing` (держим ~99%)
- Packaging-тесты (`-m packaging`) не трогаем — упаковка не меняется
- **Версионная независимость фикса:** нормализация выполняется ДО
  `fromisoformat`, поэтому тест зелёный и на локальном 3.12, и на floor 3.10 —
  CI на любой поддерживаемой версии валидирует поведение (отдельный прогон на
  3.10 не требуется)

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

- **Точка правки:** первая строка тела `from_local_iso`, ДО `try:
  datetime.fromisoformat(value)`. Если `value.endswith("Z")` — заменить
  хвостовой `Z` на `+00:00`: `value = f"{value[:-1]}+00:00"`. Далее парсинг и
  существующие проверки без изменений.
- **Только заглавная `Z`:** ISO 8601 и `datetime.fromisoformat` (3.11+)
  используют заглавный `Z`; строчный `z` невалиден и намеренно НЕ нормализуется
  (не расширяем контракт за пределы стандарта — мусор должен падать `ValueError`).
- **Malformed не маскируется:** нормализация — простая замена суффикса; строка
  вроде `"not-a-dateZ"` станет `"not-a-date+00:00"` и всё равно упадёт
  `ValueError` в `fromisoformat` (проверка «ISO 8601»). Naive-строки не
  оканчиваются на `Z` (Z и есть признак UTC), поэтому их путь не затрагивается.
- **Существующий контракт сохраняется:** offset-форма (`+00:00`, `+03:00`)
  парсится как раньше; round-trip `from_local_iso(to_local_iso(...))` не
  меняется; `ManifestError` по-прежнему НЕ используется (значение, не схема).
- **Докстринг** `from_local_iso` дополняется абзацем про нормализацию `Z` и
  мотив (floor 3.10 + отсутствие ложного «naive»).
- **CHANGELOG:** отдельная запись НЕ нужна — `from_local_iso` новый в 0.3.0 и
  ещё не выпущен, это доводка нового кода до релиза, а не изменение
  выпущенного поведения (сверить/подтвердить в задаче verify).

## Technical Details

- Сигнатура и возврат не меняются: `from_local_iso(value: str) -> datetime`
  (aware).
- Изменение локально в теле функции; ни один вызывающий (`resolve.py::_resolve`)
  не затрагивается.
- Edge-кейсы, которые обязаны продолжать работать:
  - `...:00Z` → UTC, `utcoffset()` == 0, равен написанию `...:00+00:00`
  - `...:00.123456Z` (микросекунды + Z) → парсится
  - `09:00:00Z` == `12:00:00+03:00` как момент времени (не лексикографически)
  - naive без offset → `ValueError` (как раньше)
  - malformed (в т.ч. оканчивающийся на `Z`, голый `"Z"`) → `ValueError`
  - строчный `z` (`...09:14:22z`) → `ValueError` (границу не расширяем)

## What Goes Where

- **Implementation Steps** (`[ ]`): правка `from_local_iso`, тесты, докстринг —
  всё в этом репозитории.
- **Post-Completion** (без чекбоксов): включение фикса в релиз `v0.3.0`
  (тег/merge ветки) — вне объёма этой правки.

## Implementation Steps

### Task 1: Нормализация суффикса `Z` в `from_local_iso`

**Files:**
- Modify: `src/airflow_provider_gmail/dates.py`
- Modify: `tests/test_dates.py`

- [x] в `from_local_iso` (`dates.py`) первой строкой тела, ДО
  `datetime.fromisoformat`, добавить нормализацию хвостового заглавного `Z`:
  `if value.endswith("Z"): value = f"{value[:-1]}+00:00"` (строчный `z` НЕ
  трогаем — невалиден по ISO 8601, пусть падает `ValueError`)
- [x] дополнить докстринг `from_local_iso` абзацем: почему нормализуем `Z`
  (floor `>=3.10`, `fromisoformat` научился `Z` только в 3.11; свой
  `to_local_iso` пишет offset, но чужой манифест может нести `Z`) и что это
  убирает ложное сообщение «naive» для реального UTC
- [x] write tests (success, `test_dates.py`): `...T09:14:22Z` парсится как
  aware-UTC, `utcoffset().total_seconds() == 0`, равен
  `from_local_iso("...T09:14:22+00:00")`; `09:00:00Z` ==
  `12:00:00+03:00` как один момент; микросекунды + `Z`
  (`...T09:14:22.123456Z`) парсятся
- [x] write tests (edge/error, `test_dates.py`): malformed-строка,
  оканчивающаяся на `Z` (`"not-a-dateZ"`) и голый `"Z"`, по-прежнему →
  `ValueError` (нормализация не маскирует мусор — plan-review #2); строчный
  `z` (`"2026-07-10T09:14:22z"`) → `ValueError` (фиксируем границу «только
  заглавная `Z`», чтобы будущий рефактор вроде `rstrip("Z")` /
  case-insensitive не расширил контракт молча — plan-review #1); подтвердить,
  что существующие naive/malformed/round-trip тесты остаются зелёными без
  правок
- [x] run tests - must pass before next task

### Task 2: Verify acceptance criteria

- [x] проверить, что все требования Overview выполнены: `Z` читается, ложное
  «naive» ушло, offset-форма и round-trip не изменились, `ManifestError` не
  затронут
- [x] проверить edge-кейсы из Technical Details (микросекунды+Z, момент через
  разные offset'ы, naive/malformed по-прежнему падают)
- [x] сверить CHANGELOG: подтвердить, что отдельная запись не нужна
  (`from_local_iso` новый в невыпущенной 0.3.0) — если решено иначе, добавить
  строку в существующую секцию `## [0.3.0]` и отметить это здесь
- [x] прогнать полный набор: `pytest`
- [x] проверить покрытие: `pytest --cov=airflow_provider_gmail
  --cov-report=term-missing` (держим ~99%, `dates.py` — 100%)

### Task 3: [Final] Update documentation

- [x] README.md / README_RU.md — правок не требуется (внутренняя деталь
  парсинга, не пользовательский контракт); подтвердить и не менять
- [x] AGENTS.md / CONTEXT.md — новых паттернов/терминов нет; подтвердить и не
  менять
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

*Информационно, без чекбоксов — внешние действия*

**Включение в релиз:**
- Фикс входит в невыпущенную **0.3.0** (ветка `manifest-resolver-uri-xcom`).
  Отдельного релиза/бампа версии не требует — уедет вместе с тегом `v0.3.0`
  при мерже/публикации ветки.
