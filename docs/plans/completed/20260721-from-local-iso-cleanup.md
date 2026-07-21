# Чистка `from_local_iso`: явный isinstance-гард + сужение except + упрощение naive-проверки

## Overview

Две minor-правки в `from_local_iso` (`src/airflow_provider_gmail/dates.py`),
найденные `/code-review` (2026-07-21) поверх Z-фикса, уточнённые ревью плана
(Claude + codex, 2026-07-21). **Ни одна не является багом** — функция уже
работает корректно; это чистота кода и диагностика. Обе на ветке
`manifest-resolver-uri-xcom` (невыпущенная 0.3.0), уедут вместе с тегом
`v0.3.0`.

1. **Явный `isinstance(value, str)`-гард + сужение `except` до `ValueError`.**
   Чтобы нестроковый вход (`None`/`int`/`bytes`) давал нормализованный
   `ValueError`, в перехват сейчас добавлен `AttributeError`, а исторически
   там же живёт `TypeError`. Обе — широкие ловушки: после явной проверки типа
   `datetime.fromisoformat(<str>)` кидает **только** `ValueError`, поэтому и
   `AttributeError`, и `TypeError` для str-пути недостижимы. Оставлять их —
   значит по-прежнему маскировать будущую опечатку внутри `try` (например
   `datetime.fromisoformatt(...)` → `AttributeError`, или неверные аргументы →
   `TypeError`) под видом ошибки ввода. Явный гард в начале + `except
   ValueError` делает намерение явным и не маскирует будущие ошибки
   программирования (codex-ревью 2026-07-21, CRITICAL #1: НЕ ссылаться на
   `_parse_iso_date` — у него нет type guard, контракт иной).
2. **Упрощение naive-проверки `parsed.tzinfo is None or parsed.utcoffset() is
   None` → `parsed.tzinfo is None`.** `datetime.fromisoformat` возвращает либо
   naive-дату (`tzinfo is None`), либо дату с fixed-offset `timezone` (у
   которого `utcoffset()` — всегда timedelta, никогда `None`; проверено на
   3.10–3.12). Вторая половина `or` недостижима — мёртвая защитная ветка.
   Pre-existing код (не из Z-фикса).

## Context (from discovery)

- Файл: `src/airflow_provider_gmail/dates.py`, функция `from_local_iso`
  (строки ~107-156). Чистый модуль — только `datetime`/`zoneinfo`.
- Единственный вызывающий: `resolve.py::_resolve` (`pick="latest"`), передаёт
  `internal_date`, который `Manifest.from_json` уже валидирует как `str` — то
  есть non-str в проде не доходит; правка #1 сохраняет контракт для прямых
  вызовов функции.
- Тесты: `tests/test_dates.py`. Существующий
  `test_non_str_input_raises_valueerror` (None/123/bytes → `ValueError`)
  зелёный и со СТАРЫМ широким `except`, поэтому он **не** доказывает цель
  рефактора (что внутренний `AttributeError`/`TypeError` больше не
  маскируется). Нужен отдельный targeted-тест (codex-ревью, CRITICAL #2).
- Релиз: код новый в невыпущенной 0.3.0 → отдельная запись в CHANGELOG не
  нужна.

## Development Approach

- **testing approach**: test-first для targeted-теста (он и есть доказательство
  цели рефактора — падает со старым широким `except`, зеленеет после сужения);
  остальное — Regular
- make small, focused changes
- **CRITICAL: все существующие тесты зелёные** — правки обязаны сохранять
  публичный тип и текст ошибки для str-входов и `ValueError` для non-str
- прогон: `pytest` (packaging-маркер деселектится сам)

## Testing Strategy

- **targeted regression test (новый, обязателен)** — доказывает главную цель
  правки #1. **Механизм подмены (важно):** нельзя `monkeypatch.setattr(
  dates.datetime, "fromisoformat", ...)` — `dates.datetime` это stdlib-класс
  `datetime.datetime`, immutable C-type, `setattr` на него падает `TypeError:
  cannot set 'fromisoformat' attribute of immutable type` (plan-review
  2026-07-21). Вместо этого подменять **модульное имя целиком**:
  `monkeypatch.setattr(dates, "datetime", Fake)`, где `Fake` — стенд-ин с
  classmethod/staticmethod `fromisoformat`, кидающим `AttributeError` (второй
  кейс — `TypeError`). Тест проверяет, что из `from_local_iso(<валидный str>)`
  исходный `AttributeError`/`TypeError` **выходит наружу** (а не превращается в
  `ValueError`). Тест **падает** на текущем широком `except (ValueError,
  TypeError, AttributeError)` и **зеленеет** после сужения до `except
  ValueError` — это его смысл (codex-ревью, CRITICAL #2).
- **существующие тесты — регресс-страховка эквивалентности:** non-str
  (None/123/bytes → `ValueError`), naive, malformed, bare-Z, double-offset-Z,
  round-trip остаются зелёными БЕЗ изменения ассертов (правка #1 сохраняет
  публичный тип/текст ошибки; правка #2 — no-op на выходе).
- Полный прогон: `pytest`; покрытие ~99% (`dates.py` 100% — правка #2 убирает
  недостижимую ветку; при line-coverage 100% сохраняется)
- Packaging (`-m packaging`) не трогаем

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview

Итоговый вид тела `from_local_iso` (после докстринга):

```python
original = value
if not isinstance(value, str):
    raise ValueError(
        f"internal_date must be an ISO 8601 timestamp with a UTC offset, "
        f"got {original!r}"
    )
try:
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    parsed = datetime.fromisoformat(value)
except ValueError as exc:
    raise ValueError(
        f"internal_date must be an ISO 8601 timestamp with a UTC offset, "
        f"got {original!r}"
    ) from exc
if parsed.tzinfo is None:
    raise ValueError(
        f"internal_date must be timezone-aware (carry a UTC offset), got "
        f"naive {original!r}"
    )
return parsed
```

- **Правка #1:** добавить `isinstance`-гард ПЕРВЫМ (после `original = value`);
  сузить `except` до **только `ValueError`** (убрать и `AttributeError`, и
  `TypeError` — оба недостижимы для str после гарда, а их сохранение
  противоречит самой цели «не маскировать будущие ошибки»). Текст сообщения
  гарда идентичен except-сообщению (тот же «ISO 8601», `got {original!r}`),
  поэтому `test_non_str_input_raises_valueerror` (`match="ISO 8601"`) остаётся
  зелёным.
- **Правка #2:** `if parsed.tzinfo is None or parsed.utcoffset() is None:` →
  `if parsed.tzinfo is None:`.
- **Docstring:** текущий абзац «Error messages report the caller's original
  input…» остаётся точным и достаточным — **менять не нужно** (codex-ревью,
  MINOR: не оставлять «опциональную» размытую правку).
- **Устаревший комментарий:** комментарий в `test_non_str_input_raises_valueerror`
  сейчас объясняет, что «Z-нормализация не должна выходить из `try`, иначе
  non-str контракт сломается». После явного гарда это уже не причина
  корректности (non-str отсекается раньше) — комментарий обновить, чтобы он
  описывал новый контракт (гард + узкий `except`), не старую реализацию
  (codex-ревью, MAJOR #2).
- **Exception chaining меняется намеренно (не «поведение неизменно»):** старый
  нормализованный `ValueError` нёс `AttributeError`/`TypeError` в `__cause__`;
  новый гард поднимает `ValueError` без `__cause__` для non-str. Сохраняются
  **публичный тип и текст** ошибки (что и проверяют тесты), а цепочка
  исключений для non-str осознанно упрощается (codex-ревью, MINOR).

## Implementation Steps

### Task 1: Гард типа, сужение except и упрощение naive-проверки в from_local_iso

**Files:**
- Modify: `src/airflow_provider_gmail/dates.py`
- Modify: `tests/test_dates.py`

- [x] (test-first) добавить в `tests/test_dates.py` targeted-тест: подменить
  модульное имя `monkeypatch.setattr(dates, "datetime", Fake)` (НЕ
  `dates.datetime.fromisoformat` — immutable C-type, `setattr` упадёт), где
  `Fake.fromisoformat` кидает `AttributeError` — убедиться, что
  `from_local_iso(<валидный str>)` пропускает `AttributeError` наружу (а не
  оборачивает в `ValueError`); аналогичный кейс для `TypeError`. Тест должен
  **падать** на текущем коде (широкий `except`) — зафиксировать это как
  доказательство цели
- [x] правка #1: добавить `if not isinstance(value, str): raise ValueError(...)`
  первой строкой после `original = value`; сузить `except (ValueError,
  TypeError, AttributeError)` до `except ValueError`; текст сообщения гарда
  скопировать **дословно (байт-в-байт)** из `except`-ветки — иначе ассерт
  `match="ISO 8601"` в `test_non_str_input_raises_valueerror` расцепится
  (plan-review 2026-07-21)
- [x] правка #2: заменить `if parsed.tzinfo is None or parsed.utcoffset() is
  None:` на `if parsed.tzinfo is None:`
- [x] обновить устаревший комментарий в `test_non_str_input_raises_valueerror`
  (`tests/test_dates.py`): описать новый контракт (isinstance-гард + узкий
  `except`), убрать упоминание «Z-нормализация не должна выходить из try»
- [x] прогнать `pytest`: новый targeted-тест теперь зелёный; ВСЕ существующие
  тесты `test_dates.py` (Z, naive, malformed, round-trip, non-str, bare-Z,
  double-offset-Z) остаются зелёными БЕЗ изменения ассертов; проверить
  покрытие `pytest --cov=airflow_provider_gmail --cov-report=term-missing`
  (~99%, `dates.py` 100%) — один финальный coverage-прогон

### Task 2: [Final] Подтверждение и перенос плана

- [x] README/README_RU/AGENTS/CONTEXT/CHANGELOG — правок не требуется
  (внутренняя деталь функции; код новый в невыпущенной 0.3.0); подтвердить и
  не менять
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

- Правки входят в невыпущенную **0.3.0** (ветка `manifest-resolver-uri-xcom`),
  уедут с тегом `v0.3.0`; отдельного бампа/релиза не требуют.
- Перенос плана в `completed/` — административное действие финала (в
  `/planning:exec` его выполняет harness), не самостоятельная кодовая задача.
