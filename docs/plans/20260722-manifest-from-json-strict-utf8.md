# Строгий UTF-8 в Manifest.from_json — единообразный контракт порчи S3 vs local (codex #2) + нейтральное сообщение from_s3 (codex #1)

## Overview

Codex-ревью дельты 0.3.0 (2026-07-22) нашёл два MINOR по только что
внесённым контракт-правкам (`Manifest.from_s3`):

1. **#2 (главное) — контракт порчи S3 vs local НЕ единообразен для не-UTF-8.**
   `Manifest.from_json` (`manifest.py:132-137`) для **bytes**-входа (локальное
   чтение) вызывает `json.loads(raw)`, а он через `json.detect_encoding`
   авто-распознаёт UTF-16/UTF-32 BOM и **принимает** такой манифест. S3-путь
   же (`read_key` декодирует строго UTF-8 ДО `from_json`) на том же теле даёт
   `UnicodeDecodeError` → `from_s3` оборачивает в `ManifestError` → **отклоняет**.
   Codex доказал эмпирически: локальный манифест, записанный в UTF-16, проходит
   `resolve_attachments(...)` без ошибки. Из-за этого docstring'и `resolve.py:117`,
   `sensors/gmail.py:333`, `operators/gmail.py:565` («broken manifest — invalid
   JSON *or* non-UTF-8 bytes — raises `ManifestError`» в обоих путях) сейчас
   **ложны** для UTF-16/32.

2. **#1 — литерал `s3://` в сообщении `from_s3`** (`manifest.py:240`,
   `f"manifest at s3://{bucket}/{key} is not valid UTF-8: …"`) **нарушает
   задокументированный инвариант**: `utils/paths.py:34` прямо объявляет
   `S3_URI_SCHEME` «the **only** place this literal lives in `src`». Значит
   строка `from_s3` — реальное нарушение, независимо от того, строит она ключ
   или сообщение.

**Выбор пользователя (Approach A) для #2:** привести **local**-путь к строгому
UTF-8 внутри `from_json`, чтобы контракт стал **реально** единообразным (local
не-канонично-UTF-8 — UTF-16/UTF-32 и UTF-8-with-BOM — → `ManifestError`, как в
S3), а существующие docstring'и стали честными. Сам провайдер всегда пишет
манифест в UTF-8 (`to_json` → `.encode("utf-8")`, `manifest.py:122`), поэтому
реальные манифесты не затронуты — меняется лишь обработка подделанного/битого
контента (консистентно с `CONTEXT.md`: «loud failure on corruption»).

**Решение #1 (plan-review codex):** НЕ импортировать `S3_URI_SCHEME` в чистый
`manifest.py` (сломало бы инвариант «no Airflow, no S3, no paths»), а сделать
сообщение **нейтральным, без литерала `s3://`** —
`f"manifest in bucket {bucket!r} at key {key!r} is not valid UTF-8: {exc}"`.
Это реально закрывает #1 (инвариант `paths.py:34` восстановлен), не тянет импорт
и снимает нужду в оправдательном inline-комментарии.

**Конфликт с авторитетной документацией (plan-review codex):** completed master
plan `docs/plans/completed/20260710-airflow-provider-gmail.md:~931` предписывает
для `str` `raw.encode()` перед `json.loads`; `AGENTS.md` называет тот план
авторитетным **для поведения**. Approach A меняет именно эту нормализацию, значит
конфликт надо разрешить **явно** (не просто перенести новый план в `completed/`):
закрепить новый контракт в `CONTEXT.md` и оставить в master plan
supersede-пометку (Task 4).

## Context (from discovery)

- `manifest.py`:
  - `from_json(cls, raw: str | bytes)` (строки 124-137): `str` → `.encode("utf-8")`;
    `bytes` → сразу `json.loads(raw)` (вот здесь авто-распознавание UTF-16/32/BOM).
    `except (json.JSONDecodeError, ValueError, TypeError)` → `ManifestError`
    («manifest is not valid JSON: …»).
  - `from_s3(cls, hook, bucket, key)` (строки 221-242): `try read_key except
    UnicodeDecodeError → ManifestError(<сообщение с s3://>)`, затем `from_json`.
    **Остаётся нужным** и после Approach A: для S3 не-UTF-8 падает в `read_key`
    (декодирование), ДО `from_json`, поэтому узкий catch в `from_s3` не избыточен.
    Сообщение — переписать на нейтральное (см. #1).
  - `to_json` (122): всегда UTF-8 без BOM → провайдерские манифесты валидны.
  - Модуль чистый: `import enum, json, dataclasses, typing.Any` — S3/airflow/пути
    НЕ импортируются (инвариант). `S3_URI_SCHEME` живёт в `utils/paths.py:34-35`
    и объявлен там единственным носителем литерала `s3://` в `src`.
- Три call-site уже используют `from_s3`; их docstring'и (`resolve.py:116-117`,
  `sensors/gmail.py:332-333`, `operators/gmail.py:563-565`) утверждают
  единообразность — под Approach A становятся **истинными** (проверено
  plan-review: править не нужно, только сверить).
- Тесты (`tests/test_manifest.py`):
  - `test_str_and_bytes_inputs_equivalent` (строки 116-120) — валидный
    UTF-8-`str`/`bytes`, regression.
  - `from_s3`-набор (строки ~233-289): валидный / не-UTF-8 (моделируется
    `b"\xff".decode("utf-8")` → `UnicodeDecodeError` в fake-hook) → `ManifestError`
    / прочая ошибка проходит / битый JSON.
  - **ВАЖНО (правка codex):** прямого **local** bytes-теста `from_json(b"\xff")`
    здесь НЕТ — `b"\xff"` фигурирует лишь в моделировании S3-decode-fail. Значит
    прямой тест local bytes-ветки нового сообщения — **обязателен и новый**, без
    оговорки «если уже есть».
- `tests/test_resolve.py`: `test_broken_local_manifest_raises_manifest_error`,
  `test_non_utf8_s3_manifest_raises_manifest_error`,
  `test_missing_s3_manifest_propagates_without_wrapping` — regression, не
  дублировать. НЕТ теста на локальный UTF-16/32 → добавить (это и есть новое
  поведение).
- `tests/test_operator_s3.py` / `tests/test_sensor_s3.py`: не-UTF-8 через
  `b"\xff"` в fake-store — regression.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next; small focused changes
- **CRITICAL: каждая кодовая задача ОБЯЗАНА содержать тесты** (success + error);
  дубли существующих тестов НЕ писать — ссылаться как regression
- **CRITICAL: тесты нового поведения должны РЕАЛЬНО пинить его** — для не-UTF-8
  из local bytes-ветки ассертить `match="not valid UTF-8"`, а не только тип
  `ManifestError` (тип проходит и на старом коде для `b"\xff"`)
- **CRITICAL: все тесты зелёные перед следующей задачей**
- **CRITICAL: обновлять этот план при изменении scope**
- прогон: targeted (`pytest tests/<file>.py`) после кодовой задачи; полный
  coverage-прогон в Verify
- maintain backward compatibility: канонические BOM-less UTF-8-манифесты
  (единственные, что пишет `to_json`) ведут себя как раньше; меняется только
  отклонение не-канонично-UTF-8 контента — UTF-16/UTF-32 **и** UTF-8-with-BOM
  (`utf-8-sig`) — теперь единообразно на обоих путях

## Testing Strategy

- **unit (`test_manifest.py`)**:
  - **error, local bytes-ветка** (пинит новую ветку через `match`): `from_json`
    на **bytes** в UTF-16 (`.encode("utf-16")`), UTF-32 (`.encode("utf-32")`) и
    **UTF-8-with-BOM** (`.encode("utf-8-sig")`) → `pytest.raises(ManifestError,
    match="not valid UTF-8")` для UTF-16/32 и для `b"\xff"` (raw-байты);
    UTF-8-with-BOM (bytes) и str-аналог `"﻿" + json.dumps(...)` → `ManifestError`
    (ведущий BOM → `JSONDecodeError`-ветка → сообщение «not valid JSON» — это ок,
    тип пинить, `match` не навязывать). Опционально: `exc.__cause__` —
    `UnicodeDecodeError` для raw-байт.
  - **from_s3 нейтральное сообщение**: не-UTF-8 через fake-hook →
    `pytest.raises(ManifestError, match="not valid UTF-8")` **и** ассерт
    `"s3://" not in str(exc)` (закрепляет #1: литерал ушёл; сообщение содержит
    bucket/key).
  - **success**: валидный BOM-less UTF-8-`bytes` и валидный `str` парсятся как
    прежде — regression на `test_str_and_bytes_inputs_equivalent`, добавить только
    недостающее.
- **unit (`test_resolve.py`)**: локальный манифест в UTF-16 на диске →
  `resolve_attachments([...])` поднимает `ManifestError` (новый; парный к
  существующему S3-кейсу, доказывает единообразность). Существующие
  local-broken / S3-non-UTF-8 / missing — regression.
- **e2e**: нет — не применимо
- Полный прогон + `--cov` ~99% — в Verify
- Packaging (`-m packaging`) не трогаем

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview

### #2 — строгая UTF-8-проверка в `from_json` (bytes-ветка)

Единственная точка, где решается кодировка чтения, — `from_json`. Для
**bytes**-входа декодируем строго UTF-8 ДО `json.loads`, оборачивая
`UnicodeDecodeError` в `ManifestError`; **str**-вход парсится напрямую (без
прежнего `.encode()`-round-trip).

**Полная поверхность изменения:** меняется НЕ только UTF-16/UTF-32. Прежний
`json.loads(bytes)` через `detect_encoding` принимал (а) UTF-16/UTF-32 (по BOM) и
(б) **UTF-8-with-BOM** (`utf-8-sig`, BOM молча срезался); прежний str-путь тоже
пропускал ведущий `﻿` (`.encode()` → снова `utf-8-sig` детект → срез). Новый код
на обоих путях BOM не срезает: ведущий `﻿` доживает до `json.loads(str)` →
`JSONDecodeError` → `ManifestError`. Итог: любой не-канонично-UTF-8 контент
(UTF-16, UTF-32 **и** UTF-8-with-BOM) отклоняется единообразно на S3 и local.
Реальные манифесты не затронуты — `to_json` пишет чистый UTF-8 без BOM.

```python
@classmethod
def from_json(cls, raw: str | bytes) -> "Manifest":
    """Parse+validate a manifest → ManifestError on any violation.

    ``str`` (S3, already decoded) is parsed directly; ``bytes`` (local read)
    must be canonical BOM-less UTF-8 — a non-UTF-8 body raises ManifestError,
    matching the S3 path. A leading BOM is rejected (json's UTF-16/32/BOM
    auto-detection is deliberately not relied on)."""
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ManifestError(f"manifest is not valid UTF-8: {exc}") from exc
    else:
        text = raw
    try:
        obj: Any = json.loads(text)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
    ...  # остальная валидация без изменений
```

- `from_s3` по логике НЕ меняется (S3 не-UTF-8 всё ещё ловится в `read_key`, ДО
  `from_json`); его docstring про «which the local bytes path would have surfaced
  as a ManifestError via from_json» становится буквально верным.

### #1 — нейтральное сообщение `from_s3` (без `s3://`)

`from_s3` ловит `UnicodeDecodeError` из `read_key` и оборачивает в `ManifestError`
с сообщением **без** литерала `s3://` (инвариант `paths.py:34`):

```python
raise ManifestError(
    f"manifest in bucket {bucket!r} at key {key!r} is not valid UTF-8: {exc}"
) from exc
```

- Убирает нарушение инварианта, сохраняет диагностический контекст (bucket/key),
  не требует импорта `utils.paths` и inline-оправданий.
- Сообщения после правок: local не-UTF-8 → «manifest is not valid UTF-8: …»;
  S3 не-UTF-8 → «manifest in bucket … at key … is not valid UTF-8: …». Оба —
  `ManifestError`, оба явно про UTF-8.
- **Смена класса сообщения (для сведения):** локальный `b"\xff"` раньше давал
  `ManifestError("manifest is not valid JSON: …")` (`UnicodeDecodeError` — подкласс
  `ValueError`), теперь — `"manifest is not valid UTF-8: …"`. Тесты, ассертящие
  подстроку, — только на **новую** формулировку.

### docstring'и call-site'ов — сверить, не переписывать (в Task 4)

`resolve.py:116-117`, `sensors/gmail.py:332-333`, `operators/gmail.py:563-565`
уже говорят «invalid JSON *or* non-UTF-8 bytes → ManifestError» для обоих путей.
Под Approach A это истина — оставить; поправить ТОЛЬКО если найдётся формулировка,
привязанная к старому «json.loads принимает bytes как есть».

### Разрешение конфликта с master plan (в Task 4)

`CONTEXT.md` (описание `Manifest`) — закрепить новый контракт: `from_json`
принимает `str | bytes`, но `bytes` обязаны быть каноничным BOM-less UTF-8;
любая порча (не-UTF-8, BOM, битый JSON, нарушение схемы) → единый `ManifestError`,
единообразно для S3 и local. В master plan
(`completed/20260710-…:~931`) добавить короткую supersede-пометку, что деталь
«`raw.encode()` при `str`» заменена строгим bytes-декодом этим планом (файл
остаётся историческим, но без «висящего» устаревшего предписания).

## What Goes Where

- **Implementation Steps** (`[ ]`): `manifest.py` (`from_json` + сообщение
  `from_s3`), `tests/test_manifest.py`, `tests/test_resolve.py`; в финале —
  `CONTEXT.md`, master plan supersede-пометка, сверка docstring'ов, перенос плана.
- **Post-Completion** (без чекбоксов): включение в невыпущенную 0.3.0 (тег
  `v0.3.0`), сверка даты CHANGELOG при теге.

## Implementation Steps

### Task 1: Строгий UTF-8 в from_json + нейтральное сообщение from_s3 (#2 + #1)

**Files:**
- Modify: `src/airflow_provider_gmail/manifest.py`, `tests/test_manifest.py`

- [x] `from_json` (`manifest.py`): для `bytes`-входа декодировать строго UTF-8
  (`raw.decode("utf-8")`) в `try/except UnicodeDecodeError → ManifestError("manifest
  is not valid UTF-8: …") from exc` ДО `json.loads`; `str`-вход парсить напрямую
  (убрать прежний `.encode("utf-8")`-round-trip); остальная schema-валидация без изменений
- [x] `from_json` docstring: кратко — `str` парсится напрямую; `bytes` обязаны
  быть каноничным BOM-less UTF-8; ведущий BOM отклоняется; любая порода порчи →
  `ManifestError`; контракт единообразен с S3 (без разбора внутренней механики
  `json.loads`)
- [x] `from_s3` (`manifest.py:240`): заменить сообщение на нейтральное **без**
  `s3://` — `f"manifest in bucket {bucket!r} at key {key!r} is not valid UTF-8:
  {exc}"`; логику (`try read_key except UnicodeDecodeError`) не менять
- [x] write tests (`test_manifest.py`, error, local bytes-ветка): UTF-16
  (`.encode("utf-16")`) и UTF-32 → `pytest.raises(ManifestError, match="not valid
  UTF-8")`; `b"\xff"` → `pytest.raises(ManifestError, match="not valid UTF-8")`
  (обязательный НОВЫЙ тест — прямого local-кейса не было); UTF-8-with-BOM bytes
  (`.encode("utf-8-sig")`) и str-аналог `"﻿" + <json-str>` → `ManifestError` (тип)
- [x] write tests (`test_manifest.py`, from_s3): не-UTF-8 через fake-hook (напр.
  `bucket="reports"`, `key="k"`) → `pytest.raises(ManifestError, match="not valid
  UTF-8")`; ОБЯЗАТЕЛЬНО ассертить, что сообщение сохранило контекст объекта —
  `"reports" in str(exc)` **и** `"k" in str(exc)` (repr) — **и** `"s3://" not in
  str(exc)` (закрепляет #1: контекст есть, литерал схемы ушёл)
- [x] write tests (`test_manifest.py`, success): валидный BOM-less UTF-8-`bytes`
  и `str` парсятся как прежде — regression на `test_str_and_bytes_inputs_equivalent`,
  добавить только недостающее
- [x] run tests (`pytest tests/test_manifest.py`) — must pass before next task

### Task 2: Парный resolver regression (local UTF-16 → ManifestError)

**Files:**
- Modify: `tests/test_resolve.py`

- [x] write test (`test_resolve.py`): взять **полностью валидный** манифест
  (существующий sample-хелпер файла / `Manifest.build(...).to_json().decode("utf-8")`),
  записать его на диск в UTF-16 (`path.write_bytes(<valid-json-str>.encode("utf-16"))`)
  — чтобы изолировать проверку кодировки от schema-валидации — →
  `resolve_attachments([str(path)])` поднимает `pytest.raises(ManifestError,
  match="not valid UTF-8")` (парный к существующему S3-non-UTF-8 тесту;
  существующие local-broken / S3 / missing — regression, не дублировать)
- [x] run tests (`pytest tests/test_resolve.py tests/test_manifest.py`) — must
  pass before next task

### Task 3: Verify acceptance criteria

- [x] проверить требования Overview: local не-канонично-UTF-8-манифест
  (UTF-16/UTF-32 и UTF-8-with-BOM) → `ManifestError` (как S3); каноничные
  BOM-less UTF-8-манифесты и `str`-вход — как раньше; missing-контракт
  (`ClientError`) не задет; инвариант чистоты `manifest.py` сохранён (нет импорта
  путей/S3); **#1 закрыт** — в `manifest.py` больше нет литерала `s3://`
  (`grep -n 's3://' src/airflow_provider_gmail/manifest.py` → пусто). Полная
  сверка инварианта `paths.py:34`: `rg -n --fixed-strings 's3://' src` — все
  совпадения допустимы, только если это (а) `S3_URI_SCHEME`/его использования в
  `utils/paths.py` либо (б) докстринги/комментарии; **построения URI руками
  (f-string/format с `s3://…`) вне `utils/paths.py` быть не должно** (именно это
  чинит #1)
  — VERIFIED: `grep s3:// manifest.py` пусто; `rg s3:// src` — все совпадения
  докстринги/комментарии + `S3_URI_SCHEME`/использования в `paths.py`; ни одной
  ручной сборки URI в f-string/format вне `paths.py` (URI строятся через `s3_uri()`).
  Импорты `manifest.py`: только `enum, json, dataclasses, typing.Any` — путей/S3
  нет. UTF-16/UTF-32/UTF-8-BOM (local) → ManifestError, str/BOM-less UTF-8 без
  изменений, missing `ClientError` не задет — покрыто зелёными тестами
- [x] полный прогон `pytest` — 483 passed, 1 deselected (packaging), 9 warnings
- [x] покрытие `pytest --cov=airflow_provider_gmail --cov-report=term-missing` —
  TOTAL 99% (manifest.py 100%)

### Task 4: [Final] Документация, разрешение конфликта, перенос плана

**Files:**
- Modify: `CONTEXT.md`, `docs/plans/completed/20260710-airflow-provider-gmail.md`
- Inspect (правки лишь при неточности): `src/airflow_provider_gmail/resolve.py`,
  `src/airflow_provider_gmail/sensors/gmail.py`, `src/airflow_provider_gmail/operators/gmail.py`,
  `README.md`, `README_RU.md`, `CHANGELOG.md`

- [x] `CONTEXT.md` (описание `Manifest`): закрепить контракт — `from_json`
  принимает `str | bytes`, `bytes` обязаны быть каноничным BOM-less UTF-8; любая
  порча (не-UTF-8/BOM/битый JSON/схема) → единый `ManifestError`, единообразно
  S3 и local (это авторитетное описание нового поведения)
  — DONE: расширен пункт `from_json(raw: str | bytes)` в описании модуля `Manifest`
  (строгий BOM-less UTF-8 для `bytes`, `str` напрямую, единый `ManifestError`).
- [x] master plan (`completed/20260710-…`, пункт про `from_json` / `raw.encode()`):
  добавить короткую пометку «➕ superseded планом 20260722-manifest-from-json-strict-utf8:
  `bytes` строго-UTF-8-декодируются, `str` парсится напрямую» — снять устаревшее
  предписание, не переписывая исторический файл
  — DONE: supersede-пометка добавлена сразу после предписания `raw.encode()` (строка ~932).
- [x] Inspect docstring'и call-site'ов (`resolve.py:116-117`, `sensors/gmail.py:332-333`,
  `operators/gmail.py:563-565`) **и самого `manifest.py`** (`from_s3`/`from_json`,
  строки ~221-242 / ~124-137): под строгим UTF-8 фраза `from_s3` «which the local
  bytes path would have surfaced as a ManifestError via from_json» стала полностью
  истинной; `from_json` docstring уже обновлён в Task 1 — сверить непротиворечивость;
  править ТОЛЬКО при фактической неточности
  — VERIFIED: все docstring'и («invalid JSON *or* non-UTF-8 bytes → ManifestError»)
  теперь фактически верны; `from_json`/`from_s3` в manifest.py непротиворечивы. Правок нет.
- [x] Inspect `README.md` / `README_RU.md` / `CHANGELOG.md`: подтвердить, что
  формулировка «corrupt manifest → `ManifestError`» теперь честно и единообразно
  выполняется; правок не требуется (новая секция CHANGELOG НЕ нужна — доводка
  невыпущенной 0.3.0); поправить только при фактической неточности
  — VERIFIED: «broken/corrupt manifest raises `ManifestError`» (README, README_RU,
  CHANGELOG) выполняется честно и единообразно. Правок нет.
- [x] финальный test-gate: `pytest` — подтвердить, что правки докстрингов в
  `.py` (Inspect-набор) не сломали импорт/сборку; должно быть зелено
  — DONE: `.venv/bin/pytest` → 483 passed, 1 deselected (packaging), 9 warnings.
- [x] move this plan to `docs/plans/completed/` (orchestrator moves the plan after
  all phases — not moved here)

## Post-Completion

- Фиксы входят в невыпущенную **0.3.0** (`main`), уедут с тегом `v0.3.0`.
- При выставлении тега `v0.3.0` сверить/поправить дату `## [0.3.0] - <date>` в
  CHANGELOG на фактическую дату релиза.
- Осознанно НЕ делаем: импорт `S3_URI_SCHEME` в `manifest.py` (нарушил бы
  инвариант чистоты — вместо этого сообщение сделано нейтральным); поведение
  каноничных UTF-8-манифестов не меняется.
