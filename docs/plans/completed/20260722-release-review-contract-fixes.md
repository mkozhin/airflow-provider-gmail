# Фиксы контрактов 0.3.0 по релизному codex-ревью (#2 validate_prefix, #3 S3 ManifestError во всех путях)

## Overview

Codex-ревью всей дельты релиза (`git diff v0.2.0..HEAD`, 2026-07-22) нашёл три
MAJOR. Пользователь выбрал закрыть **#2 и #3**; **#1 оставить** как
задокументированное решение. Код уже смерджен в `main`, тег `v0.3.0` НЕ
выставлен — это доводка нового-в-0.3.0 кода ДО релиза, а не изменение
выпущенного поведения.

1. **#2 — `validate_prefix` недо-валидирует (`utils/paths.py`).** Проверка
   использует `FORBIDDEN_KEY_CHARS`, спроектированный для **имён файлов**, где
   `\` и управляющие символы уже срезаются basename-шагом/`_CONTROL_CHARS`
   внутри `sanitize_filename`. Но `prefix` через `sanitize_filename` НЕ
   проходит — попадает в объектный ключ дословно. Prefix с `\` или ASCII
   control-символом (перевод строки, таб) проходит валидацию, но ломает
   гарантию «ключи URL-безопасны по построению»: сторонний
   `urlsplit()`/`parse_s3_url` в downstream вырежет перевод строки и получит
   другой ключ. Пример codex: `reports\narchive`.
2. **#3 — не-UTF-8 S3-манифест даёт `UnicodeDecodeError`, а не `ManifestError`
   — в ТРЁХ публичных путях.** `S3Hook.read_key()` = `...read().decode("utf-8")`,
   т.е. декодирует ДО `Manifest.from_json()`. Локальное чтение отдаёт **байты**
   → `from_json` ловит `UnicodeDecodeError` (подкласс `ValueError`) → оборачивает
   в `ManifestError`. Все три S3-чтения получают голый `UnicodeDecodeError` из
   `read_key`:
   - `resolve.py:144` (`resolve_attachments`),
   - `operators/gmail.py:570` (`GmailAttachmentsToS3Operator._read_manifest`),
   - `sensors/gmail.py:343` (`GmailAttachmentToS3Sensor._has_processed_manifest`).
   Это нарушает **общий** контракт README:320 «A corrupt/invalid manifest raises
   a loud `ManifestError`». Пользователь выбрал закрыть **все три пути**
   (codex-ревью плана, CRITICAL #1).

**Вне scope (осознанно):** #1 (`s3_uri` схлопывает `//` в `files[].path`
резолвера) — задокументированное решение (план 0.3.0 + тест
`test_s3_reanchor_collapses_double_slash_in_key`). Не трогаем.

## Context (from discovery)

- `utils/paths.py`: `FORBIDDEN_KEY_CHARS = frozenset('?#%{}^[]<>~|"`')`,
  `validate_prefix(prefix)`. Комментарий у набора указывает, что `\`/`/`
  исключены намеренно ради контракта `sanitize_filename` — это про **имена
  файлов**, не про prefix. `FORBIDDEN_KEY_CHARS` шарится с
  `mime.py::sanitize_filename` — **менять нельзя**. Ужесточение — ТОЛЬКО внутри
  `validate_prefix`.
- Три S3-чтения манифеста имеют одинаковую форму
  `Manifest.from_json(hook.read_key(key, bucket_name=bucket))`:
  `resolve.py:144`, `operators/gmail.py:570`, `sensors/gmail.py:343`. Оператор и
  сенсор перед `read_key` делают `check_for_key` (missing → `None`/`False`);
  резолвер `check_for_key` НЕ делает (missing → `ClientError` наверх, отдельный
  контракт). Проверено (codex): missing → `ClientError` из `obj.get()` ДО
  decode; не-UTF-8 → `UnicodeDecodeError` в `.decode` ПОСЛЕ успешного `get`.
  Значит узкий `except UnicodeDecodeError` не задевает missing-контракт ни в
  одном из путей.
- `Manifest.from_json` (`manifest.py`) — classmethod, принимает `str|bytes`,
  оборачивает битый JSON / не-UTF-8 bytes в `ManifestError`; `manifest.py`
  импортирует только stdlib (airflow/S3 туда НЕ тянем).
- Тесты: `validate_prefix` **прямого** теста в `tests/test_paths.py` НЕ имеет;
  отклонение `#`/`%` покрыто **косвенно** через
  `tests/test_operator_s3.py::test_execute_invalid_rendered_prefix_raises` и
  `tests/test_sensor_s3.py::test_poke_invalid_rendered_prefix_raises` (оба
  `prefix="gmail/a#b"`) — остаются зелёными. Task 1 добавляет первые прямые
  unit-тесты. `tests/test_resolve.py`: фикстура `fake_s3` (`errors`-map уже
  умеет кидать заготовленное исключение — переписывать НЕ нужно); valid-UTF-8-S3
  / local / битый-JSON / `ClientError`-passthrough уже покрыты (строки ~273 /
  302 / 325 / 340) — это **regression**, дубли не писать. `tests/test_manifest.py`
  существует. `tests/test_operator_s3.py`, `tests/test_sensor_s3.py` покрывают
  оператор/сенсор.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next; small focused changes
- **CRITICAL: каждая кодовая задача ОБЯЗАНА содержать тесты** (success + error);
  дубли существующих тестов НЕ писать — ссылаться на них как regression
- **CRITICAL: все тесты зелёные перед следующей задачей**
- **CRITICAL: обновлять этот план при изменении scope**
- прогон: targeted (`pytest tests/<file>.py`) после кодовой задачи, один полный
  coverage-прогон в verify
- maintain backward compatibility (str-prefix без спецсимволов и UTF-8-манифесты
  ведут себя как раньше)

## Testing Strategy

- **#2 (`test_paths.py`)**: `validate_prefix` отвергает `\`, ASCII C0 controls
  на границах `\x00` и `\x1f` (плюс внутренние `\n`/`\r`/`\t`), `\x7f` (DEL) и
  существующие `FORBIDDEN_KEY_CHARS` (`#`,`%`,…); ПРОПУСКАЕТ `/`, пустой
  `prefix=""`, `gmail/avito`, «отрендеренный» `gmail/avito/2026-07-10`;
  сообщение показывает символы через `!r` (control-символы видны)
- **#3 (`test_manifest.py`)**: новый `Manifest.from_s3`: fake-hook,
  `read_key` кидает `UnicodeDecodeError` → `ManifestError`; валидный
  UTF-8-JSON → `Manifest`; hook, кидающий произвольное НЕ-`UnicodeDecodeError`
  (напр. `ClientError`-суррогат), — исключение проходит наверх БЕЗ обёртки
- **#3 (`test_resolve.py` / `test_operator_s3.py` / `test_sensor_s3.py`)**: по
  одному новому тесту на путь — не-UTF-8 S3-манифест (мок `read_key` →
  `UnicodeDecodeError`) → `ManifestError` из `resolve_attachments` /
  `operator.execute` (через `_read_manifest`) / `sensor.poke` (через
  `_has_processed_manifest`); существующие valid/local/JSON/ClientError-тесты —
  regression, не дублировать
- **e2e**: нет — не применимо
- Полный прогон + покрытие `--cov` ~99% — в задаче Verify
- Packaging (`-m packaging`) не трогаем

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview

### #2 — ужесточить `validate_prefix` (не трогая общий `FORBIDDEN_KEY_CHARS`)

`FORBIDDEN_KEY_CHARS` остаётся набором для имён файлов. `validate_prefix`
расширяет проверку прямо в функции: символ «плохой», если он в
`FORBIDDEN_KEY_CHARS` **или** это `\` **или** это ASCII C0 control/`DEL`
(`ord(ch) < 0x20 or ord(ch) == 0x7F`). `/` по-прежнему разрешён.

```python
def validate_prefix(prefix: str) -> None:
    bad = sorted(
        ch for ch in set(prefix)
        if ch in FORBIDDEN_KEY_CHARS or ch == "\\" or ord(ch) < 0x20 or ord(ch) == 0x7F
    )
    if bad:
        raise ValueError(
            f"prefix contains characters not allowed in an object key: "
            f"{''.join(bad)!r} (in {prefix!r}). Remove them from the prefix."
        )
```

- **Терминология (codex MINOR):** отвергаем **ASCII C0 controls (`ord < 0x20`)
  и `DEL` (`0x7F`)**. Unicode C1 (`0x80–0x9F`) НЕ включаем — это осознанно
  консистентно с `sanitize_filename` (`_CONTROL_CHARS` тоже до `127`); докстринг
  формулировать именно так, без размытого «control characters».
- Докстринг `validate_prefix` и комментарий у `FORBIDDEN_KEY_CHARS` обновить:
  набор — для имён файлов; prefix-валидация намеренно шире (`\` + ASCII
  C0/DEL), т.к. prefix не санитизируется; `/` разрешён.
- `sanitize_filename` (`mime.py`) НЕ трогаем.

### #3 — общий `Manifest.from_s3` для всех трёх S3-чтений (единая точка контракта)

Вместо трёх одинаковых inline-обёрток — один classmethod-конструктор рядом с
`from_json`, через который проходят все три места. `manifest.py` остаётся без
airflow/S3-импортов: `hook` принимается **duck-typed** (вызываем только
`.read_key`).

```python
# manifest.py, рядом с from_json
@classmethod
def from_s3(cls, hook, bucket: str, key: str) -> "Manifest":
    """Read + parse an S3 manifest via ``hook.read_key`` (str, UTF-8-decoded).

    A non-UTF-8 body makes ``read_key`` raise ``UnicodeDecodeError`` *before*
    parsing; wrap it into ``ManifestError`` so corrupt content surfaces the same
    way as the local (bytes) path. A missing object raises before the decode
    (S3 ``ClientError``) and is deliberately NOT caught here.
    """
    try:
        raw = hook.read_key(key, bucket_name=bucket)
    except UnicodeDecodeError as exc:
        raise ManifestError(
            f"manifest at s3://{bucket}/{key} is not valid UTF-8: {exc}"
        ) from exc
    return cls.from_json(raw)
```

Три места переводятся на него:
- `resolve.py:144` — `pairs.append((manifest_path, Manifest.from_s3(hook, bucket, key)))`.
- `operators/gmail.py:570` — `return Manifest.from_s3(hook, self.bucket, key)`.
- `sensors/gmail.py:343` — `manifest = Manifest.from_s3(hook, self.bucket, key)`.

- Оператор/сенсор сохраняют предшествующий `check_for_key` (missing → `None`/
  `False`) — не трогаем. Резолвер `check_for_key` не делает — missing даёт
  `ClientError` наверх (не `UnicodeDecodeError`), поэтому узкий catch его не
  трогает.
- **Почему classmethod, а не чтение байтов через `get_key().get()["Body"]`**
  (выбор пользователя): байтовый вариант потребовал бы смены S3-API и
  переписывания фикстур/ассертов; `from_s3` точечен, DRY (одна точка контракта
  на три места), сохраняет `read_key`. `UnicodeDecodeError` ловим узко.

### CHANGELOG (в Task 1, вместе с #2)

`## [0.3.0]` уже несёт breaking-пункт про валидацию `prefix`. Ужесточение #2 —
расширение того же невыпущенного поведения → дополнить существующий пункт, НЕ
добавляя секцию. **ВАЖНО (plan-review):** пункт содержит фразу, что `\`/`/`
«unaffected (basename strips them, `a\b\c.xlsx → c.xlsx`)» — она про **имена
файлов**. Развести области: имена файлов по-прежнему не трогают `\`/`/`
(basename), а отрендеренный **prefix** теперь дополнительно отвергает `\` и
ASCII C0/DEL. #3 — внутренняя доводка, отдельной записи не требует.
Дата `## [0.3.0] - 2026-07-21` — провизорная (тег не выставлен); сверить/
поправить при выставлении тега (Post-Completion).

## What Goes Where

- **Implementation Steps** (`[ ]`): `paths.py`, `manifest.py`, `resolve.py`,
  `operators/gmail.py`, `sensors/gmail.py`, тесты, CHANGELOG.
- **Post-Completion** (без чекбоксов): включение в релиз `v0.3.0` (тег), сверка
  даты changelog.

## Implementation Steps

### Task 1: Ужесточить validate_prefix + CHANGELOG (#2)

**Files:**
- Modify: `src/airflow_provider_gmail/utils/paths.py`, `tests/test_paths.py`,
  `CHANGELOG.md`

- [x] `validate_prefix` (`paths.py`): набор «плохих» = `FORBIDDEN_KEY_CHARS` ∪
  `\` ∪ ASCII C0/`DEL` (`ord < 0x20` или `== 0x7F`); `/` разрешён; сообщение
  через `!r`. НЕ менять `FORBIDDEN_KEY_CHARS`/`sanitize_filename`
- [x] докстринг `validate_prefix` + комментарий у `FORBIDDEN_KEY_CHARS`: набор
  для имён файлов; prefix-валидация шире (`\` + ASCII C0/DEL, т.к. prefix не
  санитизируется); формулировка «ASCII C0 controls и DEL», не «control chars»
- [x] CHANGELOG.md: дополнить существующий `## [0.3.0]` prefix-пункт (prefix
  теперь также `\` и ASCII C0/DEL), развести области имён файлов vs prefix (без
  самопротиворечия), новую секцию НЕ добавлять
- [x] write tests (`test_paths.py`, success): пропускает `""`, `/`,
  `gmail/avito`, `gmail/avito/2026-07-10`
- [x] write tests (`test_paths.py`, error): отвергает `\`, `\x00`, `\x1f`,
  `\n`/`\r`/`\t`, `\x7f`, `#`/`%`; сообщение содержит repr плохих символов
- [x] run tests (`pytest tests/test_paths.py`) - must pass before next task

### Task 2: Общий Manifest.from_s3 + перевод трёх S3-чтений (#3)

**Files:**
- Modify: `src/airflow_provider_gmail/manifest.py`, `tests/test_manifest.py`
- Modify: `src/airflow_provider_gmail/resolve.py`,
  `src/airflow_provider_gmail/operators/gmail.py`,
  `src/airflow_provider_gmail/sensors/gmail.py`
- Modify: `tests/test_resolve.py`, `tests/test_operator_s3.py`,
  `tests/test_sensor_s3.py`

- [x] `manifest.py`: добавить classmethod `Manifest.from_s3(cls, hook, bucket,
  key)` (см. Solution Overview): `try read_key except UnicodeDecodeError → raise
  ManifestError from exc`, затем `from_json`; `hook` duck-typed (без
  airflow/S3-импортов в модуле)
- [x] `manifest.py`: обновить докстринг МОДУЛЯ (строки 3-4, «no Airflow, no S3,
  …») — признать duck-typed S3-read seam `from_s3` (импорты остаются чистыми:
  hook инжектится, S3Hook не импортируется), чтобы «no S3» не противоречил
  новому методу (plan-review Important)
- [x] перевести три места на `Manifest.from_s3(...)`: `resolve.py` (S3-ветка;
  `bucket,key` уже есть из `split_s3_uri`), `operators/gmail.py:_read_manifest`,
  `sensors/gmail.py:_has_processed_manifest`; `check_for_key` в оператора/сенсоре
  и missing-контракт резолвера НЕ менять
- [x] обновить три устаревших докстринга call-site'ов на `from_s3`:
  `operators/gmail.py:562-564` («with check_for_key + read_key … which
  Manifest.from_json accepts»), `sensors/gmail.py:332` («through
  Manifest.from_json»), `resolve.py:116` («Both feed Manifest.from_json»)
  (plan-review Minor)
- [x] write tests (`test_manifest.py`): `from_s3` — fake-hook `read_key` кидает
  `UnicodeDecodeError` → `ManifestError`; валидный UTF-8 → `Manifest`; hook
  кидает не-`UnicodeDecodeError` (суррогат `ClientError`) → проходит наверх
- [x] write tests (по одному новому на путь): `test_resolve.py` — S3 не-UTF-8 →
  `ManifestError` наверх; `test_operator_s3.py` — `execute` при не-UTF-8
  манифесте → `ManifestError`; `test_sensor_s3.py` — `poke` аналогично →
  `ManifestError`. **Техника для operator/sensor (plan-review Minor):**
  предпочесть органичную репродукцию — положить в fake-store невалидные байты
  `b"\xff"` под ключ манифеста (существующий `FakeS3Hook.read_key` делает
  `.decode("utf-8")`, поэтому `UnicodeDecodeError` возникнет сам после
  `check_for_key`), а не конструировать `UnicodeDecodeError` вручную.
  Существующие valid/local/JSON/`ClientError` тесты — regression, дубли не
  писать
- [x] run tests (`pytest tests/test_manifest.py tests/test_resolve.py
  tests/test_operator_s3.py tests/test_sensor_s3.py`) - must pass before next
  task

### Task 3: Verify acceptance criteria

- [x] проверить требования Overview: prefix с `\`/ASCII-control отвергается;
  не-UTF-8 S3-манифест → `ManifestError` во ВСЕХ трёх путях; missing-контракт
  (`ClientError`) и #1 (`//`) не задеты; str-prefix без спецсимволов и
  UTF-8-манифесты — как раньше — подтверждено ревью кода + целевых тестов
  (`test_paths.py`, `test_manifest.py::from_s3`, `test_resolve.py`,
  `test_operator_s3.py`, `test_sensor_s3.py`); `FORBIDDEN_KEY_CHARS` и
  `//`-collapse-тест не тронуты
- [x] полный прогон `pytest` — 474 passed, 1 deselected
- [x] покрытие `pytest --cov=airflow_provider_gmail --cov-report=term-missing`
  — TOTAL 99% (`manifest.py`/`paths.py` 100%)

### Task 4: [Final] Документация и перенос плана

- [x] README/README_RU/AGENTS/CONTEXT — сверить, правок не требуется
  (README:320 контракт «corrupt → ManifestError» теперь честно выполняется во
  всех путях; формулировка не устарела); подтверждено, правок не потребовалось
- [x] CHANGELOG уже обновлён в Task 1 — сверить, что дата `## [0.3.0]`
  помечена как провизорная / подлежит сверке при теге (Post-Completion);
  добавлен провизорный HTML-комментарий у заголовка `## [0.3.0] - 2026-07-21`
- [x] move this plan to `docs/plans/completed/` (orchestrator moves the plan
  after all phases — not moved here)

## Post-Completion

- Фиксы входят в невыпущенную **0.3.0** (`main`), уедут с тегом `v0.3.0`.
- При выставлении тега `v0.3.0` сверить/поправить дату `## [0.3.0] - <date>` в
  CHANGELOG на фактическую дату релиза.
- #1 (`s3_uri` `//`-нормализация) сознательно НЕ фиксится — при желании
  пересмотреть — отдельный план.
