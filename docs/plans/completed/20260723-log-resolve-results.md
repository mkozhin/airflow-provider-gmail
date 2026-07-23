# Логировать результат резолва в логи Airflow (сколько манифестов → какие файлы)

## Обзор

`GmailResolveAttachmentsOperator.execute` (`operators/resolve.py:64-67`) —
однострочник `return resolve_attachments(...)`, в логах таска **ничего нет**: не
видно ни сколько манифестов было на входе, ни режим `pick`, ни какие файлы
получились на выходе. Симметрично проблеме download-оператора (решается отдельным
планом `20260723-log-download-results.md` — этот план его **не** трогает).

**Что делаем.** Добавить INFO-логирование **только в самом операторе**, вокруг
вызова `resolve_attachments`: одна сводка (число входных манифестов + `pick` +
число вложений на выходе) и затем каждый получившийся путь по одной строке.

**Бенефит:** по логам сразу видно, как отработал резолвер — сколько манифестов
пришло, в каком режиме, и какие именно полные пути вложений ушли в XCom (то, что
downstream и получит). Поведение оператора не меняется.

## Context (from discovery)

- `src/airflow_provider_gmail/operators/resolve.py`:
  - `GmailResolveAttachmentsOperator.execute` (стр. 64-67) — тонкий враппер над
    `resolve_attachments`; логов нет.
  - Вход: `self.manifests` (пути манифестов из XCom download-оператора), `self.pick`
    (`"all"`/`"latest"`). Выход: плоский список полных путей вложений (→ XCom).
- `src/airflow_provider_gmail/resolve.py` — **чистый** модуль (`resolve_attachments`
  I/O-фасад + pure `_resolve` core). По дизайну без логов/I/O в ядре
  (`_resolve` — pure). **Не трогаем**: логи идут только в операторе.
  - Оператор видит лишь итоговый плоский список — он **не** знает, какой манифест
    «победил» при `pick="latest"` (это внутри `_resolve`). Поэтому логируем
    вход/выход/`pick`, без «кто победил».
- Тесты: `tests/test_operator_resolve.py` (операторные; фикстура `fake_s3`, тесты
  `test_execute_returns_full_uri_list`, `test_execute_empty_input_returns_empty`).
  `tests/test_resolve.py` — тесты чистого ядра (логов там не будет, не трогаем).
- Конвенции: `%`-стиль логов (как в `operators/gmail.py:348/408`); docs/plans на
  русском; CHANGELOG на английском (Keep-a-Changelog).

## Development Approach

- **testing approach**: Regular (код + тесты в той же атомарной задаче).
- Изменение — только `self.log.info(...)` в `execute`; логика/возврат/исключения не
  трогаются, чистый модуль `resolve.py` не трогается.
- **CRITICAL: тесты обязательны** — через `caplog` проверить сводку и per-path
  строки, а также пустой выход.
- **CRITICAL: все тесты зелёные**, покрытие ≥ 99%.
- Обратная совместимость: возвращаемый список / XCom / исключения не меняются.

## Testing Strategy

- **unit tests** (`pytest` через `.venv`; тесты ставят `caplog.at_level(logging.INFO)`).
  Проверять **точную структуру**: отфильтровать записи логгера оператора и сравнить
  **упорядоченный** список `record.getMessage()` (сводка первой, затем ровно по одной
  записи на путь), а не «`caplog` содержит подстроку»:
  - **Резолв N>1 манифестов → M>1 путей** (на базе `fake_s3`): первая запись —
    сводка с числом входа, `pick`, числом выхода; далее ровно `len(result)` записей,
    по одной на путь, в порядке `result`.
  - **Error-path (обязательно):** при `ValueError` (unknown `pick`) /
    `ManifestError` (битый манифест) — исключение пробрасывается, и **ни сводки, ни
    per-path записей нет** (логи стоят после вызова). Расширить существующие
    `test_unknown_pick_propagates_value_error` / `test_manifest_error_propagates`
    через `caplog`.
  - **Непустой вход → пустой результат (`N>0 → 0`):** `pick="latest"`, победивший
    манифест с `files=[]` → сводка `... → 0 attachment(s)` при `N>0`, строк с путями
    нет. Этот же тест подтверждает отображение `pick=latest`.
  - **Пустой вход** (`manifests=[]`) → сводка `→ 0 attachment(s)`, путей нет.
  - **Недоверенный путь с `\n`/control-символом** (замокать `resolve_attachments`
    вернуть путь с `\n`): в per-path записи (через `record.getMessage()`) переноса
    нет — `%r` экранировал.
  - Чистое ядро (`tests/test_resolve.py`) не затрагивается — логов там нет.
- **e2e**: нет — неприменимо.
- Полный `pytest` + покрытие ≥ 99%.

## Progress Tracking

- `[x]` сразу; новые задачи — `➕`; блокеры — `⚠️`.

## Solution Overview

В `GmailResolveAttachmentsOperator.execute`: вызвать `resolve_attachments`, затем
залогировать сводку (`len(self.manifests)` + `self.pick` + `len(result)`) и каждый
путь результата отдельной INFO-строкой. Всё storage-agnostic — пути в `result` уже
`s3://…` или абсолютные. Ничего в исполнении/возврате не меняется.

## Technical Details

- `execute` (`operators/resolve.py`):
  ```python
  def execute(self, context: Any) -> list[str]:
      result = resolve_attachments(
          self.manifests, pick=self.pick, aws_conn_id=self.aws_conn_id
      )
      self.log.info(
          "Resolved %d manifest(s) (pick=%s) → %d attachment(s).",
          len(self.manifests), self.pick, len(result),
      )
      for path in result:
          self.log.info("  %r", path)   # %r — путь недоверенный (чужой манифест)
      return result
  ```
- **`%r` для пути (не `%s`)**: `files[].path` из манифеста недоверенный
  (`Manifest.from_json` проверяет только тип `str`; доменный контракт допускает
  чужие манифесты), путь может содержать `\n`/`\r`/control-символы. `repr` их
  экранирует → «один путь — одна строка» гарантирована, подделка записей
  невозможна. `result` при этом **не** меняется (экранирование только в логе).
- Уровень INFO. `%`-стиль. Пустой `result` → только сводка `→ 0 attachment(s)`.
- Формат сообщения (глиф `→` U+2192) выбрать один и тот же в коде и в ассерте;
  тесты сравнивают **точное** значение записи (`record.getMessage()`), а не подстроку.
- На ошибке `resolve_attachments` (`ManifestError`/missing manifest) исключение
  бросается **до** логов — оно и так громкое, отдельно не ловим/не логируем.

## What Goes Where

- **Implementation Steps** (`[ ]`): логирование в `execute` + тесты (одна атомарная
  задача), CHANGELOG, верификация, закрытие плана.
- **Post-Completion** (без чекбоксов): визуальная проверка логов резолвера на
  реальном Airflow.

## Implementation Steps

### Task 1: INFO-логирование результата в `GmailResolveAttachmentsOperator.execute` + тесты + CHANGELOG (атомарно)

**Files:**
- Modify: `src/airflow_provider_gmail/operators/resolve.py`
- Modify: `tests/test_operator_resolve.py`
- Modify: `CHANGELOG.md`

Реализация:
- [x] В `execute`: `result = resolve_attachments(...)`, затем INFO-сводка
      `"Resolved %d manifest(s) (pick=%s) → %d attachment(s)."`
      (`len(self.manifests)`, `self.pick`, `len(result)`), затем цикл
      `self.log.info("  %r", path)` по `result`, затем `return result`. **`%r`**
      для пути (недоверенный). Чистый модуль `resolve.py` не трогать; поведение/
      возврат/исключения не меняются

Тесты (в этой же задаче, `caplog.at_level(logging.INFO)`; сравнивать **упорядоченный**
список `record.getMessage()` записей логгера оператора):
- [x] **N>1 → M>1** (`fake_s3`): первая запись — сводка (число входа, `pick`, число
      выхода); далее ровно `len(result)` per-path записей в порядке `result`
- [x] **Error-path (обязательно):** `test_unknown_pick_propagates_value_error` и
      `test_manifest_error_propagates` расширить `caplog`-проверкой: исключение
      пробрасывается, **ни сводки, ни per-path записей нет**
- [x] **N>0 → 0**: `pick="latest"`, победитель с `files=[]` → сводка `... → 0
      attachment(s)` при `N>0`, путей нет (заодно проверяет `pick=latest`)
- [x] **Пустой вход** (`manifests=[]`) → сводка `→ 0 attachment(s)`, путей нет
- [x] **Недоверенный путь с `\n`** (замокать возврат `resolve_attachments`): в
      per-path записи переноса нет (`%r` экранировал)
- [x] CHANGELOG (`[Unreleased]`, English, Keep-a-Changelog): **создать подсекцию
      `### Added`** и поставить её **выше** существующей `### Fixed` (порядок
      Keep-a-Changelog); запись: `GmailResolveAttachmentsOperator` теперь логирует на
      INFO сводку (входные манифесты + `pick` + число вложений) и получившиеся пути.
      Блок `[0.3.0]` не трогать. **Координация:** соседний план
      `20260723-log-download-results.md` тоже добавляет буллет в `[Unreleased]/Added`
      — если он выполнится раньше, **дописать** свой буллет, не перезаписывать секцию
- [x] Запустить `.venv/bin/python -m pytest tests/test_operator_resolve.py
      tests/test_resolve.py` — зелёный перед Task 2

### Task 2: Verify acceptance criteria
- [x] Проверить требования из Overview: сводка + per-path строки присутствуют,
      пустой выход даёт `→ 0`, `pick` в сводке, формулировки storage-agnostic
- [x] Полный набор: `.venv/bin/python -m pytest` — все проходят
- [x] Покрытие: `.venv/bin/python -m pytest --cov=airflow_provider_gmail
      --cov-report=term-missing` — не ниже 99%; новые строки логирования покрыты
- [x] Убедиться, что чистое ядро (`tests/test_resolve.py`) и прочие тесты не сломаны
- [x] **Gate:** и полный `pytest`, и покрытие ≥99% должны пройти **до** Task 3

### Task 3: [Final] Закрыть план
- [x] Переместить план в `docs/plans/completed/` (перемещает харнесс на шаге
      завершения exec)
- [x] README/README_RU и AGENTS.md/CLAUDE.md — обновлять **не требуется**
      (наблюдаемость логов, не публичный API/поведение)

## Post-Completion
*Ручное / внешнее — без чекбоксов*

**Ручная проверка:**
- Прогнать таск с `GmailResolveAttachmentsOperator` на реальном Airflow и глазами
  убедиться, что в логах видно сводку (входные манифесты + `pick` + число вложений)
  и список получившихся путей.
