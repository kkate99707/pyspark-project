# PySpark Project — CI/CD

Проект с автоматическим деплоем PySpark-джобы на локальный Spark-кластер в Docker через GitHub Actions.

## Как это устроено

```
git push → GitHub Actions → self-hosted runner (ваш ПК) → Docker Compose → Spark кластер → результат в логах
```

- **Репозиторий** — хранит код (`main.py`, `docker-compose.yml`, `.github/workflows/deploy.yml`)
- **Self-hosted runner** — служба Windows на этом компьютере, слушает GitHub и выполняет задачи локально
- **Workflow** (`deploy.yml`) — сценарий: очистить старые контейнеры → поднять кластер → скопировать код → запустить job

## Как работать с проектом

1. Меняете `main.py` (или другие файлы) как обычно, локально
2. Коммитите и пушите:
   ```powershell
   git add .
   git commit -m "описание изменений"
   git push
   ```
3. Открываете вкладку **Actions** на GitHub — смотрите прогресс и логи выполнения
4. Результат (`df.show()` и всё, что печатает скрипт) — в логах последнего шага **"Run Spark job"**

## Перед началом работы — проверить, что всё готово

**Служба runner'а должна быть запущена:**
```powershell
Get-Service "actions.runner.*"
```
Если `Stopped`:
```powershell
Start-Service "actions.runner.*"
```

**Docker Desktop должен быть запущен** — иначе `docker compose up -d` в пайплайне упадёт.

## Структура репозитория

```
PySparkProject1/
├── main.py                        # основной PySpark-скрипт
├── docker-compose.yml             # spark-master + 2 воркера
├── .gitignore                     # исключает .venv, __pycache__ и т.п.
└── .github/
    └── workflows/
        └── deploy.yml             # CI/CD пайплайн
```

## Если нужно добавить новый файл, который использует скрипт

Например, CSV с данными. Добавьте копирование файла в `deploy.yml`, рядом с `main.py`:

```yaml
      - name: Copy data file into container
        run: docker cp data.csv spark-master:/opt/spark/work-dir/data.csv
```

## Если нужно изменить сам пайплайн

Редактируете `.github/workflows/deploy.yml` как любой другой файл — `git add/commit/push`, изменения применятся при следующем запуске.

## Полезные команды для отладки

```powershell
# Посмотреть, что сейчас запущено
docker ps

# Логи конкретного контейнера
docker logs spark-master

# Зайти внутрь контейнера руками
docker exec -it spark-master bash

# Веб-интерфейс Spark Master (проверить, что воркеры живы)
http://localhost:8080

# Полностью остановить и удалить кластер вручную
docker compose down --remove-orphans

# Перезапустить службу runner'а
Restart-Service "actions.runner.*"
```

## История основных решённых проблем

| Проблема | Решение |
|---|---|
| `Python worker failed to connect back` | Задать `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` |
| `UnknownHostException: spark-master` | Использовать имя сервиса, а не `localhost`, изнутри Docker-сети |
| `running scripts is disabled` | `Set-ExecutionPolicy RemoteSigned -Scope LocalMachine` |
| `permission denied ... docker_engine` | Добавить учётку службы runner'а в группу `docker-users` |
| `container name already in use` | Шаг `docker compose down --remove-orphans` перед `up -d` |
| `pwsh: command not found` | Использовать `shell: powershell`, а не `pwsh` (он не установлен) |