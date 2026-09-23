# QA test suite

## Current scope

Install the QA dependencies and run the suite:
```bash
python -m pip install -r requirements-test.txt
python -m pytest
```

`test_qa_setup.py` verifies that the asynchronous HTTPX/ASGI client factory
works. `conftest.py` also provides an `async_client` fixture that loads the
Docker runtime entry point, `src.main:app`, 

## Checks waiting for the backend
- Every reported third-party component has a version and a non-negative
  response time.
- `/healthz` remains successful when the database is unavailable and does not
  call the database health check.
- `/api/v1/health` reports a database failure using the agreed response/status
  contract. The backend should expose its database probe as an injectable
  dependency or service so the failure can be tested without a live database.
- The version response matches the version source chosen by the backend.

```bash
python -m pytest --cov=src --cov-report=term-missing --cov-fail-under=80
```
