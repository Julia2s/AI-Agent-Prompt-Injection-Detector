| metric | без ограничений, 1 воркер | с ограничениями, 1 воркер | с ограничениями, 2 воркера | с ограничениями, 1→2 воркера |
|---|---|---|---|---|
| max queue ready | 6087 | 50 | 50 | 50 |
| max ready+unacked | 41870 | 51 | 52 | 51 |
| publish attempts/s at broker (steady) | 473.03 | 4.31 | 6.46 | 5.84 |
| worker ack/s (steady) | 1.79 | 2.15 | 4.31 | 3.68 |
| max mem_worker_mb | 698.8 | 249.7 | 497.9 | 503.2 |
| max mem_producer_mb | 299.4 | 43.3 | 41.7 | 41.7 |
| max mem_rabbitmq_mb | 446.6 | 262.1 | 220.9 | 244.0 |
| SOURCE accepted tasks/s (steady) | 1626.39 | 1.9 | 3.87 | 3.6 |
| source max buffered tasks | 27964 | 1 | 1 | 1 |
| source max RSS, MiB | 303.7 | 54.6 | 54.5 | 54.6 |
| rejected publish attempts | 0 | 114 | 115 | 172 |
| LOST tasks | 0 | 0 | 0 | 0 |
| tasks finished | 74 | 144 | 281 | 287 |
| inference ms (mean) | 554.4 | 508.1 | 508.0 | 508.1 |
| latency s p50 | 25.8 | 29.49 | 13.95 | 16.2 |
| latency s p95 | 48.83 | 29.87 | 16.53 | 29.68 |
| latency s max | 51.93 | 29.94 | 16.58 | 29.84 |
