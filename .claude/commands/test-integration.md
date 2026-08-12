---
description: Roda os testes de integração de pkg/database/sql (exige Docker)
allowed-tools: Bash(docker info:*), Bash(make test-integration), Bash(go test:*), Read, Grep, Glob
---

Testes de integração de `pkg/database/sql`, que sobem um Postgres 15.3 via `ory/dockertest`.

1. Cheque o daemon primeiro: `docker info > /dev/null 2>&1`.
   Se falhar, **pare aqui** e diga que o Docker precisa estar rodando — não tente `make test-integration`, porque `TestMain` faz `log.Fatalf` e a mensagem de erro real fica enterrada.

2. Com o Docker no ar, rode `make test-integration`.

Ao interpretar falhas, tenha em mente que esta suíte é ordem-dependente de propósito:
- `TestTransactionContext_Commit_Error` mata o container (`pgsql.Close()`) para forçar erro de commit.
- O helper `db()` recria o Postgres via `isContainerRunning()` quando isso acontece.
- `resource.Expire(120)` derruba o container após 120s.

Ou seja: falhas de conexão no meio da suíte podem ser efeito colateral esperado, não regressão. Rodar um teste isolado com `-run` costuma esclarecer.
