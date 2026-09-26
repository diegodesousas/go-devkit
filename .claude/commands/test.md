---
description: Roda os testes unitários (rápido, sem Postgres)
allowed-tools: Bash(make test), Bash(docker compose run --rm dev go test:*), Read, Grep, Glob
---

Rode `make test` (testes unitários com `-race`, no container `dev`). Para um pacote ou teste isolado: `docker compose run --rm dev go test ./pkg/<pkg>/... -race -run <Teste>`. Nunca `go test` direto no host.

Se tudo passar, reporte em uma linha e pare.

Se falhar:
- Liste os pacotes vermelhos e a asserção exata que quebrou.
- **Não conserte nada por conta própria.** Explique a causa provável e pergunte se deve corrigir.
- Se o erro mencionar Docker, dockertest ou `pgsql`, algo perdeu o `//go:build integration` — aponte o arquivo.

Este alvo exclui `pkg/database/sql`, que é integração. Para aquele pacote use `/test-integration`.
