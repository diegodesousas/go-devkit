---
description: Roda gofmt, go vet e golangci-lint
allowed-tools: Bash(make lint), Bash(docker compose run --rm dev gofmt:*), Bash(docker compose run --rm dev go vet:*), Bash(docker compose run --rm dev golangci-lint:*), Bash(git diff:*), Read, Grep, Glob
---

Rode `make lint` (`gofmt -l` + `go vet ./...` + `golangci-lint run`, todos no container `dev`). Nunca rode `gofmt`, `go vet` ou `golangci-lint` direto no host.

Ao reportar, separe os achados em dois grupos — a distinção importa aqui:

**Nos arquivos tocados nesta branch** (cruze com `git diff --name-only main...HEAD`): são acionáveis, corrija ou proponha correção.

**Nos demais arquivos**: é dívida pré-existente, arquivos fora do `gofmt` desde o commit inicial. Apenas mencione a contagem; **não saia reformatando o repo**.

Contexto para calibrar expectativa:
- A config está em [.golangci.yml](.golangci.yml): os defaults do v2 mais `errorlint`, `bodyclose`, `noctx` e `gosec`.
- A imagem e o CI rodam a mesma versão, **v2.14.0**. O CI usa `only-new-issues: true`, então achados antigos que aparecem local e não no CI são normais.
