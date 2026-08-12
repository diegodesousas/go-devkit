---
description: Roda gofmt, go vet e golangci-lint
allowed-tools: Bash(make lint), Bash(gofmt:*), Bash(go vet:*), Bash(golangci-lint:*), Bash(git diff:*), Read, Grep, Glob
---

Rode `make lint` (`gofmt -l` + `go vet ./...` + `golangci-lint run`).

Ao reportar, separe os achados em dois grupos — a distinção importa aqui:

**Nos arquivos tocados nesta branch** (cruze com `git diff --name-only main...HEAD`): são acionáveis, corrija ou proponha correção.

**Nos demais arquivos**: é dívida pré-existente. 34 arquivos já nascem fora do `gofmt` no commit inicial. Apenas mencione a contagem; **não saia reformatando o repo**.

Contexto para calibrar expectativa:
- Não existe `.golangci.yml` — rodam só os linters default (errcheck, gosimple, govet, ineffassign, staticcheck, unused).
- O binário local é **v1.64.8**; o CI pina **v1.55.2** com `only-new-issues: true`. Achados que aparecem local e não no CI são normais.
