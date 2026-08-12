# go-devkit

Biblioteca Go (`github.com/diegodesousas/go-devkit`) com blocos reutilizáveis para aplicações multiproposito: HTTP server, consumer/dispatcher Kafka, logger, conexão SQL, cache, métricas e utilitários.

Não há binário — só `pkg/` (a lib) e `examples/` (um `package main` por recurso, que serve de documentação executável).

## Comandos

| Comando | O que faz |
|---|---|
| `make test` | Testes unitários, `-race`. **Não precisa de Docker.** ~5s |
| `make test-integration` | Só `pkg/database/sql`, com `-tags=integration`. **Exige Docker rodando** |
| `make test-all` | Os dois |
| `make lint` | `gofmt -l` + `go vet ./...` + `golangci-lint run` |
| `make fmt` | `gofmt -w` em `./pkg` e `./examples` |

`CGO_ENABLED=1` é obrigatório: `confluent-kafka-go v1.9.2` embute librdkafka via cgo, e `-race` também depende disso.

## Convenções de código

Ao escrever código novo, siga o que já existe — mesmo quando houver alternativa mais moderna. Consistência aqui vale mais que idiomatismo isolado.

**Erros — sempre `github.com/pkg/errors`.** `errors.Wrap`, `errors.Wrapf`, `errors.WithStack`, `errors.Cause`. Não existe um único `fmt.Errorf` em código de produção, e consequentemente **não há wrapping com `%w` em lugar nenhum**. `pkg/log/helpers.go` extrai `StackTrace()` dos erros, o que só funciona com os wrappers do `pkg/errors`.

Sentinelas exportadas ficam em `errors.go`, com prefixo `Err`. Referência: [pkg/database/sql/errors.go](pkg/database/sql/errors.go) — é também o único arquivo do repo com godoc de verdade.

**Functional options — `func(s settings) settings`**, valor entra, valor sai. Referência: [pkg/httpserver/server_options.go](pkg/httpserver/server_options.go). As options não retornam erro nem validam; quando há validação, ela roda no construtor (único caso: `settings.validate()` em [pkg/database/sql/upsert/query_builder.go](pkg/database/sql/upsert/query_builder.go)).

`pkg/log` usa `func(*settings)` por mutação — é a exceção histórica, não copie.

**Construtores retornam interface, com struct privada como implementação.** `New(...) Connection` devolvendo `*dbConn`, `New(...) Server` devolvendo `*server`, e assim por diante. `pkg/database/sql` é o único que usa struct de config (`sql.Config`) em vez de options.

**`ctx context.Context` sempre como primeiro parâmetro** nas APIs de I/O. Três valores trafegam por contexto: logger (`log.WithLogger`/`FromContext`), transação (`sql.WithTransaction`) e métrica (`pkg/metrics`).

**Tudo em inglês** — símbolos, comentários, mensagens de erro e de log.

## Convenções de teste

- **Black-box por padrão: `package x_test`.** White-box só onde é preciso tocar `settings` privado — 3 arquivos, ex.: [pkg/httpserver/server_options_test.go](pkg/httpserver/server_options_test.go).
- **`testify/assert`**, não `require` (~380 chamadas contra 20). `assert.Nil(t, err)` é usado como sinônimo de `assert.NoError` na maior parte do repo.
- Table-driven com `t.Run` para lógica pura (validator, encoding, upsert, message). Um-teste-por-cenário, com bloco `var (...)` de `expected*` no topo, nos pacotes de stream e http.
- Em tabelas, o campo de erro é `wantErr assert.ErrorAssertionFunc` com closure inline — não `wantErr bool`.
- **Mocks são escritos à mão** com `testify/mock`. Não há `mockery` nem `go:generate`. `mock_test.go` para mock interno ao pacote; `mock.go` (público, exportado da lib) só em `pkg/log` e `pkg/httpclient`.
- Ninguém usa `t.Parallel()`, `t.Cleanup()` nem `t.Helper()`.

## Armadilhas

**`pkg/database/sql` é integração pura.** Os três arquivos de teste (`startup_test.go`, `connection_test.go`, `transaction_test.go`) têm `//go:build integration` e sobem um Postgres 15.3 via `ory/dockertest`. Sem a tag, o pacote reporta `[no test files]`.

A suíte é **ordem-dependente por construção**: `TestTransactionContext_Commit_Error` mata o container de propósito (`pgsql.Close()`) para forçar erro de commit, e o helper `db()` recria o Postgres via `isContainerRunning()`. Por isso `-count=1`.

**Efeito colateral do build tag:** o gopls/LSP não indexa esses três arquivos sem `buildFlags: ["-tags=integration"]` na config do editor. Erros do tipo "No packages found for open file" ali são esperados, não são bug.

**dd-trace-go está sempre ligado**, não é opcional: spans em `dispatcher.Dispatch`, em todos os métodos de `sql.dbConn` e no router chi. A option `WithAPM` do httpserver controla apenas `tracer.Start()`, não a instrumentação.

**Três serializadores JSON coexistem**: `goccy/go-json` em `pkg/cache` e `pkg/encoding`, `encoding/json` da stdlib em `pkg/stream`, e o `JSONFormatter` do logrus. Ao mexer num pacote, use o que ele já usa.

**`pkg/encoding` e `pkg/httpclient` não são consumidos por nenhum outro pacote** — são ilhas. `pkg/cache` serializa com `goccy/go-json` direto em vez de usar `pkg/encoding`.

**`pkg/cache` e `pkg/metrics` não têm nenhum teste.** `metrics.New()` tem `"localhost:8125"` hardcoded.

## Dívida conhecida (não mexer sem pedir)

- **Não existe `.golangci.yml`.** O CI roda golangci-lint **v1.55.2** com os linters default e `only-new-issues: true`; o binário local é **v1.64.8**. Divergência entre local e CI é esperada.
- **Os workflows pinam `go-version: '1.21'`, mas `go.mod` declara `go 1.24.0`.** Funciona só porque o toolchain switching automático baixa o 1.24 em silêncio — o pin é ficção, e custa um download por job (`cache: false`).
- **34 dos 80 arquivos `.go` falham no `gofmt -l`** (indentados com espaços, herança do `.editorconfig` antigo). Não reformate em massa: o hook de `PostToolUse` normaliza cada arquivo conforme for editado.
- O coverprofile do CI é gerado e descartado — sem upload, sem threshold.
- Inconsistências de nomenclatura já existentes: `server_options.go` (plural) vs `client_option.go` (singular); `GeneralErr` em `pkg/database/sql/errors.go` foge do prefixo `Err`; campo `onConflictDonNothing` (typo) em `upsert`.
