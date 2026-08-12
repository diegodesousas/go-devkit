---
description: Cria um pacote novo em pkg/ seguindo as convenções do repo
argument-hint: <nome-do-pacote> [descrição curta do que ele faz]
allowed-tools: Read, Write, Edit, Grep, Glob, Bash(go build:*), Bash(go test:*), Bash(gofmt:*)
---

Crie o pacote `pkg/$1` seguindo o padrão estabelecido do repo. Contexto adicional: $2

Antes de escrever, leia [pkg/httpserver/server_options.go](pkg/httpserver/server_options.go) e [pkg/database/sql/errors.go](pkg/database/sql/errors.go) — são as referências canônicas de options e de erros.

## Estrutura

**`pkg/$1/$1.go`** — interface exportada + struct privada + construtor variádico:

```go
package $1

type Thing interface {
	Do(ctx context.Context, input string) error
}

type thing struct {
	settings settings
}

func New(options ...Option) Thing {
	s := defaultSettings
	for _, opt := range options {
		s = opt(s)
	}

	return &thing{settings: s}
}
```

O construtor só retorna `error` se houver algo real que possa falhar (I/O, validação). Não invente erro.

**`pkg/$1/$1_options.go`** — declaração agrupada em `type (...)`, options valor→valor, defaults em `var defaultSettings`:

```go
type (
	Option   func(s settings) settings
	settings struct {
		timeout time.Duration
	}
)

var defaultSettings = settings{
	timeout: defaultTimeout,
}

func WithTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.timeout = d

		return s
	}
}
```

Options **não** retornam erro e **não** validam. Se precisar validar, faça no construtor.

**`pkg/$1/errors.go`** — só se o pacote precisar de erros próprios. Sentinelas com prefixo `Err`, criadas com `github.com/pkg/errors`:

```go
var ErrSomethingFailed = errors.New("$1: something failed")
```

**`pkg/$1/$1_test.go`** — `package $1_test`, black-box, `testify/assert`. Table-driven com `t.Run` se a lógica for pura; um-teste-por-cenário se envolver rede/mocks. Se precisar de mock, escreva à mão com `testify/mock` em `mock_test.go` (não existe mockery aqui).

## Regras que valem sempre

- `github.com/pkg/errors` para wrapping — nunca `fmt.Errorf`, nunca `%w`.
- `ctx context.Context` como primeiro parâmetro em I/O.
- Tudo em inglês.
- Sem godoc obrigatório: o repo praticamente não tem, não destoe inventando um padrão novo.

## Ao final

Rode `go build ./...` e `go test ./pkg/$1/...`, e mostre o resultado. Se o pacote merecer entrar no [README.md](README.md), pergunte antes de editá-lo — o README lista só os recursos de destaque.
