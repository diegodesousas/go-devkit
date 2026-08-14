# Kafka: troca de driver e redesenho do consumer

Data: 2026-08-13 · Versão alvo: **v0.3.0** (breaking)

## Contexto

Os três pacotes de Kafka — `pkg/stream`, `pkg/stream/consumer`, `pkg/stream/dispatcher` — carregam problemas que a auditoria de 2026-08-13 e a inspeção de usabilidade que a seguiu mapearam, mas que nenhuma das duas rodadas anteriores resolveu:

- **`confluent-kafka-go` v1.9.2**, no import path pré-módulo, sem manutenção. Embute librdkafka via cgo, o que torna `CGO_ENABLED=1` obrigatório e é o maior atrito de build do repo: afeta `-race`, cross-compilation e tempo de CI.
- **O handler nunca é cancelável.** O contexto da mensagem nasce de `context.Background()`, então não há caminho para interromper um `Handle` em andamento — nem no shutdown, nem por timeout.
- **`runRetry` trava o loop.** Faz `time.Sleep` cru e chama `backoff.Retry` sem contexto. Com `MaxElapsedTime` alto o consumer para de fazer poll e o broker o expulsa do grupo por `session.timeout.ms`.
- **Falha ao publicar no DLT derrota o consumer inteiro**, sem política explícita.
- **Knobs mortos e sem SASL/SSL.** `sessionTimeoutMs`, `offsetReset` e `autoCommit` existem em `clientSettings` sem option; não há como conectar em Confluent Cloud ou MSK.
- **Tipos do confluent na API pública** — `kafka.Message`, `kafka.Event`, `kafka.RebalanceCb`, `kafka.TopicPartition` — o que faz qualquer troca de driver ser breaking.
- **Config de broker duplicada e já divergente**: `consumer.WithBootstrapServer` (singular) contra `dispatcher.WithBootstrapServers` (plural), cada um montando seu próprio `ConfigMap`.
- **`sync.WaitGroup` decorativo** em `Run`: `Add(1)` seguido de chamada síncrona, então `Wait()` sempre vê zero.

## Decisões

Fechadas em brainstorming antes deste documento:

| # | Decisão |
|---|---|
| 1 | Trocar o driver **e** corrigir comportamento na mesma rodada — o franz-go reescreve a camada de cliente de qualquer forma, e as correções de ciclo de vida moram no mesmo código |
| 2 | **Concorrência por partição**: uma goroutine por partição, sequencial dentro de cada uma |
| 3 | **`Run(ctx) error` bloqueante**, substituindo `Run() Shutdown` + `ListenShutdown() chan error` |
| 4 | **DLT com retry, depois parada da partição** — nada se perde, o estrago fica contido |
| 5 | **Manter a costura de driver, com tipos próprios** (`stream.Record`) em vez de tipos do confluent |
| 6 | **Pacote `pkg/stream/kafka` dedicado** ao driver, com config de broker unificada |

## Arquitetura

Quatro pacotes, dependência em uma direção só:

```
pkg/stream/            Record, Header, Message, Reader, Writer, erros sentinela
        ▲                        ▲                      ▲
        │                        │                      │
pkg/stream/kafka/      implementa Reader e Writer sobre franz-go
                       config de broker: brokers, SASL, TLS, timeouts

pkg/stream/dispatcher/ usa stream.Writer
pkg/stream/consumer/   usa stream.Reader + dispatcher.Dispatcher (para o DLT)
```

`consumer` e `dispatcher` **não importam `kafka`**. O pacote `kafka` implementa interfaces declaradas em `stream` e não conhece nenhum dos dois. É isso que torna a próxima troca de driver um PR num pacote só, sem quebra de API — esta é a última quebra por esse motivo.

### Costuras, em `pkg/stream`

```go
type Reader interface {
    Poll(ctx context.Context) ([]Record, error)
    Commit(ctx context.Context, records ...Record) error
    Close() error
}

type Writer interface {
    Produce(ctx context.Context, record Record) error
    Flush(ctx context.Context) error
    Close() error
}
```

`Poll` devolve lote porque é o que o franz-go entrega, e é o que habilita a concorrência por partição. `Commit` recebe records em vez de offsets para que a tradução de "qual offset commitar" fique no driver, não no loop.

### `Record`, o tipo que substitui `*kafka.Message`

```go
type Record struct {
    Topic     string
    Partition int32
    Offset    int64
    Key       []byte
    Value     []byte
    Headers   []Header
    Timestamp time.Time
}

type Header struct {
    Key   string
    Value []byte
}
```

`stream.NewMessageType` passa a receber `Record`. Com isso some o último tipo do confluent da API pública.

## Componentes

### `pkg/stream/kafka`

Único lugar que importa `github.com/twmb/franz-go`. Expõe dois construtores e uma família de options compartilhada:

```go
func NewReader(groupID string, topics []string, opts ...Option) (stream.Reader, error)
func NewWriter(opts ...Option) (stream.Writer, error)

func WithBrokers(brokers ...string) Option
func WithSASLPlain(user, pass string) Option
func WithSASLSCRAM(user, pass string, mechanism SCRAMMechanism) Option
func WithTLS(cfg *tls.Config) Option
func WithSessionTimeout(d time.Duration) Option
func WithStartOffset(o StartOffset) Option   // StartEarliest | StartLatest
func WithProduceTimeout(d time.Duration) Option
func WithClientID(id string) Option
```

Uma família só resolve a divergência `WithBootstrapServer`/`WithBootstrapServers` e faz SASL/TLS ser escrito e testado uma vez.

Notas de configuração:

- **Auto-commit desligado.** O commit continua sendo do `consumer`, após sucesso do handler.
- **`ProduceTimeout` default sobe de 1s para 30s.** O 1s atual está muito abaixo do default do librdkafka (300s) e transforma qualquer soluço de broker em erro de entrega. 30s é conservador sem ser hostil.
- **Ordenação preservada.** O franz-go mantém uma requisição em voo por partição com `MaxProduceRequestsInflightPerBroker(1)`, o equivalente ao `max.in.flight=1` de hoje.

### `pkg/stream/consumer`

```go
func New[T any](
    reader stream.Reader,
    dlt dispatcher.Dispatcher,
    handler Handler[T],
    opts ...Option,
) (Consumer, error)

type Consumer interface {
    Run(ctx context.Context) error
}
```

`Run` bloqueia até o contexto ser cancelado (devolve `nil`) ou o loop falhar (devolve o erro). Somem `Shutdown`, `ListenShutdown` e o par de canais `shutdownStarted`/`shutdownFinished`.

**`Handler[T]` encolhe para um método:**

```go
type Handler[T any] interface {
    Handle(ctx context.Context, content T) error
}
```

Os outros quatro métodos somem porque a informação passou a existir em um lugar só:

- **`ID()` e `Topic()`** eram a identidade do grupo e do tópico — que agora são parâmetros de `kafka.NewReader`, quem de fato faz o subscribe. Manter os dois seria a mesma informação declarada duas vezes, com risco de divergir.
- **`ShouldSkip()` e `ConfigRetry()`** eram política, não identidade, e viram options.

O nome do tópico DLT deriva de `record.Topic` — o `Record` carrega o tópico de origem, então o consumer não precisa que ninguém lhe conte qual é.

```go
consumer.WithSkip(func(T) bool)
consumer.WithRetry(ConfigRetry)
consumer.WithDeadLetterTopic(topic string)  // default: <record.Topic>-dlt
consumer.WithLogger(log.Logger)
consumer.WithStringGenerator(gen.StringGenerator)
```

O caso trivial passa de cinco métodos para um.

### `pkg/stream/dispatcher`

```go
func New(w stream.Writer, opts ...Option) Dispatcher

type Dispatcher interface {
    Dispatch(ctx context.Context, topic, key string, content stream.Message) error
    Close(ctx context.Context) error
}

func WithFlushTimeout(d time.Duration) Option
```

`Shutdown()` vira `Close(ctx) error`: hoje o retorno do `Flush` — quantas mensagens **não** saíram — é descartado, então perda no shutdown é invisível. `Dispatch` passa a respeitar `ctx.Done()` enquanto espera a confirmação de entrega.

## Fluxo de dados

### Publicação

```
Message ──Serialize()──► []byte
                          │
        Type() ───────────┼──► header DEVKIT_CONTENT_TYPE
        span context ─────┴──► headers de trace (novo)
                          ▼
                    stream.Record ──► Writer.Produce ──► broker
                                       (espera confirmação, respeitando ctx)
```

**Propagação de trace (novo).** `Dispatch` injeta o contexto do span nos headers do record; o consumer extrai e continua o trace. Hoje `tracer.StartSpanFromContext` descarta o contexto retornado e nada é injetado, então produtor e consumidor aparecem como traces desconexos — perdendo exatamente a visibilidade que justifica o dd-trace estar sempre ligado.

### Consumo

```
Reader.Poll(ctx) ──► []Record
                       │
                       ├─ agrupa por partição
                       │
                       ├─ goroutine partição 0 ─► msg ─► msg ─► msg  (sequencial)
                       ├─ goroutine partição 1 ─► msg ─► msg
                       └─ goroutine partição N ─► msg
                       │
                       ├─ aguarda todas (errgroup)
                       │
                       └─ Commit dos records processados com sucesso
```

Por mensagem, dentro da partição:

1. `NewMessageType(record)` escolhe o decoder pelo header. Falha ⇒ DLT como texto.
2. `Deserialize` no tipo `T`. Falha ⇒ DLT como texto.
3. Skip configurado devolve `true` ⇒ commita sem processar.
4. `Handle(ctx, content)`. Sucesso ⇒ pronto para commit.
5. Falha com erro em `RetryableErrors` ⇒ retry com backoff, **cancelável pelo ctx**.
6. Falha não-retryable, ou retry esgotado ⇒ DLT.

**Commit é por partição, até o último sucesso contíguo.** Se a mensagem de offset 7 falha em definitivo numa partição, commita-se até 6 e a partição para ali — as demais seguem. Isso é o que torna a política de DLT da decisão 4 implementável sem perder mensagem.

## Tratamento de erros

| Situação | Comportamento |
|---|---|
| Payload indecifrável (header ausente/desconhecido, `Deserialize` falha) | DLT como texto, commita, segue |
| `Handle` falha com erro retryable | Backoff exponencial cancelável; sucesso commita |
| `Handle` falha não-retryable, ou retry esgota `MaxElapsedTime` | DLT, commita, segue |
| **Publicar no DLT falha** | Retry com backoff. Persistindo: loga `WarningTypeCritical`, **para de commitar aquela partição**, demais seguem |
| `Poll` falha | Erro sobe, `Run` retorna com ele |
| `ctx` cancelado | Aguarda as goroutines em voo, commita o que concluiu, `Run` retorna `nil` |

`backoff/v4` → **`backoff/v5`**, cuja API é context-first (`backoff.Retry(ctx, op, ...)`). Isso resolve de graça o `runRetry` incancelável e permite remover o `time.Sleep(InitialInterval)` cru — o `InitialInterval` do próprio backoff já cobre essa espera.

Convenção de erros do repo preservada: `github.com/pkg/errors`, sentinelas exportadas em `errors.go` com prefixo `Err`.

## Testes

- **A costura preserva a suíte sem broker.** `Reader` e `Writer` são interfaces, então os mocks à mão com `testify/mock` continuam sendo a estratégia — nada migra para `test-integration`, e `make test` segue sem Docker.
- **`testing/synctest`** (GA neste toolchain) substitui os 20+ `time.Sleep` e o helper `waitForCalls` de `consumer_test.go`. Relógio falso e quiescência determinística de goroutines: é exatamente a flakiness que já custou os PRs #14 e #15.
- **Cobertura nova obrigatória**, nos caminhos que hoje não existem:
  - cancelamento do ctx interrompendo um `Handle` em andamento;
  - retry cancelado no meio pelo shutdown;
  - falha ao publicar no DLT parando só a partição afetada, com as outras commitando;
  - lote com duas partições processado concorrentemente, com ordem preservada dentro de cada uma;
  - propagação de trace: header injetado no dispatch e extraído no consumo.
- `pkg/stream/kafka` ganha teste de construção de config (SASL, TLS, offsets) sem subir broker.

## Migração

Quebras da v0.3.0, todas em `pkg/stream*`:

| Antes | Depois |
|---|---|
| `consumer.NewFactory(...)` | `kafka.NewReader(groupID, topics, ...)` |
| `dispatcher.NewClient(...)` | `kafka.NewWriter(...)` |
| `consumer.Client`, `consumer.Factory` | `stream.Reader` |
| `dispatcher.Client` | `stream.Writer` |
| `c.Run() (Shutdown, error)` + `ListenShutdown()` | `c.Run(ctx) error` |
| `d.Shutdown()` | `d.Close(ctx) error` |
| `Handler.ID()`, `Handler.Topic()` | parâmetros de `kafka.NewReader` |
| `Handler.ShouldSkip/ConfigRetry` | options `WithSkip`/`WithRetry` |
| `stream.NewMessageType(*kafka.Message)` | `stream.NewMessageType(stream.Record)` |

### O group ID passa a ser explícito — e por quê

Hoje o `group.id` é montado como `devkit-<handler.ID()>`. Tirar o prefixo em silêncio faria o Kafka enxergar um **grupo novo, sem offsets commitados**; com início em `earliest`, o primeiro deploy da v0.3.0 **reprocessaria o tópico inteiro**. Em produção isso é cobrança e e-mail duplicados.

Por isso o group ID é **parâmetro posicional** de `kafka.NewReader`, não uma option com default:

```go
reader, err := kafka.NewReader("devkit-billing", []string{"orders"}, kafka.WithBrokers(...))
```

Ninguém constrói um reader sem nomear o grupo — é erro de compilação, não de runtime, e não existe default para herdar em silêncio. Quem estava em `devkit-billing` é obrigado a escrever `devkit-billing` ao migrar.

As notas de release devem abrir com esse item, incluindo a instrução de manter o prefixo `devkit-` para preservar os offsets.

### Saída do cgo

Com o franz-go (Go puro), `CGO_ENABLED=1` deixa de ser obrigatório. Sai do Makefile e do CI, `-race` e cross-compilation destravam. `go.mod` perde `confluent-kafka-go` e ganha `twmb/franz-go`.

## Fora de escopo

- Reescrever `pkg/database/sql`, `pkg/cache`, `pkg/metrics` ou `pkg/httpserver`.
- `logrus` → `log/slog`: decidido e aprovado, mas é rodada própria.
- Baixar a diretiva `go` do `go.mod` (achado M1): independente desta mudança.
- API de lote/assíncrona no dispatcher. `Dispatch` segue síncrono e um-a-um; a concorrência entra no consumer, onde o gargalo real está.
