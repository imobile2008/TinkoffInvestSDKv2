# tinvest-cpp — архитектура C++ SDK для T-Invest API

## 1. Цели

- **Полнота**: поддержка всех контрактов T-Invest API v2
  (https://opensource.tbank.ru/invest/invest-contracts, зеркало github.com/RussianInvestments/investAPI):
  12 gRPC-сервисов, ~110 RPC, включая двунаправленный и серверные стримы.
- **Скорость**: минимум накладных расходов поверх gRPC — один мультиплексируемый
  HTTP/2-канал, callback-API gRPC (без выделенных потоков опроса completion queue),
  отсутствие лишних копирований (пользователь работает напрямую с protobuf-сообщениями),
  отсутствие исключений на горячем пути.
- **Удобство**: типобезопасные обёртки каждого метода (sync + async в одном имени через
  перегрузку), `Result<T>` с богатой ошибкой (`x-tracking-id`, rate-limit метаданные),
  `Decimal`/`Money` для `Quotation`/`MoneyValue`, стримы с автопереподключением и
  автоматическим восстановлением подписок.
- **Целевая платформа**: Linux (Ubuntu 22.04/24.04), C++20, GCC ≥ 11 / Clang ≥ 14.
  Код кроссплатформенный, но CI/тесты — Linux.

## 2. Слои

```
┌────────────────────────────────────────────────────────────┐
│  Пользовательский код                                      │
├────────────────────────────────────────────────────────────┤
│  L3  Удобства: Decimal/Money, хелперы построения запросов  │
├────────────────────────────────────────────────────────────┤
│  L2  Сервисные обёртки (8 unary-сервисов) + стримы         │
│      UsersService, InstrumentsService, MarketDataService,  │
│      OperationsService, OrdersService, StopOrdersService,  │
│      SandboxService, SignalService;                        │
│      MarketDataStream (bidi), PortfolioStream,             │
│      PositionsStream, TradesStream, OrderStateStream,      │
│      MarketDataServerSideStream                            │
├────────────────────────────────────────────────────────────┤
│  L1  Client: канал, TLS, auth-метаданные, retry на         │
│      RESOURCE_EXHAUSTED, дефолтные таймауты, Error-маппинг │
├────────────────────────────────────────────────────────────┤
│  L0  Сгенерированные protoc/grpc_cpp_plugin стабы          │
│      (namespace tinkoff::public_::invest::api::contract::v1│
│       — protoc экранирует ключевое слово `public`)         │
└────────────────────────────────────────────────────────────┘
```

## 3. Ключевые решения

### 3.1 Транспорт: gRPC callback API

Для unary-вызовов и стримов используется **callback API** (`stub->async()->Method(...)`,
`grpc::ClientBidiReactor`, `grpc::ClientReadReactor`) — официально рекомендованный
асинхронный интерфейс без ручного управления completion queue. Колбэки исполняются в
пуле потоков gRPC (EventEngine); долгую работу пользователь переносит в свой поток.
Синхронные варианты методов блокируются на `absl`-нотификации поверх того же пути.

### 3.2 Публичные типы = protobuf-сообщения

Обёртки не перекладывают данные в собственные DTO — это лишние копии и отставание от
контрактов. Публичный API принимает и возвращает сгенерированные типы через алиас
`tinvest::pb` (`namespace pb = tinkoff::public_::invest::api::contract::v1`).
Удобства (L3) — это *дополнение*: `Decimal(q)`, `to_string(money)`, фабрики частых
запросов, а не замена protobuf.

### 3.3 Ошибки: `Result<T>` вместо исключений

```cpp
tinvest::Result<pb::PostOrderResponse> r = client.orders().post_order(req);
if (!r) { log(r.error().message, r.error().tracking_id); }
```

`Error` содержит: `grpc::StatusCode`, `message` (из trailing-метаданных `message`,
человекочитаемое описание кода ошибки API), `api_code`, `tracking_id`
(`x-tracking-id`), rate-limit поля (`x-ratelimit-limit/-remaining/-reset`).
Исключений SDK не бросает (кроме фатальных ошибок конфигурации при создании клиента).

### 3.4 Sync + async через перегрузку одного имени

RPC `PostOrderAsync` существует в самом контракте, поэтому суффикс `_async` для
колбэк-вариантов создал бы коллизии. Вместо этого:

```cpp
// синхронно
Result<Resp> post_order(const Req& = {}, const CallOptions& = {});
// асинхронно (колбэк из потока gRPC)
void post_order(Req, Callback<Resp>, const CallOptions& = {});
```

Каждый из ~110 методов объявляется одной строкой X-макроса, разворачивающегося в обе
перегрузки поверх общих шаблонов `detail::unary_sync/unary_async` — единая точка
логики (метаданные, дедлайны, retry, маппинг ошибок), нулевое дублирование.

### 3.5 Retry и rate-limit

Лимиты API — поминутные, при превышении приходит `RESOURCE_EXHAUSTED` и заголовок
`x-ratelimit-reset` (секунды до сброса). Синхронные вызовы (opt-in, включено по
умолчанию, настраивается в `Config::retry`) ждут `reset` и повторяют запрос до N раз.
Также повторяются идемпотентные по своей природе `UNAVAILABLE` (обрыв канала).
`PostOrder`/`PostOrderAsync` повторяются безопасно благодаря клиентскому
`order_id`-идемпотентности API, но по умолчанию торговые методы **не** ретраятся —
консервативный дефолт. Async-вызовы не ретраятся (пользователь решает сам).

### 3.6 Стримы: реакторы + автопереподключение

- **`MarketDataStream`** — bidi `MarketDataStream`. Хранит реестр активных подписок
  (свечи/стаканы/сделки/статусы/последние цены) и очередь исходящих записей
  (реактор допускает только один `StartWrite` за раз). При обрыве: экспоненциальный
  backoff (0.1→30 с, джиттер) → новый реактор → повторная отправка всех подписок.
  События доставляются через набор колбэков `MarketDataHandlers` (on_candle,
  on_order_book, on_trade, on_last_price, on_trading_status, on_open_interest,
  on_subscription_result, on_state). Ping от сервера обрабатывается прозрачно.
- **`ServerStream<Req, Resp>`** — общий шаблон для серверных стримов
  (`PositionsStream`, `PortfolioStream`, `TradesStream`, `OrderStateStream`,
  `MarketDataServerSideStream`) на `grpc::ClientReadReactor<Resp>` с тем же
  backoff-переподключением; ping фильтруется, остальное — в колбэк пользователя.
- Управление временем жизни: реакторы самоуничтожаются в `OnDone`; классы-владельцы
  (`MarketDataStream` и др.) — RAII, `stop()`/деструктор корректно закрывает стрим и
  дожидается `OnDone`.

### 3.7 Производительность

- Один `grpc::Channel` на клиент (HTTP/2 мультиплексирование), стабы создаются один раз.
- Callback API — нет собственных потоков SDK для unary-вызовов.
- keepalive: `keepalive_time=30s`, `keepalive_timeout=10s`, permit without calls —
  быстрое обнаружение мёртвых соединений; всё переопределяется через
  `Config::channel_args`.
- Запросы передаются по значению/rvalue в async-пути — protobuf move, без копий.
- Ответы читаются напрямую из сгенерированных сообщений — zero re-marshalling.
- `Decimal` — целочисленная арифметика (units + nano, int64/int32), без double на пути
  цен; конвертация в/из `Quotation`/`MoneyValue` — inline.
- Опциональный `grpc::Arena`-путь не включаем в v1: выигрыш заметен только на
  массовых мелких сообщениях, а усложняет владение; отмечено как будущее расширение.

### 3.8 Аутентификация и endpoint

- Метаданные каждого вызова: `authorization: Bearer <token>`, `x-app-name`
  (по умолчанию `tinvest-cpp`, настраивается — биржа просит идентифицировать SDK).
- TLS: системные корневые сертификаты (`grpc::SslCredentials({})`).
- Endpoint по умолчанию `invest-public-api.tbank.ru:443`
  (константа `kLegacyEndpoint = invest-public-api.tinkoff.ru:443` тоже доступна);
  песочница — тот же endpoint, отдельный сервис `SandboxService` + sandbox-токен.

## 4. Структура репозитория

```
InvestAPI/
├── CMakeLists.txt              # библиотека tinvest + tinvest_proto (codegen)
├── cmake/protogen.cmake        # protoc + grpc_cpp_plugin → build/gen
├── contracts/                  # вендоренные .proto (+ google/api/field_behavior)
├── scripts/
│   ├── setup-ubuntu.sh         # bootstrap чистой Ubuntu: deps + build + test
│   └── update-contracts.sh     # обновление contracts/ из upstream
├── include/tinvest/
│   ├── tinvest.hpp             # зонтичный заголовок
│   ├── pb.hpp                  # инклюды *.pb.h и алиас tinvest::pb
│   ├── decimal.hpp             # Decimal, Money (<-> Quotation/MoneyValue)
│   ├── error.hpp               # Error, Result<T>
│   ├── call_options.hpp        # таймаут, метаданные, retry override
│   ├── client.hpp              # Config, Client, доступ к сервисам/стримам
│   ├── detail/unary.hpp        # unary_sync/unary_async, retry, маппинг ошибок
│   ├── services.hpp            # 8 unary-сервисов (X-макросы)
│   └── streams/
│       ├── market_data_stream.hpp
│       └── server_stream.hpp   # шаблон + typedef'ы 5 серверных стримов
├── src/
│   ├── client.cpp
│   └── market_data_stream.cpp
├── examples/                   # accounts, instruments, quotes_stream, sandbox_trade
├── tests/                      # gtest: decimal, mock-сервер (unary/error/retry/стримы)
└── docs/ARCHITECTURE.md        # этот файл
```

## 5. Сборка

- CMake ≥ 3.22. Зависимости: gRPC/protobuf. Поиск: `find_package(gRPC CONFIG)` →
  fallback `pkg-config`. На Ubuntu ставится из apt (`libgrpc++-dev`,
  `protobuf-compiler-grpc`) — `scripts/setup-ubuntu.sh` делает всё сам.
- Кодогенерация во время сборки системным protoc — версии всегда согласованы.
- Артефакты: статическая библиотека `tinvest` (+`tinvest_proto`), примеры, тесты.
- GoogleTest — через FetchContent (только при `TINVEST_BUILD_TESTS=ON`).

## 6. Тестирование

1. **Unit**: Decimal-арифметика и конвертации (границы nano, отрицательные, округление).
2. **Mock-сервер**: in-process gRPC-сервер, реализующий Users/Orders/MarketDataStream:
   - корректность метаданных (authorization, x-app-name);
   - маппинг ошибок и tracking-id;
   - retry по RESOURCE_EXHAUSTED с x-ratelimit-reset;
   - bidi-стрим: подписка → данные → принудительный обрыв → авто-восстановление подписок.
3. **Интеграционные** (опционально, требуют `TINVEST_TOKEN` с sandbox-токеном):
   песочница — счёт → пополнение → ордер → позиции → отмена.

## 7. Добавлено после v1

- **Coroutine-слой** (`awaitable.hpp`): третья перегрузка каждого unary-метода
  `name(Req, tinvest::use_awaitable, opts)` возвращает awaiter поверх
  колбэк-пути; минимальный ленивый `Task<T>` с симметричным transfer и
  `sync_wait`. Возобновление — на потоке gRPC.
- **Предиктивный rate-limiter** (`rate_limiter.hpp`): группы методов с общим
  окном (метод → группа из `GetUserTariffResponse.unary_limits`,
  `Client::load_rate_limits()`), fixed-window счётчики. Sync-вызовы блокируются
  до начала следующего окна, async/awaitable отказывают мгновенно локальной
  ошибкой `RESOURCE_EXHAUSTED, api_code="client"`. Не настроен — не влияет.
- **CI**: GitHub Actions (ubuntu-24.04, apt gRPC 1.51) — сборка + ctest.
- **TLS**: по умолчанию используется системное хранилище
  (`/etc/ssl/certs/ca-certificates.crt`, важно для Russian Trusted CA на
  endpoint tbank.ru), переопределяется `Config::ca_file`.

## 8. Вне рамок (осознанно)

- Кэш инструментов, переподписка чанками > лимита стрима, метрики/трейсинг-хуки.
- Учёт stream-лимитов тарифа в rate-limiter (только unary).
