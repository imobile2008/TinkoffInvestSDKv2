# tinvest-cpp

Быстрый и удобный C++20 SDK для [T-Invest API](https://developer.tbank.ru/invest/intro/intro)
(gRPC-контракты [invest-contracts](https://opensource.tbank.ru/invest/invest-contracts)).

Покрывает **все** контракты API: 12 сервисов, ~110 методов, включая
двунаправленный стрим маркет-даты и серверные стримы позиций, портфеля,
сделок и заявок. Архитектура — в [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Возможности

- Все unary-методы в двух видах через перегрузку одного имени:
  блокирующий (`Result<T>`) и асинхронный (колбэк, gRPC callback API — без
  собственных потоков SDK).
- Стримы с автопереподключением и автоматическим восстановлением подписок.
- `Result<T>` вместо исключений; ошибка несёт `x-tracking-id`, код API и
  rate-limit метаданные.
- Автоматический retry по лимитам (`RESOURCE_EXHAUSTED` + `x-ratelimit-reset`)
  и обрывам (`UNAVAILABLE`) — настраивается.
- `co_await`: каждый unary-метод имеет перегрузку с `tinvest::use_awaitable`
  плюс лёгкий `tinvest::Task<T>`/`sync_wait` (свой рантайм не обязателен).
- Предиктивный клиентский rate-limiter: `client.load_rate_limits()` загружает
  реальные поминутные лимиты тарифа из `GetUserTariff`; блокирующие вызовы
  ждут окно, async/awaitable мгновенно возвращают ошибку `api_code="client"`.
- `Decimal`/`Money` — целочисленная фикс-точка для `Quotation`/`MoneyValue`.
- Один HTTP/2-канал на клиент, keepalive-настройки для быстрого обнаружения
  обрывов; тюнинг канала через хук.

## Сборка (Ubuntu)

```bash
./scripts/setup-ubuntu.sh   # ставит зависимости, собирает, гоняет тесты
```

Вручную: нужны CMake ≥ 3.22, protobuf и gRPC ≥ 1.46
(Ubuntu 24.04: `apt install protobuf-compiler-grpc libgrpc++-dev libprotobuf-dev`).

```bash
cmake -S . -B build -G Ninja && cmake --build build -j && ctest --test-dir build
```

## Быстрый старт

```cpp
#include <tinvest/tinvest.hpp>

tinvest::Client client({.token = std::getenv("TINVEST_TOKEN")});

// Синхронно
auto accounts = client.users().get_accounts();
if (!accounts) {
  // accounts.error(): code, message, api_code, tracking_id, ratelimit
}

// Асинхронно (колбэк на потоке gRPC)
client.users().get_accounts({}, [](auto r) { /* ... */ });

// Корутиной
tinvest::Task<int> demo(tinvest::Client& c) {
  auto r = co_await c.users().get_accounts({}, tinvest::use_awaitable);
  co_return r ? 0 : 1;
}
// ... int rc = tinvest::sync_wait(demo(client));

// Клиентский rate-limiter по реальному тарифу токена
client.load_rate_limits();

// Ордер
tinvest::pb::PostOrderRequest req;
req.set_account_id(id);
req.set_instrument_id("BBG004730N88");
req.set_quantity(1);
req.set_direction(tinvest::pb::ORDER_DIRECTION_BUY);
req.set_order_type(tinvest::pb::ORDER_TYPE_LIMIT);
*req.mutable_price() = tinvest::Decimal(315, 500'000'000).to_quotation();
auto posted = client.orders().post_order(req);
```

Стрим маркет-даты (подписки переживают реконнект):

```cpp
tinvest::MarketDataHandlers h;
h.on_last_price = [](const tinvest::pb::LastPrice& p) { /* ... */ };
tinvest::MarketDataStream stream(client, h);
stream.start();
stream.subscribe_last_prices({"BBG004730N88"});
```

Серверные стримы: `make_positions_stream`, `make_portfolio_stream`,
`make_trades_stream`, `make_order_state_stream`,
`make_market_data_server_stream` (см. `tinvest/streams/server_stream.hpp`).

## Алготрейдинг (`tinvest/algo`)

Заголовочный модуль поверх SDK: потоковые индикаторы (SMA, EMA, RSI, MACD,
Bollinger, Donchian), шесть классических стратегий (кроссоверы SMA/EMA,
RSI- и Bollinger-реверсия, MACD, пробой Дончиана «turtle») и long-only
бэктестер с комиссиями и метриками (return, max drawdown, Sharpe, win rate).
Референсные реализации для исследований — не инвестиционная рекомендация.

```cpp
#include <tinvest/algo/strategies.hpp>
#include <tinvest/algo/backtest.hpp>
// bars — из market_data().get_candles(...) через tinvest::algo::to_bar
tinvest::algo::SmaCross strat(20, 50);
auto report = tinvest::algo::run_backtest(strat, bars, /*commission_pct=*/0.05);
```

## Примеры

| Пример | Что делает |
|---|---|
| `examples/accounts.cpp` | счета, тариф, стоимость портфеля |
| `examples/quotes_stream.cpp` | стрим цен и свечей |
| `examples/sandbox_trade.cpp` | песочница: счёт → пополнение → ордер → закрытие |
| `examples/coro_trade.cpp` | тот же цикл, но на `co_await` |
| `examples/backtest.cpp` | бэктест 6 стратегий на реальной истории свечей |
| `examples/paper_trade.cpp` | live paper-трейдинг EMA-кросса на стриме (без ордеров) |

```bash
TINVEST_TOKEN=t.xxx ./build/examples/accounts
```

## TLS и российские сертификаты

Endpoint по умолчанию `invest-public-api.tbank.ru:443` подписан УЦ Минцифры
(«Russian Trusted CA») — его нет в стандартных хранилищах. `setup-ubuntu.sh`
устанавливает его в системное хранилище автоматически; SDK по умолчанию читает
`/etc/ssl/certs/ca-certificates.crt`, поэтому дальше всё работает из коробки.
Альтернативы: свой PEM-бандл через `Config::ca_file` или legacy-endpoint
`tinvest::kLegacyEndpoint` (invest-public-api.tinkoff.ru, публичный УЦ).

Замечание про песочницу: sandbox-токен работает с `SandboxService` и
маркет-датой; `UsersService`/`OperationsService` и др. требуют боевой токен
(иначе `40003 Authentication token is missing or invalid`).

## Производительность и стабильность

Инструменты: `bench/bench` (микробенчмарки) и `bench/soak [сек]` (soak-тест с
реконнектами, churn'ом клиентов и мониторингом RSS). Замеры на Ubuntu 24.04,
4 vCPU, gRPC 1.51, loopback (оверхед SDK+gRPC без сети):

| Метрика | Результат |
|---|---|
| `Decimal` сложение / умножение | ~8 / ~14 нс/оп |
| Бэктестер (SmaCross) | ~57 млн баров/с |
| Unary sync, латентность | p50 156 мкс, p99 352 мкс |
| Unary async, throughput | ~31 000 req/s (окно 256) |
| Стрим маркет-даты | ~152 000 msg/s |
| Soak 5 мин | 7 млн вызовов, 2.8 млн сообщений, 5 531 реконнект, 0 ошибок, RSS — плато ~60 МБ |

Санитайзеры: ASan+UBSan+LSan — все тесты и soak чисты (утечек нет);
valgrind memcheck — чисто (definite-утечек нет). TSan (требует
`sysctl vm.mmap_rnd_bits=28` на ядрах 6.5+): с системным неинструментированным
gRPC/absl даёт ложный шум; стерильный прогон против gRPC/absl/protobuf,
собранных с `-fsanitize=thread`, — **ноль предупреждений** на тестах и soak
(один реальный узкий race в публикации реактора был найден именно этим
прогоном и исправлен). Сборка: `-DTINVEST_SANITIZE=address|thread`, для
стерильного TSan — `-DCMAKE_PREFIX_PATH=/opt/grpc-tsan` с инструментированным
префиксом.

## Потоки и потокобезопасность

- `Client` и все сервисы потокобезопасны.
- Колбэки (async-методы, обработчики стримов) вызываются на потоках gRPC:
  не блокируйте их — копируйте сообщение и передавайте в свою очередь.
- Автоматический retry действует только на блокирующие вызовы; торговые
  методы безопасно повторять благодаря клиентскому `order_id`
  (ключ идемпотентности задаёте вы).

## Обновление контрактов

```bash
./scripts/update-contracts.sh
```
