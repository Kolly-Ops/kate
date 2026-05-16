# Rithmic Adapter Spec for Kate

Date: 2026-05-09  
Owner: Codex  
Context: Sierra demoted to observation-only; Rithmic-direct path locked pending Edgeclear R|API+ credentials.

## Goal

Build `RithmicBrokerAdapter` on top of `async_rithmic==1.5.10` so Kate can replace Sierra DTC for:

- account state / NLV;
- order submission;
- order updates;
- position updates;
- live tick feed for 1-minute candle aggregation.

The ORB strategy, risk engine, and state store should remain broker-neutral.

## Current Surfaces

### Existing DTCClient Methods Used By Engine

| Current Engine Use | DTC Method / Raw Send | Rithmic Equivalent |
|---|---|---|
| transport connect | `DTCClient.connect()` | `RithmicClient.connect(plants=[ORDER_PLANT, PNL_PLANT, TICKER_PLANT])` |
| auth/logon | `DTCClient.logon(...)` | part of `RithmicClient.connect()` per plant |
| event queue | `DTCClient.recv_event()` / raw `DTCMessage` | adapter-owned `asyncio.Queue[BrokerEvent]`, fed by async_rithmic callbacks |
| account seed | raw `pack_account_balance_request` | `client.list_account_summary(account_id=...)` |
| positions seed | raw `pack_current_positions_request` | `client.list_positions(account_id=...)` |
| open orders seed | raw `pack_open_orders_request` | `client.list_orders(account_id=...)` |
| entry submit | `submit_order(...)` | `client.submit_order(...)` via ORDER_PLANT |
| stop/target submit | engine submits DTC child stop + limit after fill | prefer native Rithmic bracket via `stop_ticks` + `target_ticks` on entry submit |
| cancel sibling | `cancel_order(client_order_id=...)` | `client.cancel_order(order_id=...)` or `cancel_order(basket_id=..., account_id=...)` |
| market data | `.scid` file via `CandleManager`, not DTC | `client.get_front_month_contract(...)` + `client.subscribe_to_market_data(...)` |
| heartbeat | DTC heartbeat loop | handled internally by async_rithmic plant heartbeats |

## async_rithmic API Notes

Installed and inspected:

```text
async-rithmic==1.5.10
```

Constructor:

```python
RithmicClient(
    user: str,
    password: str,
    system_name: str,
    app_name: str,
    app_version: str,
    url: str,
    manual_or_auto=OrderPlacement.MANUAL,
)
```

Test gateway used by probe:

```text
rituz00100.rithmic.com:443
```

The fake-credential probe reached Rithmic and failed at auth with `rpCode ['13', 'permission denied']`, which confirms network/TLS/system-name viability from this machine.

## Proposed Files

```text
trading_bot/core/execution/rithmic_adapter.py
trading_bot/core/data/tick_candle_aggregator.py
tests/unit/test_rithmic_adapter_mapping.py
tests/unit/test_tick_candle_aggregator.py
```

Do not put credentials in repo. Read runtime config from env or a local ignored file.

## Configuration

Suggested env vars:

```text
KATE_BROKER=rithmic
RITHMIC_USER=...
RITHMIC_PASSWORD=...
RITHMIC_SYSTEM_NAME=Rithmic Test
RITHMIC_URL=rituz00100.rithmic.com:443
RITHMIC_ACCOUNT_ID=...
RITHMIC_APP_NAME=kate
RITHMIC_APP_VERSION=0.1
```

Production gateway/system name must come from Edgeclear/Rithmic. Do not assume test values for live.

## Adapter Lifecycle

### `connect()`

Create one `RithmicClient` with:

```python
manual_or_auto=OrderPlacement.AUTO
```

Connect only required plants:

```python
await client.connect(plants=[
    SysInfraType.ORDER_PLANT,
    SysInfraType.PNL_PLANT,
    SysInfraType.TICKER_PLANT,
])
```

Reason:

- ORDER_PLANT: accounts, trade routes, orders, brackets.
- PNL_PLANT: account NLV and positions.
- TICKER_PLANT: MES ticks and BBO.
- HISTORY_PLANT: not needed for day-one direct runtime. The library docs say the test environment does not provide historical market data, so do not build the critical path on it.

### Callback Wiring

Wire callbacks to normalize events:

```python
client.on_tick += handle_tick
client.on_account_pnl_update += handle_account_pnl
client.on_instrument_pnl_update += handle_instrument_pnl
client.on_rithmic_order_notification += handle_rithmic_order
client.on_exchange_order_notification += handle_exchange_order
client.on_bracket_update += handle_bracket
client.on_disconnected += handle_disconnected
```

Each callback places `BrokerEvent` into an internal queue.

## Account State Mapping

### Snapshot

Use:

```python
await client.list_account_summary(account_id=account_id)
```

Expected output template ID: 451 (`AccountPnLPositionUpdate`) in async_rithmic internals.

Map to:

```python
AccountBalanceEvent(
    cash=...,
    nlv=...,
    pnl=...,
    margin_requirement=...,
    currency="USD",
)
```

Exact field names must be verified with real credentials because Rithmic protobuf field names are broker/account dependent. Use `MessageToDict(..., preserving_proto_field_name=True)` in the first real run and record the field map.

### Subscription

Use:

```python
await client.subscribe_to_pnl_updates()
```

This subscribes per account discovered from ORDER_PLANT login.

## Market Data Mapping

### Front Month

Resolve:

```python
contract = await client.get_front_month_contract("MES", "CME")
```

The adapter should maintain a mapping:

```text
logical symbol MESM26 -> rithmic contract returned by front-month resolver
```

For backtest/live consistency, store the resolved contract in logs at startup.

### Subscribe

```python
data_type = int(DataType.LAST_TRADE) | int(DataType.BBO)
await client.subscribe_to_market_data(contract, "CME", data_type)
```

`on_tick` receives dict-like data with:

- `datetime`
- `data_type`
- trade fields for LAST_TRADE
- bid/ask fields for BBO

Exact field names must be confirmed in first real tick capture.

### Candle Aggregation

Do not force Rithmic into existing `.scid` `CandleManager`.

Build a small `TickCandleAggregator`:

```python
class TickCandleAggregator:
    def __init__(self, timeframe_minutes: int = 1): ...
    def ingest_tick(self, symbol: str, timestamp: datetime, price: float, size: float) -> list[Candle]: ...
```

Semantics should match current `.scid` tailer:

- emits only closed candles;
- first tick starts the current bucket;
- when a tick arrives in a new bucket, emit the previous bucket;
- no time-forced close until later.

This allows `ManagedFuturesEngine` to keep receiving `Candle` objects and running ORB unchanged.

## Order Mapping

### Entry With Native Bracket

Preferred Rithmic path:

```python
await client.submit_order(
    order_id=intent.intent_id,
    symbol=rithmic_contract,
    exchange=intent.exchange,
    qty=int(intent.quantity),
    transaction_type=TransactionType.BUY or TransactionType.SELL,
    order_type=OrderType.MARKET,
    account_id=account_id,
    stop_ticks=stop_ticks,
    target_ticks=target_ticks,
)
```

Convert absolute prices to tick offsets:

```python
stop_ticks = round(abs(intent.price - intent.stop_loss) / tick_size)
target_ticks = round(abs(intent.take_profit - intent.price) / tick_size)
```

Validation:

- `stop_ticks >= 1`
- `target_ticks >= 1` when target present
- `quantity` must be integer for futures; reject fractional sizes in adapter.

### Side Mapping

Current Kate/DTC constants:

```text
proto.BUY  = 1
proto.SELL = 2
```

Map to Rithmic:

```text
BUY  -> TransactionType.BUY
SELL -> TransactionType.SELL
```

Confirm enum names in real code using `async_rithmic.enums.TransactionType`.

### Order Types

Current Kate/DTC:

```text
MARKET -> OrderType.MARKET
LIMIT  -> OrderType.LIMIT
STOP   -> OrderType.STOP_MARKET or equivalent trigger order
```

Day-one Rithmic direct should use market entries with native bracket offsets. Avoid recreating Sierra's submit-entry-then-child-stop/target flow unless native brackets prove unreliable.

## Event Mapping

### Order Events

Rithmic callbacks:

- `on_rithmic_order_notification`
- `on_exchange_order_notification`
- `on_bracket_update`

Normalize to:

```text
ORDER_ACK
ORDER_FILLED
ORDER_PARTIAL_FILL
ORDER_REJECTED
ORDER_CANCELED
```

Open question for implementation: which callback reliably carries the `user_tag`/order ID used by `client.submit_order(order_id=...)`. First real credential run should submit no orders, but later sim-order test must capture these raw callback payloads before automation.

### Position Events

Rithmic PNL plant:

```python
await client.list_positions(account_id=...)
client.on_instrument_pnl_update += ...
```

Normalize to `PositionEvent(symbol, quantity, avg_price)`.

## Recommended BrokerAdapter Adjustments

Claude's current `BrokerAdapter` ABC is directionally right. I recommend these changes before implementation hardens:

1. **Add seed methods separately**

Current:

```python
request_account_state(...)
```

Add:

```python
async def request_positions(...)
async def request_open_orders(...)
```

Reason: current engine seeds account balance, positions, and open orders independently. Rithmic supports the same via `list_account_summary`, `list_positions`, and `list_orders`.

2. **Do not require `logon()` as a separate public lifecycle step**

Rithmic logs in during `connect(plants=...)`; DTC logs in after TCP connect. Interface can still expose `connect()` only, or `connect()` plus `authenticate()`, but a DTC-shaped `logon()` leaks protocol details into Rithmic.

Suggested:

```python
async def connect(self) -> None
async def disconnect(self) -> None
```

Credentials are supplied to adapter constructor/config, not `logon()`.

3. **Make market data mandatory for Rithmic engine path**

The ABC currently treats `subscribe_market_data` as optional. For Rithmic-direct, market data is not optional; it replaces `.scid`.

Either:

- keep optional at ABC level but create `MarketDataAdapter` separately, or
- make `BrokerAdapter` explicitly include market data for the Rithmic path.

4. **Add explicit `symbol_map` concept**

The Rithmic adapter must map:

```text
Kate logical symbol: MESM26
Rithmic product root: MES
Rithmic resolved contract: returned front month, e.g. MESM6-like value
Exchange: CME
```

Do not rely on `InstrumentMeta.dtc_symbol` for Rithmic.

5. **Account state cannot be optional**

The entire Sierra exit is about broken account visibility. Rithmic adapter acceptance criteria must require reliable account summary snapshot at startup before strategy evaluation begins.

## Acceptance Criteria For First Real Credential Run

The probe must print:

- `ok: true`
- at least one account in `accounts`
- selected `account_id`
- non-empty `account_summary`
- resolved `front_month_contract`
- `tick_count > 0` after 30 seconds during market hours

If market is closed, `tick_count == 0` is inconclusive, but account + contract resolution still proves most of the stack.

## Acceptance Criteria For Adapter Milestone 1

No trading yet:

- Connects with real credentials.
- Logs account summary NLV.
- Subscribes to MES ticks.
- Aggregates ticks into 1-minute `Candle`.
- Feeds ORB strategy in dry/no-submit mode.
- Reconnects after forced disconnect and resubscribes.

## Acceptance Criteria For Adapter Milestone 2

Sim trading:

- Submits one tiny controlled market order with native bracket.
- Receives order ack.
- Receives fill callback.
- Receives position update.
- Receives account summary update.
- Cancels or exits cleanly.
- StateStore records order lifecycle without drift.

## Biggest Risks

1. **Field mapping unknown until real credentials**
   - Account PNL and order callbacks need raw capture.

2. **Historical warmup**
   - Rithmic test may not provide history. Need either live-only warmup, yfinance/SCID backfill for warm start, or a separate historical provider.

3. **Native bracket semantics**
   - Rithmic `stop_ticks`/`target_ticks` should be cleaner than Sierra, but must be sim-tested carefully.

4. **Front-month/rollover behavior**
   - Resolve contract at startup and log it. Rollover policy needs a separate gate.

5. **Exchange/data entitlements**
   - Rithmic login can succeed while CME MES data remains unauthorized. Probe must verify live ticks.

## Recommended Sequencing

1. Wait for Edgeclear credentials/API entitlement.
2. Run `tools/trading/rithmic_probe.py`.
3. Capture raw account/tick JSON in a local ignored artifact.
4. Implement `TickCandleAggregator`.
5. Implement read-only `RithmicBrokerAdapter` account + ticks.
6. Wire ORB dry/no-submit mode against live Rithmic ticks.
7. Add controlled sim order submission only after account/tick telemetry is stable.

