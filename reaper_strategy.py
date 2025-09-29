"""Dynamic volatility-adaptive grid strategy (Reaper).

动态波动率自适应网格策略 (Reaper)。

Usage / 使用说明:
    uv run reaper_strategy.py --dry-run --symbol BTC \\
        --performance-db data/reaper_performance.db --enable-adaptive

    - 将 API 密钥写入 `.env` (LIGHTER_KEY / LIGHTER_SECRET)，线上运行前先使用
      `--dry-run` 模式确认网格布局与日志。
    - `--performance-db` 可自定义 SQLite 存储路径，传空字符串关闭持久化。
    - 启用 `--enable-adaptive` 后，ATR 比例会自动调节网格间距、层数与 ADX 阈值；
      可配合 `--adaptive-*` 选项微调阈值。
    - `--trend-exit-mode` 控制趋势行情下如何处理现有挂单（pause/cancel/flatten）。
    - 性能统计默认每 120 秒写入一次，可用 `--performance-interval` 调整。

Key arguments / 关键参数:
    --symbol:
        Trading pair to manage. Defaults to `BTC` and works with any market the
        pylighter SDK exposes.
        交易对，默认 BTC，支持 pylighter SDK 已接入的任意品种。

    --indicator-period:
        Candlestick window length (in minutes) for ADX/ATR. Higher values smooth
        signals but react slower.
        ADX/ATR 指标使用的分钟 K 线周期，越大越平滑但响应越慢。

    --atr-multiplier / --grid-layers:
        Control grid spacing and depth when adaptive mode is disabled. When
        adaptive mode is on (默认开启)，会根据波动率在 multiplier 和 layers 范围内自动调整。

    --total-notional:
        Total quote capital distributed across buys and sells (split 50/50).
        网格分配的报价资金总额，买卖各占一半，用于限制风险敞口。

    --stop-buffer:
        Multiplier applied to spacing to position the master stop below the
        lower channel. Larger buffers widen the protective band.
        全局止损距离系数，越大越耐受极端波动。

    --trend-exit-mode:
        Behaviour when ADX indicates a trend:
            pause   -> keep existing grid orders in place (默认)
            cancel  -> remove resting orders but leave positions untouched
            flatten -> cancel orders and flatten inventory
        趋势模式下的处理策略：pause 保留已有挂单便于继续套利，cancel 清空挂单但保留仓位，
        flatten 则全撤单并尝试平仓。

    --performance-db / --performance-interval:
        Manage persistence cadence for equity snapshots. Passing an empty string
        disables SQLite tracking entirely.
        控制收益追踪的持久化频率，传空字符串可关闭记录。

Default behaviour / 默认行为:
    Adaptive mode + pause exit keeps the grid earning through mild regime flips
    while still obeying the master stop-loss. 在趋势触发时保留现有挂单、不强制平仓，
    通常能兼顾震荡收益与突然行情的风险控制；需要更激进的防守可切换为 cancel 或 flatten。

This strategy follows the design from ``reaper.md``:
- Uses ADX to filter market regime (range vs trend)
- Derives grid spacing from ATR to adapt to volatility
- Deploys symmetric buy/sell limit orders across a dynamic price channel
- Enforces a master stop-loss below the working range and optional position flattening

The implementation builds on pylighter SDK helpers for market data, orders, and
WebSocket price streaming. Logging is handled via ``utils.logger_config``.

该实现严格遵循 ``reaper.md`` 的思路：
- ADX 判别行情处于区间震荡还是趋势单边
- ATR 决定网格间距，随波动率动态收缩或放大
- 在动态价格通道里对称挂买卖限价单
- 通过全局止损与仓位平掉机制控制极端风险

代码复用 pylighter SDK 提供的行情、订单与 WebSocket 管理工具，并统一使用
``utils.logger_config`` 的日志体系。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv

from pylighter.client import Lighter
from pylighter.market_utils import MarketConstraints, MarketDataManager
from pylighter.order_manager import BatchOrderManager, OrderInfo
from pylighter.websocket_manager import PriceWebSocketManager
from utils.logger_config import get_strategy_logger

# ------------------------------
# Data containers
# ------------------------------


@dataclass
class IndicatorState:
    """Snapshot of the current indicator values and adaptive bounds.

    指标状态快照，记录 ADX/ATR 与当前自适应边界。
    """

    adx: float
    atr: float
    upper: float
    lower: float
    mid: float
    close: float
    timestamp: float


@dataclass(frozen=True)
class OrderPlan:
    """Desired order specification for reconciliation.

    目标订单规划，用于与实际挂单做差异化同步。
    """

    side: str
    price: float
    quantity: float

    def key(self, price_precision: int) -> str:
        formatted_price = format(self.price, f".{price_precision}f")
        return f"{self.side}:{formatted_price}"


@dataclass
class PerformanceSnapshot:
    """Point-in-time performance overview.

    策略表现快照：记录权益、PnL 与 ROI。
    """

    timestamp: float
    total_equity: float
    realized_pnl: float
    unrealized_pnl: float
    roi_pct: float


# ------------------------------
# Persistence utilities
# ------------------------------


class PerformanceStorage:
    """Lightweight SQLite-backed storage for performance snapshots.

    使用 SQLite 持久化策略表现数据，支持程序重启后继续累计。"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

    def initialize(self) -> None:
        """Create tables if needed and prepare the connection.

        初始化数据库文件与基础表结构。"""

        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_snapshots (
                timestamp REAL PRIMARY KEY,
                equity REAL NOT NULL,
                realized REAL NOT NULL,
                unrealized REAL NOT NULL,
                roi REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()

        with self._lock:
            if self.conn is not None:
                self.conn.close()
            self.conn = conn

    def close(self) -> None:
        with self._lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def get_baseline(self) -> Optional[float]:
        with self._lock:
            if self.conn is None:
                return None
            cursor = self.conn.execute(
                "SELECT value FROM performance_meta WHERE key='baseline_equity'"
            )
            row = cursor.fetchone()
        return float(row[0]) if row is not None else None

    def set_baseline(self, baseline: float) -> None:
        with self._lock:
            if self.conn is None:
                return
            self.conn.execute(
                "REPLACE INTO performance_meta(key, value) VALUES('baseline_equity', ?)",
                (str(baseline),),
            )
            self.conn.commit()

    def fetch_recent(self, limit: int) -> List[PerformanceSnapshot]:
        with self._lock:
            if self.conn is None:
                return []
            cursor = self.conn.execute(
                """
                SELECT timestamp, equity, realized, unrealized, roi
                FROM performance_snapshots
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        rows.reverse()
        return [
            PerformanceSnapshot(
                timestamp=float(r[0]),
                total_equity=float(r[1]),
                realized_pnl=float(r[2]),
                unrealized_pnl=float(r[3]),
                roi_pct=float(r[4]),
            )
            for r in rows
        ]

    def insert_snapshot(self, snapshot: PerformanceSnapshot, baseline: float) -> None:
        with self._lock:
            if self.conn is None:
                return
            self.conn.execute(
                """
                INSERT INTO performance_snapshots(timestamp, equity, realized, unrealized, roi)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(timestamp) DO UPDATE SET
                    equity=excluded.equity,
                    realized=excluded.realized,
                    unrealized=excluded.unrealized,
                    roi=excluded.roi
                """,
                (
                    snapshot.timestamp,
                    snapshot.total_equity,
                    snapshot.realized_pnl,
                    snapshot.unrealized_pnl,
                    snapshot.roi_pct,
                ),
            )
            self.conn.execute(
                "REPLACE INTO performance_meta(key, value) VALUES('baseline_equity', ?)",
                (str(baseline),),
            )
            self.conn.commit()


# ------------------------------
# Indicator helpers
# ------------------------------


def calculate_adx_atr(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int,
) -> Tuple[Optional[float], Optional[float]]:
    """Return the latest ADX and ATR using Wilder's smoothing.

    使用 Wilder 平滑算法计算最新 ADX 与 ATR。
    """

    length = len(highs)
    if length < period + 2:
        return None, None

    tr_values: List[float] = []
    plus_dm_values: List[float] = []
    minus_dm_values: List[float] = []

    for idx in range(1, length):
        high = highs[idx]
        low = lows[idx]
        prev_high = highs[idx - 1]
        prev_low = lows[idx - 1]
        prev_close = closes[idx - 1]

        true_range = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        tr_values.append(true_range)

        up_move = high - prev_high
        down_move = prev_low - low

        plus_dm_values.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm_values.append(down_move if down_move > up_move and down_move > 0 else 0.0)

    if len(tr_values) < period:
        return None, None

    # Wilder smoothing initial values
    atr = sum(tr_values[:period]) / period
    plus_dm_smooth = sum(plus_dm_values[:period])
    minus_dm_smooth = sum(minus_dm_values[:period])

    atr_series: List[float] = [atr]
    dx_series: List[float] = []

    plus_di = 100 * (plus_dm_smooth / period) / atr if atr > 0 else 0.0
    minus_di = 100 * (minus_dm_smooth / period) / atr if atr > 0 else 0.0
    denominator = plus_di + minus_di
    dx = 100 * abs(plus_di - minus_di) / denominator if denominator > 0 else 0.0
    dx_series.append(dx)

    for idx in range(period, len(tr_values)):
        current_tr = tr_values[idx]
        atr = ((atr * (period - 1)) + current_tr) / period
        atr_series.append(atr)

        plus_dm_smooth = plus_dm_smooth - (plus_dm_smooth / period) + plus_dm_values[idx]
        minus_dm_smooth = minus_dm_smooth - (minus_dm_smooth / period) + minus_dm_values[idx]

        plus_di = 100 * (plus_dm_smooth / period) / atr if atr > 0 else 0.0
        minus_di = 100 * (minus_dm_smooth / period) / atr if atr > 0 else 0.0
        denominator = plus_di + minus_di
        dx = 100 * abs(plus_di - minus_di) / denominator if denominator > 0 else 0.0
        dx_series.append(dx)

    if not dx_series:
        return None, atr_series[-1] if atr_series else None

    if len(dx_series) < period:
        adx_value = float(np.mean(dx_series))
    else:
        adx = float(np.mean(dx_series[:period]))
        for idx in range(period, len(dx_series)):
            adx = ((adx * (period - 1)) + dx_series[idx]) / period
        adx_value = adx

    latest_atr = atr_series[-1] if atr_series else None
    return adx_value, latest_atr


# ------------------------------
# Strategy implementation
# ------------------------------


class DynamicVolatilityGridReaper:
    """Reaper strategy orchestrator.

    Reaper 策略调度器，负责初始化 SDK、指标更新、订单执行与风险控制。
    """

    def __init__(
        self,
        symbol: str,
        dry_run: bool,
        indicator_period: int,
        adx_threshold: float,
        atr_multiplier: float,
        grid_layers: int,
        total_notional: float,
        range_lookback: int,
        indicator_interval: int,
        order_sync_interval: int,
        stop_buffer: float,
        min_grid_spacing_pct: float,
        candle_lookback: int,
        reduce_only_exits: bool,
        performance_interval: int,
        performance_history: int,
        adaptive_enabled: bool,
        adaptive_low_vol: float,
        adaptive_high_vol: float,
        adaptive_multiplier_min: float,
        adaptive_multiplier_max: float,
        adaptive_layers_min: int,
        adaptive_layers_max: int,
        adaptive_adx_min: float,
        adaptive_adx_max: float,
        performance_db: Optional[str],
        trend_exit_mode: str,
        max_position_ratio: float = 0.4,  # Maximum position as ratio of total_notional
    ) -> None:
        self.symbol = symbol
        self.dry_run = dry_run
        self.indicator_period = indicator_period
        self.adx_threshold = adx_threshold
        self.atr_multiplier = atr_multiplier
        self.grid_layers = grid_layers
        self.total_notional = total_notional
        self.range_lookback = range_lookback
        self.indicator_interval = indicator_interval
        self.order_sync_interval = order_sync_interval
        self.stop_buffer = stop_buffer
        self.min_grid_spacing_pct = min_grid_spacing_pct
        self.candle_lookback = candle_lookback
        self.reduce_only_exits = reduce_only_exits
        self.performance_interval = performance_interval
        self.adaptive_enabled = adaptive_enabled
        self.adaptive_low_vol = adaptive_low_vol
        self.adaptive_high_vol = adaptive_high_vol
        self.adaptive_multiplier_min = adaptive_multiplier_min
        self.adaptive_multiplier_max = adaptive_multiplier_max
        self.adaptive_layers_min = adaptive_layers_min
        self.adaptive_layers_max = adaptive_layers_max
        self.adaptive_adx_min = adaptive_adx_min
        self.adaptive_adx_max = adaptive_adx_max
        self.max_position_ratio = max_position_ratio

        # Sanity guards for adaptive ranges
        if self.adaptive_layers_max < self.adaptive_layers_min:
            self.adaptive_layers_min, self.adaptive_layers_max = (
                self.adaptive_layers_max,
                self.adaptive_layers_min,
            )
        if self.adaptive_multiplier_max < self.adaptive_multiplier_min:
            self.adaptive_multiplier_min, self.adaptive_multiplier_max = (
                self.adaptive_multiplier_max,
                self.adaptive_multiplier_min,
            )
        if self.adaptive_adx_max < self.adaptive_adx_min:
            self.adaptive_adx_min, self.adaptive_adx_max = (
                self.adaptive_adx_max,
                self.adaptive_adx_min,
            )
        if self.adaptive_high_vol <= self.adaptive_low_vol:
            self.adaptive_high_vol = self.adaptive_low_vol + 1e-6

        allowed_trend_modes = {"pause", "cancel", "flatten"}
        normalized_trend_mode = trend_exit_mode.lower()
        if normalized_trend_mode not in allowed_trend_modes:
            raise ValueError(
                f"trend_exit_mode must be one of {sorted(allowed_trend_modes)}, got '{trend_exit_mode}'"
            )
        self.trend_exit_mode = normalized_trend_mode

        self.logger = get_strategy_logger("reaper")

        self.performance_db_path = (
            Path(performance_db).expanduser() if performance_db else None
        )
        self.performance_storage: Optional[PerformanceStorage] = None
        self.lighter: Optional[Lighter] = None
        self.market_manager: Optional[MarketDataManager] = None
        self.batch_manager: Optional[BatchOrderManager] = None
        self.price_ws: Optional[PriceWebSocketManager] = None
        self.market_constraints: Optional[MarketConstraints] = None

        self.latest_price: float = 0.0
        self.regime: str = "unknown"
        self.indicator_state: Optional[IndicatorState] = None
        self.desired_orders: List[OrderPlan] = []
        self.current_grid_spacing: float = 0.0
        self.stop_loss_price: Optional[float] = None

        self.grid_active: bool = False
        self.stop_triggered: bool = False
        self.shutdown: bool = False

        self.pending_flatten: bool = False

        self.state_lock = asyncio.Lock()
        self.plan_dirty = asyncio.Event()
        self.tasks: List[asyncio.Task] = []

        # Performance tracking
        # 保存策略表现历史，便于后续扩展可视化或写入持久化介质
        self.performance_history: Deque[PerformanceSnapshot] = deque(maxlen=max(1, performance_history))
        self.performance_baseline_equity: Optional[float] = None
        self.last_performance_update: float = 0.0
        self.active_atr_multiplier: float = atr_multiplier
        self.active_adx_threshold: float = adx_threshold
        self.active_grid_layers: int = grid_layers
        self._closed: bool = False

        # Position tracking for risk management
        self._last_long_position: float = 0.0
        self._last_short_position: float = 0.0

    # --- setup & teardown -------------------------------------------------

    async def setup(self) -> None:
        load_dotenv()
        api_key = os.getenv("LIGHTER_KEY")
        api_secret = os.getenv("LIGHTER_SECRET")
        api_key_index = int(os.getenv("API_KEY_INDEX", "1"))

        if not api_key or not api_secret:
            raise ValueError("LIGHTER_KEY and LIGHTER_SECRET must be set in environment")

        self.lighter = Lighter(key=api_key, secret=api_secret, api_key_index=api_key_index)
        await self.lighter.init_client()
        self.market_manager = MarketDataManager(self.lighter)
        self.batch_manager = BatchOrderManager(self.lighter, dry_run=self.dry_run)

        self.market_constraints = await self.market_manager.get_market_constraints(self.symbol)
        market_id = self.market_constraints.market_id
        self.logger.info(
            "Initialized market %s (id=%s) with min_quote=%.4f", 
            self.symbol,
            market_id,
            self.market_constraints.min_quote_amount,
        )

        self.price_ws = PriceWebSocketManager([market_id])
        self.price_ws.set_price_callback(self.on_price_update)

        await self.setup_performance_storage()
        await self.refresh_indicator_state(initial_bootstrap=True)
        if not self.latest_price and self.indicator_state:
            self.latest_price = self.indicator_state.close

        await self.update_performance_metrics(force=True)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self.shutdown = True
        self.plan_dirty.set()

        self.logger.info("Initiating graceful shutdown")

        if self.price_ws:
            self.price_ws.shutdown()

        for task in self.tasks:
            task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        await self.ensure_positions_flattened()

        if self.lighter:
            await self.lighter.cleanup()
        if self.performance_storage:
            await asyncio.to_thread(self.performance_storage.close)

        self.logger.info("Graceful shutdown complete")

    async def ensure_positions_flattened(self) -> None:
        if self.batch_manager or self.dry_run:
            try:
                await self.cancel_all_orders()
                await self.wait_until_no_open_orders()
                self.logger.info("Outstanding orders cancelled before exit")
            except Exception as exc:
                self.logger.warning("Failed to cancel open orders on shutdown: %s", exc)
        else:
            self.logger.debug("Batch manager unavailable; skipping order cancellation")

        if self.lighter or self.dry_run:
            try:
                await self.flatten_positions()
                await self.wait_until_flat()
                self.logger.info("Positions flattened before exit or already flat")
            except Exception as exc:
                self.logger.warning("Failed to flatten positions on shutdown: %s", exc)
        else:
            self.logger.debug("Client unavailable; skipping position flattening")

    # --- price handling ----------------------------------------------------

    def on_price_update(self, market_id: int, order_book: Dict) -> None:
        try:
            bids = order_book.get("bids", [])
            asks = order_book.get("asks", [])
            if not bids or not asks:
                return
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            mid_price = (best_bid + best_ask) / 2
            self.latest_price = mid_price
        except Exception as exc:
            self.logger.error("Failed to handle price update: %s", exc)

    # --- indicator processing ---------------------------------------------

    async def refresh_indicator_state(self, initial_bootstrap: bool = False) -> None:
        if not self.lighter:
            return

        try:
            response = await self.lighter.candlesticks(
                self.symbol,
                resolution="1m",
                count_back=self.candle_lookback,
                set_timestamp_to_end=False,
            )
        except Exception as exc:
            self.logger.warning("Failed to pull candlesticks: %s", exc)
            return

        if not isinstance(response, dict) or response.get("code") != 200:
            self.logger.warning("Unexpected candlestick response: %s", response)
            return

        candles = response.get("candlesticks", [])
        if len(candles) < self.indicator_period + 2:
            self.logger.warning("Not enough candles for indicator calculation (%d)", len(candles))
            return

        highs = np.array([float(c["high"]) for c in candles], dtype=float)
        lows = np.array([float(c["low"]) for c in candles], dtype=float)
        closes = np.array([float(c["close"]) for c in candles], dtype=float)

        adx_value, atr_value = calculate_adx_atr(highs, lows, closes, self.indicator_period)
        if adx_value is None or atr_value is None:
            self.logger.warning("Indicator calculation failed (adx=%s, atr=%s)", adx_value, atr_value)
            return

        lookback = min(self.range_lookback, len(highs))
        upper = float(np.max(highs[-lookback:]))
        lower = float(np.min(lows[-lookback:]))
        mid = (upper + lower) / 2
        last_close = float(closes[-1])

        state = IndicatorState(
            adx=float(adx_value),
            atr=float(atr_value),
            upper=upper,
            lower=lower,
            mid=mid,
            close=last_close,
            timestamp=time.time(),
        )

        min_spacing = abs(state.mid) * self.min_grid_spacing_pct

        # 根据波动率决定实时参数（若启用自适应开关）
        if self.adaptive_enabled:
            adaptive_multiplier, adaptive_layers, adaptive_adx = self.calculate_adaptive_parameters(state)
            self.active_atr_multiplier = adaptive_multiplier
            self.active_grid_layers = adaptive_layers
            self.active_adx_threshold = adaptive_adx
        else:
            self.active_atr_multiplier = self.atr_multiplier
            self.active_grid_layers = self.grid_layers
            self.active_adx_threshold = self.adx_threshold

        adaptive_spacing = max(min_spacing, state.atr * self.active_atr_multiplier)
        stop_loss = max(0.0, state.lower - adaptive_spacing * self.stop_buffer)

        new_regime = "range" if state.adx < self.active_adx_threshold else "trend"
        regime_changed = False

        async with self.state_lock:
            previous_regime = self.regime
            previous_grid_active = self.grid_active
            should_reset_stop = (
                self.stop_triggered and state.mid > stop_loss and state.adx < self.active_adx_threshold
            )
            if should_reset_stop:
                self.logger.info("Conditions met to reset stop state")
                self.stop_triggered = False

            if self.stop_triggered:
                plan: List[OrderPlan] = []
            elif new_regime == "range":
                plan = self.build_grid_plan(state, adaptive_spacing)
            else:
                plan = []

            self.indicator_state = state
            self.regime = new_regime
            regime_changed = previous_regime != new_regime

            if self.regime == "trend":
                if self.trend_exit_mode == "flatten" and previous_regime != "trend":
                    self.pending_flatten = True

            self.desired_orders = plan
            self.current_grid_spacing = adaptive_spacing
            self.stop_loss_price = stop_loss
            if self.regime == "trend" and self.trend_exit_mode == "pause":
                self.grid_active = previous_grid_active
            else:
                self.grid_active = bool(plan)

        if initial_bootstrap:
            self.plan_dirty.set()
        else:
            self.plan_dirty.set()

        if regime_changed:
            self.logger.info("Regime transition: %s -> %s", previous_regime, new_regime)

        self.logger.info(
            (
                "Indicator update: adx=%.2f atr=%.6f regime=%s upper=%.4f lower=%.4f spacing=%.6f "
                "stop=%.4f layers=%d atr_mult=%.3f adx_thr=%.2f"
            ),
            state.adx,
            state.atr,
            self.regime,
            state.upper,
            state.lower,
            adaptive_spacing,
            stop_loss,
            self.active_grid_layers,
            self.active_atr_multiplier,
            self.active_adx_threshold,
        )

    async def setup_performance_storage(self) -> None:
        if not self.performance_db_path:
            return

        storage = PerformanceStorage(self.performance_db_path)
        await asyncio.to_thread(storage.initialize)
        self.performance_storage = storage

        # 读取历史数据与基准值，方便重启后继续累计
        baseline = await asyncio.to_thread(storage.get_baseline)
        if baseline is not None:
            self.performance_baseline_equity = baseline

        recent_snapshots = await asyncio.to_thread(
            storage.fetch_recent, self.performance_history.maxlen
        )
        if recent_snapshots:
            self.performance_history.clear()
            self.performance_history.extend(recent_snapshots)
            self.last_performance_update = recent_snapshots[-1].timestamp
            if self.performance_baseline_equity is None:
                self.performance_baseline_equity = recent_snapshots[0].total_equity

    def build_grid_plan(self, state: IndicatorState, spacing: float) -> List[OrderPlan]:
        layers = max(1, self.active_grid_layers)
        levels: List[float] = [state.mid]
        for step in range(1, layers + 1):
            down = state.mid - spacing * step
            up = state.mid + spacing * step
            if down >= state.lower:
                levels.append(down)
            if up <= state.upper:
                levels.append(up)
        levels = sorted(set(levels))

        reference_price = self.latest_price or state.close
        buy_levels = [price for price in levels if price < reference_price]
        sell_levels = [price for price in levels if price >= reference_price]

        if not buy_levels and not sell_levels:
            return []

        constraints = self.market_constraints
        assert constraints is not None

        available_buy_quote = self.total_notional * 0.5
        available_sell_quote = self.total_notional * 0.5

        # Position limits to prevent excessive one-sided exposure
        max_position_value = self.total_notional * self.max_position_ratio

        # Get current positions (use sync version since we're not in async context)
        # We'll need to modify this to work with async, but for now we'll rely on last known positions
        # This is a temporary approach - ideally we'd make this function async
        current_long = getattr(self, '_last_long_position', 0.0)
        current_short = getattr(self, '_last_short_position', 0.0)

        long_position_value = current_long * reference_price
        short_position_value = current_short * reference_price

        # Reduce buy orders if long position is too large
        if long_position_value > max_position_value * 0.7:  # 70% threshold
            available_buy_quote *= 0.3  # Reduce buy capacity by 70%
            self.logger.warning(
                "Large long position detected (%.2f vs max %.2f), reducing buy orders",
                long_position_value, max_position_value
            )

        # Reduce sell orders if short position is too large
        if short_position_value > max_position_value * 0.7:  # 70% threshold
            available_sell_quote *= 0.3  # Reduce sell capacity by 70%
            self.logger.warning(
                "Large short position detected (%.2f vs max %.2f), reducing sell orders",
                short_position_value, max_position_value
            )

        if available_buy_quote < constraints.min_quote_amount:
            self.logger.warning(
                "Buy budget %.2f is below minimum quote %.2f; skipping buy orders",
                available_buy_quote,
                constraints.min_quote_amount,
            )
            buy_levels = []

        if available_sell_quote < constraints.min_quote_amount:
            self.logger.warning(
                "Sell budget %.2f is below minimum quote %.2f; skipping sell orders",
                available_sell_quote,
                constraints.min_quote_amount,
            )
            sell_levels = []

        orders: List[OrderPlan] = []

        if buy_levels:
            max_supported = max(1, int(available_buy_quote // constraints.min_quote_amount))
            buy_levels = buy_levels[-max_supported:]
            per_order_quote = available_buy_quote / len(buy_levels)

            for price in buy_levels:
                formatted_price = self.market_manager.format_price(price, self.symbol)  # type: ignore[arg-type]
                quantity, valid, msg = self.market_manager.calculate_quantity_for_quote_amount(
                    formatted_price,
                    per_order_quote,
                    self.symbol,
                )
                if not valid or quantity <= 0:
                    self.logger.debug("Skipping buy order at %.6f: %s", formatted_price, msg)
                    continue
                orders.append(
                    OrderPlan(
                        side="buy",
                        price=formatted_price,
                        quantity=quantity,
                    )
                )

        if sell_levels:
            max_supported = max(1, int(available_sell_quote // constraints.min_quote_amount))
            sell_levels = sell_levels[: max_supported]
            per_order_quote = available_sell_quote / len(sell_levels)

            for price in sell_levels:
                formatted_price = self.market_manager.format_price(price, self.symbol)  # type: ignore[arg-type]
                quantity, valid, msg = self.market_manager.calculate_quantity_for_quote_amount(
                    formatted_price,
                    per_order_quote,
                    self.symbol,
                )
                if not valid or quantity <= 0:
                    self.logger.debug("Skipping sell order at %.6f: %s", formatted_price, msg)
                    continue
                orders.append(
                    OrderPlan(
                        side="sell",
                        price=formatted_price,
                        quantity=quantity,
                    )
                )

        return orders

    # --- order management --------------------------------------------------

    async def order_loop(self) -> None:
        while not self.shutdown:
            try:
                await asyncio.wait_for(self.plan_dirty.wait(), timeout=self.order_sync_interval)
            except asyncio.TimeoutError:
                pass
            if self.shutdown:
                break
            self.plan_dirty.clear()

            async with self.state_lock:
                regime = self.regime
                stop_price = self.stop_loss_price
                stop_active = self.stop_triggered
                plan_snapshot = list(self.desired_orders)
                spacing = self.current_grid_spacing

            if stop_active:
                continue

            if regime != "range" or not plan_snapshot:
                if await self.cancel_all_orders_if_needed():
                    async with self.state_lock:
                        self.grid_active = False
                if regime == "trend" and self.trend_exit_mode == "flatten":
                    await self.ensure_flatten_positions_if_needed()
                await asyncio.sleep(1)
                continue

            if await self.sync_orders(plan_snapshot):
                async with self.state_lock:
                    self.grid_active = True

            if stop_price and self.latest_price and self.latest_price <= stop_price:
                await self.handle_stop_loss(stop_price, spacing)

    async def cancel_all_orders_if_needed(self) -> bool:
        # Only cancel orders if we have active orders AND we should cancel them
        existing_orders = await self.fetch_active_orders()
        if not existing_orders:
            return False

        cancel_reason: Optional[str] = None
        if self.regime == "trend":
            if self.trend_exit_mode in {"cancel", "flatten"}:
                cancel_reason = f"trend-{self.trend_exit_mode}"
        elif not self.grid_active:
            cancel_reason = "inactive-grid"

        if cancel_reason:
            self.logger.info(
                "Deactivating grid orders (%s) - regime=%s, active_orders=%d",
                cancel_reason,
                self.regime,
                len(existing_orders),
            )
            await self.cancel_all_orders()
            return True

        return False

    async def ensure_flatten_positions_if_needed(self) -> None:
        if self.trend_exit_mode != "flatten" and not self.pending_flatten:
            return

        flatten_required = False

        async with self.state_lock:
            if self.pending_flatten:
                flatten_required = True
                self.pending_flatten = False

        # Only check positions via API if we don't already know we need to flatten
        # and we're not in dry run mode
        if not flatten_required and not self.dry_run:
            long_qty, short_qty = await self.fetch_positions()
            # Get minimum base amount to avoid checking dust positions
            constraints = self.market_constraints
            min_base_amount = constraints.min_base_amount if constraints else 0.0

            # Only consider positions above minimum trading amount as requiring flattening
            flatten_required = (long_qty >= min_base_amount) or (short_qty >= min_base_amount)

            if long_qty > 0 and long_qty < min_base_amount:
                self.logger.debug("Ignoring dust long position %.6f (below minimum %.6f)", long_qty, min_base_amount)
            if short_qty > 0 and short_qty < min_base_amount:
                self.logger.debug("Ignoring dust short position %.6f (below minimum %.6f)", short_qty, min_base_amount)

        if not flatten_required:
            return

        self.logger.info("Flattening residual exposure while in trend regime")

        await self.flatten_positions()
        await self.wait_until_flat()

        # Only verify positions after flatten attempt if not in dry run
        if not self.dry_run:
            long_qty, short_qty = await self.fetch_positions()
            constraints = self.market_constraints
            min_base_amount = constraints.min_base_amount if constraints else 0.0

            # Only consider significant positions as requiring retry
            significant_positions = (long_qty >= min_base_amount) or (short_qty >= min_base_amount)

            if significant_positions:
                self.logger.warning(
                    "Significant positions still detected after flatten attempt (long=%.6f, short=%.6f)",
                    long_qty,
                    short_qty,
                )
                async with self.state_lock:
                    self.pending_flatten = True
            elif long_qty > 0 or short_qty > 0:
                self.logger.info(
                    "Only dust positions remain after flatten (long=%.6f, short=%.6f), ignoring",
                    long_qty,
                    short_qty,
                )

    async def sync_orders(self, desired_orders: List[OrderPlan]) -> bool:
        existing = await self.fetch_active_orders()
        constraints = self.market_constraints
        price_precision = constraints.price_precision if constraints else 6

        desired_map: Dict[str, OrderPlan] = {
            order.key(price_precision): order for order in desired_orders
        }
        existing_map: Dict[str, OrderInfo] = {
            self.order_key(info, price_precision): info for info in existing
        }

        to_cancel = [info for key, info in existing_map.items() if key not in desired_map]
        to_create = [order for key, order in desired_map.items() if key not in existing_map]

        if to_cancel:
            await self.cancel_orders([info.order_id for info in to_cancel])
        if to_create:
            for order in to_create:
                await self.place_limit_order(order)
        return True

    def order_key(self, order: OrderInfo, price_precision: int) -> str:
        formatted_price = format(order.price, f".{price_precision}f")
        return f"{order.side}:{formatted_price}"

    async def fetch_active_orders(self) -> List[OrderInfo]:
        if not self.lighter:
            return []
        try:
            response = await self.lighter.account_active_orders(self.symbol)
        except Exception as exc:
            self.logger.warning("Failed to fetch active orders: %s", exc)
            return []

        if not isinstance(response, dict) or response.get("code") != 200:
            self.logger.warning("Unexpected active order response: %s", response)
            return []

        orders: List[OrderInfo] = []
        for item in response.get("orders", []):
            try:
                order_id = str(item.get("order_id", item.get("order_index", "")))
                if not order_id:
                    continue
                status = str(item.get("status", "")).lower()
                if status not in {"active", "open", "pending", "live"}:
                    continue
                remaining = float(item.get("remaining_base_amount", "0"))
                if remaining <= 0:
                    continue
                info = OrderInfo(
                    order_id=order_id,
                    symbol=self.symbol,
                    side="sell" if item.get("is_ask", False) else "buy",
                    price=float(item.get("price", "0")),
                    quantity=float(item.get("base_amount", "0")),
                    remaining_quantity=remaining,
                    status=status,
                    timestamp=time.time(),
                )
                orders.append(info)
            except (TypeError, ValueError) as exc:
                self.logger.debug("Failed to parse order entry: %s", exc)
        return orders

    async def cancel_all_orders(self) -> None:
        if self.dry_run:
            self.logger.info("DRY RUN - cancel all orders")
            return
        if not self.batch_manager:
            self.logger.debug("Batch manager not initialized; skipping cancel_all_orders")
            return

        # Call cancel_all_orders_safe and log the result
        result = await self.batch_manager.cancel_all_orders_safe()  # type: ignore[union-attr]

        if result['success']:
            self.logger.info("Successfully cancelled %d orders using %s method",
                           result.get('cancelled_count', 0), result.get('method', 'unknown'))
        else:
            self.logger.error("Failed to cancel orders: %s", result.get('error', 'Unknown error'))

    async def wait_until_no_open_orders(self, timeout: float = 12.0) -> None:
        if self.dry_run or not self.lighter:
            return
        deadline = time.time() + timeout
        last_count = None
        while time.time() < deadline:
            active = await self.fetch_active_orders()
            count = len(active)
            if count == 0:
                return
            if last_count != count:
                self.logger.info("Waiting for open orders to clear (%d remaining)", count)
                last_count = count
            await asyncio.sleep(0.5)
        if last_count:
            self.logger.warning("Timed out waiting for %d open orders to cancel", last_count)

    async def cancel_orders(self, order_ids: List[str]) -> None:
        if not order_ids:
            return
        if self.dry_run:
            for order_id in order_ids:
                self.logger.info("DRY RUN - cancel order %s", order_id)
            return
        for order_id in order_ids:
            try:
                await self.lighter.cancel_order(self.symbol, order_id)  # type: ignore[union-attr]
                await asyncio.sleep(0.05)
            except Exception as exc:
                self.logger.warning("Failed to cancel %s: %s", order_id, exc)

    async def place_limit_order(self, order: OrderPlan) -> None:
        if order.quantity <= 0:
            return
        amount = order.quantity if order.side == "buy" else -order.quantity
        if self.dry_run:
            self.logger.info(
                "DRY RUN - place %s %.6f @ %.6f",
                order.side,
                order.quantity,
                order.price,
            )
            return
        try:
            await self.lighter.limit_order(  # type: ignore[union-attr]
                ticker=self.symbol,
                amount=amount,
                price=order.price,
                tif="GTC",
                reduce_only=self.reduce_only_exits if order.side == "sell" else False,
            )
            await asyncio.sleep(0.05)
        except Exception as exc:
            self.logger.error("Limit order failed (%s %.6f @ %.6f): %s", order.side, order.quantity, order.price, exc)

    # --- stop-loss handling ------------------------------------------------

    async def handle_stop_loss(self, trigger_price: float, spacing: float) -> None:
        async with self.state_lock:
            if self.stop_triggered:
                return
            self.stop_triggered = True
            self.desired_orders = []
            self.grid_active = False

        self.plan_dirty.set()
        self.logger.warning(
            "Master stop-loss engaged at %.6f (last=%.6f)",
            trigger_price,
            self.latest_price,
        )

        await self.cancel_all_orders()
        await self.flatten_positions()

        cooldown = max(5.0, spacing * 2)
        self.logger.info("Applying cooldown %.1f seconds before attempting redeploy", cooldown)
        await asyncio.sleep(cooldown)

    async def flatten_positions(self) -> None:
        if self.dry_run:
            self.logger.info("DRY RUN - flatten positions skipped")
            return
        if not self.lighter or not self.market_manager:
            self.logger.debug("Client not initialized; skipping flatten_positions")
            return

        # Get minimum base amount for the current symbol
        constraints = self.market_constraints
        min_base_amount = constraints.min_base_amount if constraints else 0.0

        long_qty, short_qty = await self.fetch_positions()
        if long_qty > 0:
            qty = self.market_manager.format_quantity(long_qty, self.symbol)  # type: ignore[arg-type]
            if qty > 0:
                # Check if position is above minimum trading amount
                if qty < min_base_amount:
                    self.logger.info("Long position %.6f below minimum %.6f, ignoring dust position", qty, min_base_amount)
                else:
                    self.logger.info("Closing long position %.6f via market sell", qty)
                    if not await self._submit_market_order(-qty, reduce_only=True):
                        self.logger.info("Retrying long close without reduce-only flag")
                        if not await self._submit_market_order(-qty, reduce_only=False):
                            async with self.state_lock:
                                self.pending_flatten = True
        if short_qty > 0:
            qty = self.market_manager.format_quantity(short_qty, self.symbol)  # type: ignore[arg-type]
            if qty > 0:
                # Check if position is above minimum trading amount
                if qty < min_base_amount:
                    self.logger.info("Short position %.6f below minimum %.6f, ignoring dust position", qty, min_base_amount)
                else:
                    self.logger.info("Closing short position %.6f via market buy", qty)
                    if not await self._submit_market_order(qty, reduce_only=True):
                        self.logger.info("Retrying short close without reduce-only flag")
                        if not await self._submit_market_order(qty, reduce_only=False):
                            async with self.state_lock:
                                self.pending_flatten = True

    async def _submit_market_order(self, amount: float, reduce_only: bool) -> bool:
        try:
            order, response, error = await self.lighter.market_order(  # type: ignore[union-attr]
                self.symbol,
                amount,
                reduce_only=reduce_only,
            )
            if error:
                raise RuntimeError(f"market_order error: {error}")
            if response is None:
                raise RuntimeError("market_order returned no response")
            if hasattr(response, "code") and response.code != 200:
                raise RuntimeError(
                    f"market_order rejected (code={response.code}, message={getattr(response, 'message', '')})"
                )
            self.logger.info(
                "Submitted market order (reduce_only=%s) response_code=%s tx_hash=%s",
                reduce_only,
                getattr(response, "code", None),
                getattr(response, "tx_hash", None),
            )
            return True
        except Exception as exc:
            self.logger.error(
                "Market order failed (reduce_only=%s amount=%.6f): %s",
                reduce_only,
                amount,
                exc,
                exc_info=True,
            )
            return False

    async def wait_until_flat(self, timeout: float = 15.0) -> None:
        if self.dry_run or not self.lighter:
            return
        deadline = time.time() + timeout
        last_state: Optional[Tuple[float, float]] = None

        # Get minimum base amount to consider dust positions
        constraints = self.market_constraints
        min_base_amount = constraints.min_base_amount if constraints else 0.0

        while time.time() < deadline:
            long_qty, short_qty = await self.fetch_positions()
            state = (long_qty, short_qty)

            # Consider positions "flat" if they're below minimum trading amounts
            significant_long = long_qty >= min_base_amount
            significant_short = short_qty >= min_base_amount

            if not significant_long and not significant_short:
                if long_qty > 0 or short_qty > 0:
                    self.logger.info(
                        "Positions considered flat with only dust remaining (long=%.6f, short=%.6f)",
                        long_qty,
                        short_qty,
                    )
                return

            if last_state != state:
                self.logger.info(
                    "Waiting for positions to flatten (long=%.6f, short=%.6f)",
                    long_qty,
                    short_qty,
                )
                last_state = state
            await asyncio.sleep(0.75)

        if last_state:
            long_qty, short_qty = last_state
            significant_long = long_qty >= min_base_amount
            significant_short = short_qty >= min_base_amount

            if significant_long or significant_short:
                self.logger.warning(
                    "Timed out waiting for significant positions to flatten (long=%.6f, short=%.6f)",
                    long_qty,
                    short_qty,
                )
            else:
                self.logger.info(
                    "Timed out but only dust positions remain (long=%.6f, short=%.6f)",
                    long_qty,
                    short_qty,
                )

    async def fetch_positions(self) -> Tuple[float, float]:
        if self.dry_run:
            return 0.0, 0.0
        try:
            response = await self.lighter.account(by="l1_address")  # type: ignore[union-attr]
        except Exception as exc:
            self.logger.warning("Failed to fetch account positions: %s", exc)
            return 0.0, 0.0
        if not isinstance(response, dict) or response.get("code") != 200:
            return 0.0, 0.0
        accounts = response.get("accounts", [])
        if not accounts:
            return 0.0, 0.0
        account = accounts[0]
        positions = account.get("positions", [])
        long_qty = 0.0
        short_qty = 0.0
        for pos in positions:
            if pos.get("symbol") != self.symbol:
                continue
            size = abs(float(pos.get("position", 0)))
            sign = pos.get("sign", 1)
            if sign > 0:
                long_qty = size
            else:
                short_qty = size
            break
        return long_qty, short_qty

    # --- indicator loop ----------------------------------------------------

    async def indicator_loop(self) -> None:
        while not self.shutdown:
            await self.refresh_indicator_state()
            await self.update_performance_metrics()
            await asyncio.sleep(self.indicator_interval)

    async def update_performance_metrics(self, force: bool = False) -> None:
        if self.performance_interval <= 0:
            return

        now = time.time()
        if not force and now - self.last_performance_update < self.performance_interval:
            return

        overview = await self.fetch_account_overview()
        if overview is None:
            return

        equity = overview["equity"]
        raw_realized = overview.get("realized", 0.0)
        unrealized = overview["unrealized"]

        if self.performance_baseline_equity is None and equity > 0:
            self.performance_baseline_equity = equity
            await self.persist_baseline(equity)

        baseline = self.performance_baseline_equity or equity
        synthetic_realized = equity - baseline - unrealized

        realized = raw_realized
        if abs(realized) < 1e-6 and abs(synthetic_realized) > 1e-6:
            realized = synthetic_realized
            self.logger.debug(
                "Using synthetic realized PnL derived from equity. raw=%.6f synthetic=%.6f",
                raw_realized,
                synthetic_realized,
            )

        roi_pct = ((equity - baseline) / baseline * 100) if baseline else 0.0

        snapshot = PerformanceSnapshot(
            timestamp=now,
            total_equity=equity,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            roi_pct=roi_pct,
        )

        self.performance_history.append(snapshot)
        self.last_performance_update = now

        long_qty = 0.0
        short_qty = 0.0
        if not self.dry_run:
            long_qty, short_qty = await self.fetch_positions()
            if self.market_manager:
                long_qty = self.market_manager.format_quantity(long_qty, self.symbol)  # type: ignore[arg-type]
                short_qty = self.market_manager.format_quantity(short_qty, self.symbol)  # type: ignore[arg-type]

        # Update cached positions for risk management
        self._last_long_position = long_qty
        self._last_short_position = short_qty

        self.logger.info(
            "Performance snapshot: equity=%.2f roi=%.2f%% realized=%.2f unrealized=%.2f long=%.6f short=%.6f",
            snapshot.total_equity,
            snapshot.roi_pct,
            snapshot.realized_pnl,
            snapshot.unrealized_pnl,
            long_qty,
            short_qty,
        )

        await self.persist_performance_snapshot(snapshot, baseline)

    async def fetch_account_overview(self) -> Optional[Dict[str, float]]:
        if self.dry_run:
            return {
                "equity": 0.0,
                "realized": 0.0,
                "unrealized": 0.0,
            }

        try:
            response = await self.lighter.account(by="l1_address")  # type: ignore[union-attr]
        except Exception as exc:
            self.logger.debug("Failed to fetch account overview: %s", exc)
            return None

        if not isinstance(response, dict) or response.get("code") != 200:
            return None

        accounts = response.get("accounts", [])
        if not accounts:
            return None

        account = accounts[0]
        equity = float(account.get("total_asset_value", 0.0))

        realized_candidates = [
            account.get("realized_pnl"),
            account.get("total_realized_pnl"),
        ]
        realized = next((float(value) for value in realized_candidates if value is not None), 0.0)

        positions = account.get("positions", [])
        unrealized = 0.0
        position_realized = 0.0
        for position in positions:
            try:
                unrealized += float(position.get("unrealized_pnl", 0.0))
                if position.get("realized_pnl") is not None:
                    position_realized += float(position.get("realized_pnl", 0.0))
            except (TypeError, ValueError):
                continue

        if abs(realized) < abs(position_realized):
            realized = position_realized

        return {
            "equity": equity,
            "realized": realized,
            "unrealized": unrealized,
        }

    def calculate_adaptive_parameters(self, state: IndicatorState) -> Tuple[float, int, float]:
        mid_price = max(state.mid, 1e-9)
        atr_ratio = state.atr / mid_price

        low = max(1e-9, self.adaptive_low_vol)
        high = max(low + 1e-9, self.adaptive_high_vol)
        clamped = min(max(atr_ratio, low), high)

        ratio = (clamped - low) / (high - low)

        multiplier = (
            self.adaptive_multiplier_min
            + ratio * (self.adaptive_multiplier_max - self.adaptive_multiplier_min)
        )
        layers_float = (
            self.adaptive_layers_max
            - ratio * (self.adaptive_layers_max - self.adaptive_layers_min)
        )
        layers = max(1, int(round(layers_float)))

        adx_threshold = (
            self.adaptive_adx_min
            + (1 - ratio) * (self.adaptive_adx_max - self.adaptive_adx_min)
        )

        return multiplier, layers, adx_threshold

    async def persist_baseline(self, baseline: float) -> None:
        if not self.performance_storage:
            return
        await asyncio.to_thread(self.performance_storage.set_baseline, baseline)

    async def persist_performance_snapshot(
        self, snapshot: PerformanceSnapshot, baseline: float
    ) -> None:
        if not self.performance_storage:
            return
        await asyncio.to_thread(
            self.performance_storage.insert_snapshot,
            snapshot,
            baseline,
        )

    # --- public entry point ------------------------------------------------

    async def run(self) -> None:
        await self.setup()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.initiate_shutdown(s)))
            except NotImplementedError:
                # Signal handlers are not available on some platforms (e.g., Windows)
                pass

        if self.price_ws:
            price_task = asyncio.create_task(self.price_ws.initialize_and_run())
            self.tasks.append(price_task)
        indicator_task = asyncio.create_task(self.indicator_loop())
        orders_task = asyncio.create_task(self.order_loop())
        self.tasks.extend([indicator_task, orders_task])

        await asyncio.gather(indicator_task, orders_task)

    async def initiate_shutdown(self, sig: signal.Signals) -> None:
        if self.shutdown:
            return
        self.logger.info("Received signal %s, shutting down", sig.name)
        await self.close()


# ------------------------------
# CLI handling
# ------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dynamic volatility grid strategy (Reaper)")
    parser.add_argument("--symbol", default="BTC", help="Trading symbol (default: BTC)")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending live orders")
    parser.add_argument("--indicator-period", type=int, default=14, help="ADX/ATR period (default: 14)")
    parser.add_argument("--adx-threshold", type=float, default=20.0, help="ADX regime threshold (lowered from 25 for earlier trend detection)")
    parser.add_argument("--atr-multiplier", type=float, default=0.3, help="Grid spacing multiplier applied to ATR (increased from 0.2 for wider spacing)")
    parser.add_argument("--grid-layers", type=int, default=3, help="Grid layers per side around the midpoint (reduced from 5 to limit exposure)")
    parser.add_argument("--total-notional", type=float, default=1000.0, help="Total quote capital to deploy across grid")
    parser.add_argument("--range-lookback", type=int, default=120, help="Candles used to detect price channel bounds")
    parser.add_argument("--indicator-interval", type=int, default=60, help="Seconds between indicator refreshes")
    parser.add_argument("--order-sync-interval", type=int, default=15, help="Seconds between order reconciliation attempts")
    parser.add_argument("--stop-buffer", type=float, default=2.0, help="Multiplier applied to spacing for stop distance (increased from 1.5 for safer stop)")
    parser.add_argument("--min-grid-spacing-pct", type=float, default=0.002, help="Minimum grid spacing as %% of midpoint price (doubled for wider spacing)")
    parser.add_argument("--candle-lookback", type=int, default=200, help="Total candles requested each refresh")
    parser.add_argument("--reduce-only-exits", action="store_true", help="Submit sell orders as reduce-only")
    parser.add_argument("--performance-interval", type=int, default=120, help="Seconds between performance snapshots (0 disables tracking)")
    parser.add_argument("--performance-history", type=int, default=200, help="Maximum performance snapshots to retain in memory")
    parser.add_argument(
        "--performance-db",
        default="data/reaper_performance.db",
        help="Path to SQLite database storing performance snapshots (empty string disables persistence)",
    )
    parser.add_argument("--enable-adaptive", action="store_true", default=True, help="Enable adaptive grid parameters based on volatility")
    parser.add_argument("--adaptive-low-vol", type=float, default=0.002, help="ATR/mid threshold considered low volatility (doubled for more conservative bounds)")
    parser.add_argument("--adaptive-high-vol", type=float, default=0.015, help="ATR/mid threshold considered high volatility (increased for more conservative bounds)")
    parser.add_argument("--adaptive-multiplier-min", type=float, default=0.25, help="Minimum ATR multiplier when volatility is low (increased from 0.15)")
    parser.add_argument("--adaptive-multiplier-max", type=float, default=0.5, help="Maximum ATR multiplier when volatility is high (increased from 0.4)")
    parser.add_argument("--adaptive-layers-min", type=int, default=2, help="Minimum grid layers when volatility is high (reduced from 3)")
    parser.add_argument("--adaptive-layers-max", type=int, default=4, help="Maximum grid layers when volatility is low (reduced from 8)")
    parser.add_argument("--adaptive-adx-min", type=float, default=20.0, help="Minimum ADX threshold when volatility is high")
    parser.add_argument("--adaptive-adx-max", type=float, default=30.0, help="Maximum ADX threshold when volatility is low")
    parser.add_argument(
        "--trend-exit-mode",
        choices=["pause", "cancel", "flatten"],
        default="cancel",
        help="How to manage existing orders when ADX signals a trend (cancel: remove orders to reduce exposure, pause: keep existing grid, flatten: cancel and close positions)",
    )
    parser.add_argument("--max-position-ratio", type=float, default=0.4, help="Maximum position value as ratio of total notional (0.4 = 40%% max exposure)")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    strategy = DynamicVolatilityGridReaper(
        symbol=args.symbol,
        dry_run=args.dry_run,
        indicator_period=args.indicator_period,
        adx_threshold=args.adx_threshold,
        atr_multiplier=args.atr_multiplier,
        grid_layers=args.grid_layers,
        total_notional=args.total_notional,
        range_lookback=args.range_lookback,
        indicator_interval=args.indicator_interval,
        order_sync_interval=args.order_sync_interval,
        stop_buffer=args.stop_buffer,
        min_grid_spacing_pct=args.min_grid_spacing_pct,
        candle_lookback=args.candle_lookback,
        reduce_only_exits=args.reduce_only_exits,
        performance_interval=args.performance_interval,
        performance_history=args.performance_history,
        adaptive_enabled=args.enable_adaptive,
        adaptive_low_vol=args.adaptive_low_vol,
        adaptive_high_vol=args.adaptive_high_vol,
        adaptive_multiplier_min=args.adaptive_multiplier_min,
        adaptive_multiplier_max=args.adaptive_multiplier_max,
        adaptive_layers_min=args.adaptive_layers_min,
        adaptive_layers_max=args.adaptive_layers_max,
        adaptive_adx_min=args.adaptive_adx_min,
        adaptive_adx_max=args.adaptive_adx_max,
        performance_db=args.performance_db or None,
        trend_exit_mode=args.trend_exit_mode,
        max_position_ratio=args.max_position_ratio,
    )
    try:
        await strategy.run()
    finally:
        await strategy.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
