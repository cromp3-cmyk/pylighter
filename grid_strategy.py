"""
简化网格交易策略 - NUR LONG mit Gewinnziel

Strategie:
- Kauft bei jedem 0.5% Fall nach (DCA)
- Verkauft erst bei 1% Gewinn (Take-Profit)
- Startet dann neu mit 0.5% Fall
- NUR LONG - keine Shorts!
"""

import os
import asyncio
import time
import argparse
import signal
import lighter
from dotenv import load_dotenv

# 使用新的 SDK 工具
from pylighter.client import Lighter
from pylighter.websocket_manager import PriceWebSocketManager
from pylighter.order_manager import OrderSyncManager, BatchOrderManager
from pylighter.market_utils import MarketDataManager

# 使用日志工具库
from utils.logger_config import get_strategy_logger

# 加载环境变量
load_dotenv()

# 初始化日志器
logger = get_strategy_logger("dca_grid")

# ==================== 配置 ====================
COIN_NAME = "XRP"

# 🎯 核心参数
GRID_SPACING = 0.005          # 0.5% Nachkauf-Abstand
TAKE_PROFIT_PERCENT = 0.01    # 1.0% Gewinnziel
INITIAL_QUANTITY = 100.0      # $100 pro Order
LEVERAGE = 10                 # 10x Hebel
MAX_ORDERS = 20               # Maximal 20 Nachkäufe
PRICE_UPDATE_THRESHOLD = 0.001 # 0.1% Preis-Update

# 🔧 API优化参数
POSITION_SYNC_INTERVAL = 60   # 1 Minute
ORDER_SYNC_INTERVAL = 30      # 30 Sekunden
STATS_DISPLAY_INTERVAL = 300  # 5 Minuten
LOG_THROTTLE_FACTOR = 10

class DCAGridBot:
    """Nur Long DCA-Grid Bot - Kauft bei Dips, verkauft bei Gewinn"""

    def __init__(self, dry_run=False, symbol=None, max_orders=None, grid_spacing=None, order_amount=None):
        self.dry_run = dry_run
        self.symbol = symbol or COIN_NAME
        self.shutdown_requested = False

        # Strategie-Parameter
        self.max_orders = max_orders or MAX_ORDERS
        self.grid_spacing = grid_spacing or GRID_SPACING
        self.initial_quantity = order_amount or INITIAL_QUANTITY
        self.take_profit_percent = TAKE_PROFIT_PERCENT

        # Core components
        self.lighter = None
        self.market_manager = None
        self.order_manager = None
        self.batch_manager = None
        self.price_ws = None

        # 状态
        self.long_position = 0
        self.avg_entry_price = 0
        self.latest_price = 0
        self.best_bid_price = None
        self.best_ask_price = None
        self.total_asset_value = 1000.0

        # Order tracking
        self.initial_quantity_per_order = 0
        self.last_order_price = 0

        # 统计
        self.total_trades = 0
        self.total_buys = 0
        self.total_sells = 0

    async def setup(self):
        """初始化所有组件"""
        api_key = os.getenv("LIGHTER_KEY")
        api_secret = os.getenv("LIGHTER_SECRET")
        if not api_key or not api_secret:
            raise ValueError("请设置 LIGHTER_KEY 和 LIGHTER_SECRET 环境变量")

        self.lighter = Lighter(key=api_key, secret=api_secret)
        await self.lighter.init_client()

        self.market_manager = MarketDataManager(self.lighter)
        self.order_manager = OrderSyncManager(self.lighter)
        self.batch_manager = BatchOrderManager(self.lighter, dry_run=self.dry_run)

        constraints = await self.market_manager.get_market_constraints(self.symbol)
        logger.info(f"✅ {self.symbol} 约束: 最小订单=${constraints.min_quote_amount}")

        await self.analyze_startup_state()

        market_id = self.lighter.ticker_to_idx[self.symbol]
        self.price_ws = PriceWebSocketManager([market_id])
        self.price_ws.set_price_callback(self.on_price_update)

        logger.info(f"✅ DCA Grid Bot初始化完成: {self.symbol}")
        logger.info(f"📊 Strategie: Nachkauf bei -{self.grid_spacing*100:.1f}%, Verkauf bei +{self.take_profit_percent*100:.1f}%")

    async def get_account_stats(self) -> dict:
        """获取官方账户统计信息"""
        try:
            response = await self.lighter.account(by='l1_address')
            if not isinstance(response, dict) or response.get('code') != 200:
                return {}

            accounts = response.get('accounts', [])
            if not accounts:
                return {}

            account = accounts[0]
            positions = account.get('positions', [])

            current_position = None
            for pos in positions:
                if pos.get('symbol') == self.symbol:
                    current_position = pos
                    break

            stats = {
                'account_info': {
                    'total_asset_value': float(account.get('total_asset_value', 0)),
                    'collateral': float(account.get('collateral', 0)),
                    'available_balance': float(account.get('available_balance', 0)),
                },
                'current_position': {}
            }

            if current_position:
                stats['current_position'] = {
                    'position': float(current_position.get('position', 0)),
                    'sign': current_position.get('sign', 1),
                    'avg_entry_price': float(current_position.get('avg_entry_price', 0)),
                    'unrealized_pnl': float(current_position.get('unrealized_pnl', 0)),
                    'realized_pnl': float(current_position.get('realized_pnl', 0)),
                }

            return stats
        except Exception as e:
            logger.error(f"获取账户统计失败: {e}")
            return {}

    def print_account_stats(self, stats: dict) -> None:
        """打印账户统计信息"""
        if not stats:
            return

        account_info = stats.get('account_info', {})
        current_pos = stats.get('current_position', {})

        logger.info("📊 ===== 账户统计 =====")
        logger.info(f"💰 总资产: ${account_info.get('total_asset_value', 0):.2f}")
        logger.info(f"💳 可用余额: ${account_info.get('available_balance', 0):.2f}")

        if current_pos:
            logger.info(f"📈 {self.symbol} 持仓:")
            logger.info(f"   数量: {current_pos.get('position', 0)}")
            logger.info(f"   均价: ${current_pos.get('avg_entry_price', 0):.6f}")
            logger.info(f"   P&L: ${current_pos.get('unrealized_pnl', 0):.2f}")
            logger.info(f"   Realisiert: ${current_pos.get('realized_pnl', 0):.2f}")

        logger.info("=" * 50)

    async def analyze_startup_state(self):
        """启动状态分析"""
        logger.info("📊 分析启动状态...")

        try:
            stats = await self.get_account_stats()
            account_info = stats.get('account_info', {})
            self.total_asset_value = account_info.get('total_asset_value', 1000.0)
            logger.info(f"✅ 账户总价值: ${self.total_asset_value:.2f}")

            self.print_account_stats(stats)

            current_position = stats.get('current_position', {})
            if current_position:
                position_value = float(current_position.get('position', 0))
                sign_value = current_position.get('sign', 1)

                if position_value != 0 and sign_value > 0:
                    self.long_position = abs(position_value)
                    self.avg_entry_price = float(current_position.get('avg_entry_price', 0))
                    logger.info(f"📈 Bestehende Position: {self.long_position} @ ${self.avg_entry_price:.6f}")

        except Exception as e:
            logger.warning(f"获取账户信息失败: {e}")

        await self.order_manager.sync_orders_from_api(self.symbol)
        tracker = self.order_manager.get_tracker(self.symbol)
        counts = tracker.get_order_counts()
        logger.info(f"启动订单: 活跃={counts['total_active']}")

    async def get_positions(self):
        """获取持仓"""
        if self.dry_run:
            return self.long_position, self.avg_entry_price

        try:
            stats = await self.get_account_stats()
            current_position = stats.get('current_position', {})
            if current_position:
                position_value = float(current_position.get('position', 0))
                sign_value = current_position.get('sign', 1)
                if position_value != 0 and sign_value > 0:
                    return abs(position_value), float(current_position.get('avg_entry_price', 0))
            return 0, 0
        except Exception:
            return self.long_position, self.avg_entry_price

    def on_price_update(self, market_id: int, order_book: dict):
        """价格更新回调"""
        try:
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])

            if bids and asks:
                self.best_bid_price = float(bids[0]['price'])
                self.best_ask_price = float(asks[0]['price'])
                self.latest_price = (self.best_bid_price + self.best_ask_price) / 2

                if self.initial_quantity_per_order == 0:
                    self.update_initial_quantities()

        except Exception as e:
            logger.error(f"价格更新处理失败: {e}")

    def update_initial_quantities(self):
        """更新初始数量"""
        if self.latest_price > 0:
            quantity, is_valid, msg = self.market_manager.calculate_quantity_for_quote_amount(
                self.latest_price, self.initial_quantity, self.symbol
            )
            if is_valid:
                self.initial_quantity_per_order = quantity
                logger.info(f"📊 Order-Größe: {quantity} {self.symbol} (${self.initial_quantity})")

    def should_update_orders(self, new_price):
        """Prüft, ob Orders aktualisiert werden sollen"""
        if self.last_order_price == 0:
            return True

        price_change_pct = abs(new_price - self.last_order_price) / self.last_order_price
        return price_change_pct >= PRICE_UPDATE_THRESHOLD

    def update_last_order_price(self):
        """Aktualisiert den letzten Order-Preis"""
        self.last_order_price = self.latest_price

    async def place_order_safe(self, side: str, price: float, quantity: float):
        """Sicher下单"""
        try:
            formatted_price = self.market_manager.format_price(price, self.symbol)
            is_valid, formatted_quantity, msg = self.market_manager.validate_order_amount(
                formatted_price, quantity, self.symbol
            )

            if not is_valid:
                logger.warning(f"订单验证失败: {msg}")
                return None

            if self.dry_run:
                logger.info(f"🔄 DRY RUN - {side.upper()}: {formatted_quantity} @ ${formatted_price:.6f}")
                return "dry_run_order_id"

            logger.info(f"📈 REAL - {side}: {formatted_quantity} @ ${formatted_price:.6f}")

            if side == 'sell':
                formatted_quantity = -abs(formatted_quantity)

            result = await self.lighter.limit_order(
                ticker=self.symbol,
                amount=formatted_quantity,
                price=formatted_price,
                tif='GTC'
            )

            return str(int(time.time() * 1000)) if result else None

        except Exception as e:
            logger.error(f"下单失败: {e}")
            return None

    async def place_buy_order(self, price: float, quantity: float):
        """Platziert eine Kauf-Order"""
        return await self.place_order_safe('buy', price, quantity)

    async def place_sell_order(self, price: float, quantity: float):
        """Platziert eine Verkaufs-Order"""
        return await self.place_order_safe('sell', price, quantity)

    async def execute_dca_grid(self):
        """Führt die DCA-Grid-Strategie aus"""
        try:
            if self.latest_price <= 0:
                return

            # 1. Prüfe, ob Grid-Orders existieren
            tracker = self.order_manager.get_tracker(self.symbol)
            counts = tracker.get_order_counts()

            # 2. Berechne Zielpreise
            buy_price = self.latest_price * (1 - self.grid_spacing)  # 0.5% unter aktuell
            sell_price = self.latest_price * (1 + self.take_profit_percent)  # 1% über aktuell

            # 3. Prüfe, ob eine Position existiert
            if self.long_position == 0:
                # Keine Position → Kauf bei -0.5%
                logger.info(f"🟢 Keine Position - Kaufe bei ${buy_price:.6f}")
                await self.place_buy_order(buy_price, self.initial_quantity_per_order)
                return

            # 4. Position existiert → Prüfe Gewinnziel
            if self.avg_entry_price > 0:
                current_profit_pct = (self.latest_price - self.avg_entry_price) / self.avg_entry_price

                # 4a. Wenn ≥ 1% Gewinn → Verkaufen
                if current_profit_pct >= self.take_profit_percent:
                    logger.info(f"✅ {current_profit_pct*100:.2f}% Gewinn erreicht! Verkaufe bei ${self.latest_price:.6f}")
                    await self.place_sell_order(self.latest_price, self.long_position)

                    # Position zurücksetzen
                    self.long_position = 0
                    self.avg_entry_price = 0
                    self.total_trades += 1
                    self.total_sells += 1

                    # Neue Kauf-Order bei -0.5%
                    buy_price = self.latest_price * (1 - self.grid_spacing)
                    logger.info(f"🟢 Neue Kauf-Order bei ${buy_price:.6f}")
                    await self.place_buy_order(buy_price, self.initial_quantity_per_order)
                    return

                # 4b. Wenn ≤ 0.5% gefallen → Nachkaufen
                if current_profit_pct <= -self.grid_spacing:
                    logger.info(f"📉 {abs(current_profit_pct)*100:.2f}% gefallen - Kaufe nach bei ${buy_price:.6f}")
                    await self.place_buy_order(buy_price, self.initial_quantity_per_order)
                    self.total_buys += 1
                    return

            # 5. Grid-Orders setzen (falls nicht vorhanden)
            if counts['sell_orders'] == 0 and counts['buy_orders'] == 0:
                # Keine Orders → Neue Orders setzen
                logger.info(f"🔄 Setze Grid-Orders: Buy @ ${buy_price:.6f}, Sell @ ${sell_price:.6f}")

                # Bei fallendem Preis kaufen
                if self.long_position == 0:
                    await self.place_buy_order(buy_price, self.initial_quantity_per_order)
                else:
                    # Bei steigendem Preis verkaufen
                    await self.place_sell_order(sell_price, self.long_position)

                    # Zusätzliche Kauf-Order für Nachkauf
                    buy_price = self.latest_price * (1 - self.grid_spacing)
                    await self.place_buy_order(buy_price, self.initial_quantity_per_order)

            self.update_last_order_price()

        except Exception as e:
            logger.error(f"DCA Grid Ausführung fehlgeschlagen: {e}")

    async def graceful_shutdown(self):
        """Graceful Shutdown"""
        logger.info("🛑 开始优雅关闭...")
        self.shutdown_requested = True

        try:
            result = await self.batch_manager.cancel_all_orders_safe()
            if result['success']:
                logger.info("✅ 所有订单已撤销")
        except Exception as e:
            logger.error(f"关闭失败: {e}")

    async def run(self):
        """Hauptschleife"""
        mode_str = "DRY RUN" if self.dry_run else "LIVE TRADING"
        logger.info(f"🚀 启动 DCA Grid Bot ({mode_str})")

        price_task = asyncio.create_task(self.price_ws.initialize_and_run())

        logger.info("等待价格数据...")
        for _ in range(20):
            if self.latest_price > 0:
                break
            await asyncio.sleep(0.5)

        if self.latest_price == 0:
            logger.error("❌ 未能获取价格数据")
            price_task.cancel()
            return

        logger.info(f"✅ 价格: ${self.latest_price:.6f}")

        loop_count = 0
        last_stats_time = time.time()

        try:
            while not self.shutdown_requested:
                loop_count += 1
                current_time = time.time()

                if loop_count % 10 == 1:
                    logger.info(f"📊 Preis: ${self.latest_price:.6f}, Position: {self.long_position}, Avg: ${self.avg_entry_price:.6f}")

                # 持仓 synchronisieren
                if current_time - last_stats_time > 60:
                    self.long_position, self.avg_entry_price = await self.get_positions()
                    last_stats_time = current_time

                # Strategie ausführen
                await self.execute_dca_grid()

                await asyncio.sleep(2)

        except KeyboardInterrupt:
            self.shutdown_requested = True
        finally:
            await self.graceful_shutdown()

        price_task.cancel()
        if self.price_ws:
            self.price_ws.shutdown()
        if self.lighter:
            await self.lighter.cleanup()

        logger.info("✅ DCA Grid Bot gestoppt")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='DCA Grid Bot - Nur Long')
    parser.add_argument('--dry-run', action='store_true', help='模拟模式')
    parser.add_argument('--symbol', default=COIN_NAME, help='交易符号')
    parser.add_argument('--max-orders', type=int, default=MAX_ORDERS,
                        help=f'最大订单数 (默认: {MAX_ORDERS})')
    parser.add_argument('--grid-spacing', type=float, default=GRID_SPACING,
                        help=f'Nachkauf-Abstand (默认: {GRID_SPACING*100:.1f}%)')
    parser.add_argument('--take-profit', type=float, default=TAKE_PROFIT_PERCENT,
                        help=f'Gewinnziel (默认: {TAKE_PROFIT_PERCENT*100:.1f}%)')
    parser.add_argument('--order-amount', type=float, default=INITIAL_QUANTITY,
                        help=f'Order-Größe USD (默认: ${INITIAL_QUANTITY})')
    args = parser.parse_args()

    bot = DCAGridBot(
        dry_run=args.dry_run,
        symbol=args.symbol,
        max_orders=args.max_orders,
        grid_spacing=args.grid_spacing,
        order_amount=args.order_amount
    )
    bot.take_profit_percent = args.take_profit

    logger.info(f"🚀 启动参数:")
    logger.info(f"   交易对: {args.symbol}")
    logger.info(f"   模式: {'模拟' if args.dry_run else '实盘'}")
    logger.info(f"   Nachkauf: -{args.grid_spacing*100:.1f}%")
    logger.info(f"   Gewinnziel: +{args.take_profit*100:.1f}%")
    logger.info(f"   Order-Größe: ${args.order_amount}")
    logger.info(f"   Max Orders: {args.max_orders}")

    if not args.dry_run:
        logger.warning("⚠️ 实盘交易模式启动!")

    def signal_handler(signum, frame):
        logger.info(f"收到信号 {signum}，关闭中...")
        bot.shutdown_requested = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await bot.setup()
        await bot.run()
    except Exception as e:
        logger.error(f"Bot fehlgeschlagen: {e}")
        await bot.graceful_shutdown()
        raise


if __name__ == "__main__":
    print("🤖 DCA Grid Bot - Nur Long")
    print("📊 Kauft bei Dips (0.5%), verkauft bei Gewinn (1%)")
    print("=" * 50)
    asyncio.run(main())
