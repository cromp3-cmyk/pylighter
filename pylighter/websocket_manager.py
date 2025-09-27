"""
WebSocket 管理工具 - 纯 SDK 功能
WebSocket Management Utilities - Pure SDK Functions

这个模块提供 WebSocket 连接管理、重连逻辑和错误处理
专门为 Lighter Protocol 优化
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Dict, Any, List

import websockets

try:
    from lighter import Configuration
except Exception:  # pragma: no cover - fallback when lighter is unavailable
    Configuration = None  # type: ignore

logger = logging.getLogger(__name__)


class AccountWebSocketManager:
    """账户 WebSocket 管理器 - 处理订单和账户更新"""

    def __init__(self, auth_token: str, market_id: int, account_idx: int):
        self.auth_token = auth_token
        self.market_id = market_id
        self.account_idx = account_idx
        self.websocket_url = "wss://mainnet.zklighter.elliot.ai/stream"

        # 连接状态
        self.connected = False
        self.subscribed = False
        self.shutdown_requested = False

        # 重连配置
        self.max_retries = 5
        self.retry_count = 0
        self.base_delay = 2

        # 回调函数
        self.on_orders_update: Optional[Callable] = None
        self.on_connection_status: Optional[Callable] = None

        # 健康监控
        self.last_message_time = None
        self.connection_start_time = None

    def set_orders_callback(self, callback: Callable[[Dict], None]):
        """设置订单更新回调函数"""
        self.on_orders_update = callback

    def set_status_callback(self, callback: Callable[[bool], None]):
        """设置连接状态回调函数"""
        self.on_connection_status = callback

    async def connect_and_run(self) -> None:
        """连接并运行 WebSocket (带重连逻辑)"""
        while not self.shutdown_requested and self.retry_count < self.max_retries:
            try:
                await self._run_websocket_session()
                self.retry_count = 0  # 重置重试计数
            except websockets.exceptions.ConnectionClosed:
                self._handle_connection_closed()
            except websockets.exceptions.WebSocketException as e:
                self._handle_websocket_error(e)
            except Exception as e:
                self._handle_unexpected_error(e)

            if not self.shutdown_requested and self.retry_count < self.max_retries:
                await self._wait_before_retry()

    async def _run_websocket_session(self):
        """运行单个 WebSocket 会话"""
        logger.info("🌐 连接到账户 WebSocket...")
        self.connection_start_time = time.time()

        async with websockets.connect(self.websocket_url) as ws:
            self.connected = False
            self.subscribed = False

            # 设置连接超时
            connection_timeout = 10
            start_time = time.time()

            async for message in ws:
                if self.shutdown_requested:
                    break

                # 检查连接超时
                if time.time() - start_time > connection_timeout and not self.connected:
                    logger.warning("⏱️ WebSocket 连接超时")
                    break

                await self._handle_message(ws, message)

    async def _handle_message(self, ws, message: str):
        """处理 WebSocket 消息"""
        try:
            data = json.loads(message)
            message_type = data.get('type', '')
            self.last_message_time = time.time()

            logger.debug(f"📨 WebSocket 消息: type={message_type}")

            if message_type == 'connected':
                await self._handle_connected(ws)
            elif message_type == 'subscribed/account_orders' or message_type.startswith('subscribed'):
                await self._handle_subscribed(data)
            elif message_type == 'update/account_orders':
                await self._handle_orders_update(data)
            elif message_type == 'error':
                self._handle_error_message(data)
            elif message_type == 'ping':
                await self._handle_ping(ws)
            elif message_type == 'pong':
                logger.debug("🏓 收到 pong")
            else:
                if message_type and message_type not in ['heartbeat', 'status']:
                    logger.debug(f"📨 未处理的消息: {message_type}")

        except json.JSONDecodeError as e:
            logger.warning(f"❌ JSON 解析失败: {e}")
        except Exception as e:
            logger.error(f"❌ 处理消息错误: {e}")

    async def _handle_connected(self, ws):
        """处理连接确认"""
        logger.info("🔗 WebSocket 已连接，发送订阅请求...")
        self.connected = True

        # 发送订阅请求
        subscribe_msg = {
            "type": "subscribe",
            "channel": f"account_orders/{self.market_id}/{self.account_idx}",
            "auth": self.auth_token
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"📋 已发送账户订单订阅请求 (市场 {self.market_id})")

        # 通知连接状态
        if self.on_connection_status:
            self.on_connection_status(True)

    async def _handle_subscribed(self, data: Dict):
        """处理订阅确认"""
        channel = data.get('channel', '')
        if 'account_orders' in channel:
            logger.info(f"✅ 成功订阅账户订单: {channel}")
            self.subscribed = True
            self.retry_count = 0  # 重置重试计数

    async def _handle_orders_update(self, data: Dict):
        """处理订单更新"""
        if not self.subscribed:
            logger.debug("忽略订单更新 - 尚未正确订阅")
            return

        orders_data = data.get('orders', {})
        market_orders = orders_data.get(str(self.market_id), [])

        logger.info(f"🔍 处理账户订单更新: {len(market_orders)} 个订单")

        # 调用回调函数
        if self.on_orders_update:
            self.on_orders_update(data)

    def _handle_error_message(self, data: Dict):
        """处理错误消息"""
        error_msg = data.get('message', data.get('error', 'Unknown error'))
        logger.error(f"❌ WebSocket 错误: {error_msg}")

    async def _handle_ping(self, ws):
        """处理 ping 消息"""
        await ws.send(json.dumps({"type": "pong"}))
        logger.debug("🏓 响应 ping")

    def _handle_connection_closed(self):
        """处理连接关闭"""
        logger.warning("🔌 WebSocket 连接已关闭")
        self.connected = False
        self.subscribed = False
        self.retry_count += 1

        if self.on_connection_status:
            self.on_connection_status(False)

    def _handle_websocket_error(self, error):
        """处理 WebSocket 错误"""
        logger.error(f"❌ WebSocket 错误: {error}")
        self.connected = False
        self.subscribed = False
        self.retry_count += 1

        if self.on_connection_status:
            self.on_connection_status(False)

    def _handle_unexpected_error(self, error):
        """处理意外错误"""
        logger.error(f"❌ 意外错误: {error}")
        self.connected = False
        self.subscribed = False
        self.retry_count += 1

    async def _wait_before_retry(self):
        """重连前等待"""
        wait_time = min(self.base_delay ** self.retry_count, 10)  # 最大10秒
        logger.info(f"⏳ {wait_time}秒后重试连接 (第{self.retry_count}/{self.max_retries}次)")
        await asyncio.sleep(wait_time)

    def shutdown(self):
        """关闭 WebSocket 连接"""
        logger.info("🛑 关闭 WebSocket 管理器")
        self.shutdown_requested = True

    def is_healthy(self, max_silence_seconds: int = 120) -> bool:
        """检查连接健康状态"""
        if not self.connected or not self.subscribed:
            return False

        if self.last_message_time is None:
            # 如果连接时间过长但没收到消息
            if self.connection_start_time and time.time() - self.connection_start_time > max_silence_seconds:
                return False
            return True

        # 检查最后收到消息的时间
        return time.time() - self.last_message_time < max_silence_seconds


class PriceWebSocketManager:
    """价格 WebSocket 管理器 - 处理订单簿更新"""

    def __init__(self, market_ids: List[int], base_url: Optional[str] = None) -> None:
        self.market_ids: List[int] = [int(mid) for mid in market_ids]
        self.shutdown_requested = False

        # 回调函数
        self.on_price_update: Optional[Callable[[int, Dict[str, Any]], None]] = None

        # 重连配置
        self.max_retries = 10
        self.retry_delay = 3

        self._order_books: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._base_url = base_url or (Configuration.get_default().host if Configuration else None)
        self._ws_url = self._build_ws_url(self._base_url)

    @staticmethod
    def _build_ws_url(base_url: Optional[str]) -> str:
        if not base_url:
            return "wss://mainnet.zklighter.elliot.ai/stream"

        if base_url.startswith("ws"):
            return base_url.rstrip("/") + "/stream"

        if base_url.startswith("http"):
            return base_url.replace("https", "wss", 1).replace("http", "ws", 1).rstrip("/") + "/stream"

        return f"wss://{base_url.rstrip('/')}" + "/stream"

    def set_price_callback(self, callback: Callable[[int, Dict[str, Any]], None]) -> None:
        """设置价格更新回调函数"""
        self.on_price_update = callback

    async def initialize_and_run(self) -> None:
        """初始化并运行价格 WebSocket"""
        logger.info("✅ 价格 WebSocket 初始化完成 (市场: %s)", self.market_ids)
        await self._run_with_retry()

    async def _run_with_retry(self) -> None:
        retry_count = 0

        while not self.shutdown_requested and retry_count < self.max_retries:
            try:
                if retry_count == 0:
                    logger.info("🌐 启动价格 WebSocket 连接...")
                else:
                    logger.info(
                        "🌐 重试价格 WebSocket 连接 (第%d/%d次)",
                        retry_count + 1,
                        self.max_retries,
                    )
                await self._connect_once()
                if self.shutdown_requested:
                    break
                retry_count = 0
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.WebSocketException as exc:
                retry_count += 1
                self._log_websocket_error(exc, retry_count)
                if retry_count < self.max_retries and not self.shutdown_requested:
                    wait_time = min(self.retry_delay * min(retry_count, 3), 30)
                    logger.info("⏳ 价格 WebSocket %d秒后重试 (第%d/%d次)", wait_time, retry_count, self.max_retries)
                    await asyncio.sleep(wait_time)
            except Exception as exc:
                retry_count += 1
                logger.error("价格 WebSocket 未知错误: %s", exc)
                if retry_count < self.max_retries and not self.shutdown_requested:
                    wait_time = min(self.retry_delay * min(retry_count, 3), 30)
                    logger.info("⏳ 价格 WebSocket %d秒后重试 (第%d/%d次)", wait_time, retry_count, self.max_retries)
                    await asyncio.sleep(wait_time)
        else:
            if retry_count >= self.max_retries:
                logger.error("❌ 价格 WebSocket 最大重试次数已达到")

    async def _connect_once(self) -> None:
        self._order_books.clear()
        try:
            async with websockets.connect(self._ws_url) as ws:
                self._ws = ws
                async for raw in ws:
                    if self.shutdown_requested:
                        break
                    await self._handle_message(ws, raw)
        finally:
            self._ws = None

    async def _handle_message(self, ws: websockets.WebSocketClientProtocol, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.debug("忽略非 JSON WebSocket 消息: %s", exc)
            return

        msg_type = data.get("type")

        if msg_type == "connected":
            await self._subscribe_markets(ws)
            return

        if msg_type == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        if msg_type == "subscribed/order_book":
            self._handle_snapshot(data)
            return

        if msg_type == "update/order_book":
            self._handle_incremental(data)
            return

        if msg_type == "error":
            logger.warning("价格 WebSocket 错误消息: %s", data.get("message", data))
            return

        logger.debug("忽略未识别的价格消息类型: %s", msg_type)

    async def _subscribe_markets(self, ws: websockets.WebSocketClientProtocol) -> None:
        for market_id in self.market_ids:
            payload = {
                "type": "subscribe",
                "channel": f"order_book/{market_id}",
            }
            await ws.send(json.dumps(payload))
        logger.info("📡 已发送价格订阅请求: %s", self.market_ids)

    def _handle_snapshot(self, data: Dict[str, Any]) -> None:
        channel = data.get("channel", "")
        try:
            market_id = int(channel.split(":")[1])
        except (IndexError, ValueError):
            logger.debug("快照消息缺少市场信息: %s", channel)
            return

        order_book = data.get("order_book", {})
        if not order_book:
            return

        self._order_books[market_id] = self._normalize_book(order_book)
        self._emit_update(market_id)

    def _handle_incremental(self, data: Dict[str, Any]) -> None:
        channel = data.get("channel", "")
        try:
            market_id = int(channel.split(":")[1])
        except (IndexError, ValueError):
            logger.debug("增量消息缺少市场信息: %s", channel)
            return

        if market_id not in self._order_books:
            # 没有快照时直接忽略，等待下一次完整快照
            logger.debug("收到增量更新但缺少基础快照，等待下次快照 (市场 %s)", market_id)
            return

        order_book = data.get("order_book", {})
        if not order_book:
            return

        current = self._order_books[market_id]
        current["asks"] = self._apply_updates(order_book.get("asks", []), current.get("asks", []))
        current["bids"] = self._apply_updates(order_book.get("bids", []), current.get("bids", []), reverse=True)
        self._emit_update(market_id)

    def _emit_update(self, market_id: int) -> None:
        if self.on_price_update:
            try:
                book = self._order_books[market_id]
                payload = {
                    "asks": [dict(item) for item in book.get("asks", [])],
                    "bids": [dict(item) for item in book.get("bids", [])],
                }
                self.on_price_update(market_id, payload)
            except Exception as exc:
                logger.error("价格更新回调错误: %s", exc)

    @staticmethod
    def _normalize_book(order_book: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        asks = sorted(order_book.get("asks", []), key=lambda x: float(x["price"]))
        bids = sorted(order_book.get("bids", []), key=lambda x: float(x["price"]), reverse=True)
        return {"asks": asks, "bids": bids}

    @staticmethod
    def _apply_updates(
        updates: List[Dict[str, Any]],
        existing: List[Dict[str, Any]],
        *,
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        price_map = {item["price"]: item for item in existing}
        for entry in updates:
            price = entry.get("price")
            if price is None:
                continue
            size = float(entry.get("size", 0))
            if size == 0:
                price_map.pop(price, None)
            else:
                price_map[price] = entry

        sorted_orders = sorted(
            price_map.values(),
            key=lambda x: float(x["price"]),
            reverse=reverse,
        )
        return sorted_orders

    def _log_websocket_error(self, exc: Exception, retry_count: int) -> None:
        message = str(exc).lower()
        noisy_keywords = ["ping", "pong", "connection reset", "connection closed", "timeout"]
        if any(keyword in message for keyword in noisy_keywords):
            logger.debug("价格 WebSocket 可恢复错误 (%s) 第%d次", exc, retry_count)
        else:
            logger.error("价格 WebSocket 错误 (%d/%d): %s", retry_count, self.max_retries, exc)

    def shutdown(self) -> None:
        """关闭价格 WebSocket"""
        logger.info("🛑 关闭价格 WebSocket 管理器")
        self.shutdown_requested = True
        ws = self._ws
        if ws:
            is_closed = getattr(ws, "closed", False)
            if not is_closed:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop and loop.is_running():
                    loop.create_task(ws.close())
