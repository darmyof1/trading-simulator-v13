from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

# --- Public enums & types your strategy can import today ---
class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"

class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"

@dataclass
class Order:
    symbol: str
    side: OrderSide
    qty: float
    type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    time_in_force: str = "day"  # or "gtc", "ioc", etc.
    client_order_id: Optional[str] = None
    extras: Dict[str, Any] = None  # room for broker-specific fields

# --- Minimal interface ---
class Broker:
    def send_order(self, order: Order) -> Dict[str, Any]:
        """Place an order and return an 'execution report'-like dict."""
        raise NotImplementedError

    def cancel_all(self) -> None:
        raise NotImplementedError

    def get_positions(self) -> List[Dict[str, Any]]:
        return []

# --- No-op broker for simulation / logging only ---
class NoOpBroker(Broker):
    def __init__(self, log_prefix: str = "[NOOP]"):
        self.log_prefix = log_prefix
        self._sent: List[Dict[str, Any]] = []

    def send_order(self, order: Order) -> Dict[str, Any]:
        report = {
            "status": "accepted",
            "symbol": order.symbol,
            "side": order.side.value,
            "qty": order.qty,
            "type": order.type.value,
            "limit_price": order.limit_price,
            "tif": order.time_in_force,
            "client_order_id": order.client_order_id,
        }
        print(f"{self.log_prefix} order -> {report}", flush=True)
        self._sent.append(report)
        # Simulate instant fill for backtests if you want:
        # report["status"] = "filled"; report["filled_qty"] = order.qty
        return report

    def cancel_all(self) -> None:
        print(f"{self.log_prefix} cancel_all()", flush=True)

    def get_positions(self) -> List[Dict[str, Any]]:
        # Stubbed positions
        return []

def get_broker(mode: str = "sim") -> Broker:
    """
    Factory: return a broker that matches the run mode.
    - "sim" => NoOpBroker (default)
    - "paper" / "live" => later you'll return a real AlpacaBroker here.
    """
    return NoOpBroker()
