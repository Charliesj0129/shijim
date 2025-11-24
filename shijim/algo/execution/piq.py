from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PiqEstimator:
    price: float
    my_qty: float
    total_qty: float
    qty_ahead: float

    @classmethod
    def initialize(cls, price: float, my_qty: float, total_exchange_qty: float) -> "PiqEstimator":
        qty_ahead = max(0.0, total_exchange_qty - my_qty)
        return cls(price=price, my_qty=my_qty, total_qty=total_exchange_qty, qty_ahead=qty_ahead)

    def on_trade(self, price: float, volume: float) -> None:
        if price != self.price:
            return
        self.qty_ahead = max(0.0, self.qty_ahead - volume)

    def on_quote(self, total_qty_at_price: float) -> None:
        if total_qty_at_price >= self.total_qty:
            self.total_qty = total_qty_at_price
            return
        delta = self.total_qty - total_qty_at_price
        if self.total_qty == 0:
            return
        cancel_share = (self.qty_ahead / self.total_qty) * delta
        self.qty_ahead = max(0.0, self.qty_ahead - cancel_share)
        self.total_qty = total_qty_at_price

    def estimated_fill_probability(self) -> float:
        if self.total_qty == 0:
            return 0.0
        return 1.0 - (self.qty_ahead / (self.qty_ahead + self.my_qty))
