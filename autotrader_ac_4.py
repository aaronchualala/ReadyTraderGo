# python3 rtg.py run autotrader_ac_4.py
import asyncio
import itertools

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

# send_amend_order(self, client_order_id: int, volume: int) -> None:
"""Amend the specified order with an updated volume."""
# send_cancel_order(self, client_order_id: int) -> None:
"""Cancel the specified order."""
# send_hedge_order(self, client_order_id: int, side: Side, price: int, volume: int) -> None:
"""Order lots in the future to hedge a position."""
# send_insert_order(self, client_order_id: int, side: Side, price: int, volume: int, lifespan: Lifespan) -> None:
"""Insert a new order into the market."""

class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        
        self.bid_price_history_of_etf = []
        self.ask_price_history_of_etf = []
        self.bid_price_history_of_future = []
        self.ask_price_history_of_future = []
        self.position_orders = 0 # total etf position in active orders

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id, price, volume)
 
    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id, price, volume)
        if client_order_id in self.bids:
            self.position += volume
            self.position_orders -= volume
            self.send_hedge_order(next(self.order_ids), Side.ASK, MIN_BID_NEAREST_TICK, volume)
        elif client_order_id in self.asks:
            self.position -= volume
            self.position_orders += volume
            self.send_hedge_order(next(self.order_ids), Side.BID, MAX_ASK_NEAREST_TICK, volume)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d", client_order_id, fill_volume, remaining_volume, fees)

        if client_order_id == self.bid_id:
            self.position_orders -= fill_volume 
        elif client_order_id == self.ask_id:
            self.position_orders += fill_volume 
                
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument, sequence_number)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument, sequence_number)
        # decision-making for etf_ask-etf_bid-price-arbitrage
            # hmm if I send orders here based on current position, but also send later based on current position, problem?
            # possibly assign 50 lots for this strat, and 50 lots to the other strat

        # ————————————————————————————————————————————————————————————————————
        # price history updates
        if instrument == Instrument.ETF:
            self.bid_price_history_of_etf.append(bid_prices[0])
            self.ask_price_history_of_etf.append(ask_prices[0])

        if instrument == Instrument.FUTURE:
            self.bid_price_history_of_future.append(bid_prices[0])
            self.ask_price_history_of_future.append(ask_prices[0])

        # the strategy after this applies to etf, we haven't thought of a strategy for future yet
        if instrument == Instrument.FUTURE:
            return

        is_latest_info = (len(self.bid_price_history_of_etf) == len(self.bid_price_history_of_future))
        if not is_latest_info:
            return

        # define key vars
        midpoint_price_of_etf = (self.bid_price_history_of_etf[-1] + self.ask_price_history_of_etf[-1])//2
        midpoint_price_of_future = (self.bid_price_history_of_future[-1] + self.
        ask_price_history_of_future[-1])//2
        multiplier = (abs(midpoint_price_of_etf - midpoint_price_of_future) // TICK_SIZE_IN_CENTS) + 1
        have_an_active_ask = self.ask_id != 0
        have_an_active_bid = self.bid_id != 0

        # sell logic
        significant_etf_overpricing = midpoint_price_of_etf > midpoint_price_of_future # (midpoint_price_of_etf/midpoint_price_of_future-1)/0.003546 > 1
        in_long_etf_position = (self.position_orders >= 0 and (midpoint_price_of_etf/midpoint_price_of_future-1)/0.003546 > 1 - (self.position_orders/100))

        should_sell = \
            significant_etf_overpricing # or in_long_etf_position
        
        # buy logic
        significant_etf_underpricing = midpoint_price_of_etf < midpoint_price_of_future # (midpoint_price_of_etf/midpoint_price_of_future-1)/0.003546 < -1 
        in_short_etf_position = (self.position_orders < 0 and (midpoint_price_of_etf/midpoint_price_of_future-1)/0.003546 < -1 - (self.position_orders/100))

        should_buy = \
            significant_etf_underpricing # or in_short_etf_position
        

        # decision-making for etf-future-midpoint-price-arbitrage
        if have_an_active_ask:  
            self.send_cancel_order(self.ask_id)
            self.ask_id = 0
        if have_an_active_bid:  
            self.send_cancel_order(self.bid_id)
            self.bid_id = 0

        # ((keep this two vars here to be safe cause exceeded last time))
        is_within_limit_after_sell = self.position > - POSITION_LIMIT + LOT_SIZE
        is_within_limit_after_buy = self.position < POSITION_LIMIT - LOT_SIZE

        if should_sell and is_within_limit_after_sell:
            volume = LOT_SIZE * multiplier if self.position - LOT_SIZE * multiplier >= -100 else 100 + self.position
            # execute !!
            self.ask_id = next(self.order_ids)
            self.send_insert_order(self.ask_id, Side.ASK, self.ask_price_history_of_etf[-1], volume, Lifespan.GOOD_FOR_DAY)
            self.asks.add(self.ask_id)
    
        if should_buy and is_within_limit_after_buy:
            volume = LOT_SIZE * multiplier if self.position + LOT_SIZE * multiplier <= 100 else 100 - self.position
            # execute !!
            self.bid_id = next(self.order_ids)
            self.send_insert_order(self.bid_id, Side.BID, self.bid_price_history_of_etf[-1], volume, Lifespan.GOOD_FOR_DAY)
            self.bids.add(self.bid_id)