from logbook import Logger

from events.exchange import *
from exchanges.core.exchanges_routing import ExchangesRouting


class OrdersSender(object):
    """
    Signal Ordering

    """

    def __init__(self, exchanges_routing):
        self.log = Logger(self.__class__.__name__)
        self.callback_handlers = dict()
        self.internal_handlers = dict()
        self.exchanges_routing = exchanges_routing

    @classmethod
    def create(cls):
        """
        Create Signal Ordering

        """
        exchanges_routing = ExchangesRouting.create()
        signal_ordering = OrdersSender(exchanges_routing)
        signal_ordering.load_handlers()
        return signal_ordering

    def execute(self, event):
        """
        Execute signal

        """
        for handler in self.internal_handlers.get(event.event, []):
            handler(event)

    def callback(self, event):
        """
        Execute callback

        """
        for callback_handler in self.callback_handlers.get(event.event, []):
            callback_handler(event)

    def load_handlers(self):
        """
        Load all the handlers

        """

        @self.on_event(Events.SIGNAL_BUY, internal=True)
        def fn(signal):
            request = BuyLimitOrderRequest(
                exchange=signal.exchange,
                symbol=signal.symbol,
                signal_id=signal.signal_id,
                type=signal.ordering_info.get('order_type', 'limit'),
                qty=float(getattr(signal, 'qty', 0)),
                price=signal.price,
                time_in_force=signal.ordering_info.get('time_in_force', 'FOK'),
                stop_loss=signal.ordering_info.get('stop_loss', None),
                take_profit=signal.ordering_info.get('take_profit', None),
                check_position=signal.ordering_info.get('check_position', False),
            )
            self.exchanges_routing.execute(request)

        @self.on_event(Events.SIGNAL_SELL, internal=True)
        def fn(signal):
            request = SellLimitOrderRequest(
                exchange=signal.exchange,
                symbol=signal.symbol,
                signal_id=signal.signal_id,
                type=signal.ordering_info.get('order_type', 'limit'),
                qty=float(getattr(signal, 'qty', 0)),
                price=signal.price,
                check_position=signal.ordering_info.get('check_position', False),
                time_in_force=signal.ordering_info.get('time_in_force', 'FOK'),
            )
            self.exchanges_routing.execute(request)

        @self.on_event(Events.BUY_LIMIT_ORDER_REQUEST, internal=True)
        def fn(signal):
            self.exchanges_routing.execute(signal)

        @self.on_event(Events.SELL_LIMIT_ORDER_REQUEST, internal=True)
        def fn(signal):
            self.exchanges_routing.execute(signal)

        @self.on_event(Events.CANCEL_ORDER_REQUEST, internal=True)
        def fn(signal):
            self.exchanges_routing.execute(signal)

        @self.on_event(Events.BALANCE_REQUEST, internal=True)
        def fn(signal):
            request = BalanceRequest(
                exchange=signal.exchange,
                symbol=signal.symbol,
            )
            self.exchanges_routing.execute(request)

        @self.exchanges_routing.on_event(Events.BUY_LIMIT_ORDER_ERROR)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.SELL_LIMIT_ORDER_ERROR)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.BUY_LIMIT_ORDER_FILLED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.SELL_LIMIT_ORDER_FILLED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.BUY_LIMIT_ORDER_NEW)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.SELL_LIMIT_ORDER_NEW)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.ORDER_EXPIRED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.BUY_LIMIT_ORDER_EXPIRED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.SELL_LIMIT_ORDER_EXPIRED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.BALANCE)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.CANCEL_ORDER_FILLED)
        def fn(result):
            self.callback(result)

        @self.exchanges_routing.on_event(Events.CANCEL_ORDER_ERROR)
        def fn(result):
            self.callback(result)

    def on_event(self, event_type, internal=False):
        """
        Register handlers

        """

        def register_handler(handler):
            try:
                if internal:
                    self.internal_handlers[event_type].append(handler)
                else:
                    self.callback_handlers[event_type].append(handler)
            except KeyError:
                if internal:
                    self.internal_handlers[event_type] = [handler]
                else:
                    self.callback_handlers[event_type] = [handler]
            return handler

        return register_handler
