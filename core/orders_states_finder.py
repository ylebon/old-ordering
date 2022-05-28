import networkx as nx
from logbook import Logger
from rx import Observable

from events.exchange import Exchange
from events.instrument import Instrument
from events.order import *


class ExchangeNodeNotFound(Exception):
    pass


class InstrumentNodeNotFound(Exception):
    pass


class OrderRequestNodeNotFound(Exception):
    pass


class ExistingDescendant(Exception):
    pass


class OrdersStatesFinder(object):
    log = Logger('SignalStates')

    @staticmethod
    def can_send_order(graph, instrument_str):
        """return if can open a position"""
        return SignalStates.is_empty_states(graph, instrument_str) or \
               SignalStates.has_filled_sell(graph, instrument_str) or \
               SignalStates.has_order_request_error(graph, instrument_str) or \
               SignalStates.has_order_request_expired(graph, instrument_str)

    @staticmethod
    def get_labels(graph):
        """return labels"""
        nodes = graph.nodes(data=True)
        labels = Observable.from_(nodes).map(lambda x: (x[0], x[1]['label'])).to_blocking()
        return dict(iter(labels))

    @staticmethod
    def get_nodes(graph):
        """return nodes"""
        nodes = graph.nodes(data=True)
        return dict(iter(nodes))

    @staticmethod
    def has_descendants(graph, node_id):
        """check """
        nodes = graph.nodes(data=True)
        nodes_found = Observable.from_(nodes).filter(
            lambda node, index: nx.descendants(graph, node_id)
        ).to_blocking()
        return list(iter(nodes_found))

    @staticmethod
    def is_single(graph, instrument_symbol):
        """check """
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: nx.descendants(graph, instrument_symbol)
        ).to_blocking()
        return not list(iter(orders))

    @staticmethod
    def get_states(graph, instrument_symbol):
        """return instruments states"""
        nodes = graph.nodes(data=True)
        states = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderFilled)
                                and node[1]['data'].instrument_str == instrument_symbol
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(states))

    @staticmethod
    def has_exchange(graph, exchange_name):
        """has exchange"""
        nodes = graph.nodes(data=True)
        exchanges = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], Exchange)
                                and node[1]['data'].name == exchange_name
        ).to_blocking()
        return list(iter(exchanges))

    @staticmethod
    def get_exchanges(graph):
        """get exchanges"""
        nodes = graph.nodes(data=True)
        exchanges = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], Exchange)
        ).to_blocking()
        return list(iter(exchanges))

    @staticmethod
    def has_instrument(graph, instrument_symbol):
        """get instrument"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], Instrument)
                                and node[1]['data'].to_string() == instrument_symbol
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_node(graph, kwargs):
        """has node"""
        nodes = graph.nodes(data=True)
        selected_nodes = Observable.from_(nodes).filter(
            lambda node, index: all([node[1]['data'].__dict__.get(k) == v for k, v in kwargs.items()])
        ).to_blocking()
        return list(iter(selected_nodes))

    @staticmethod
    def has_filled_buy(graph, instrument_str):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderFilled)
                                and node[1]['data'].instrument_str == instrument_str
                                and node[1]['data'].is_buy()
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_filled_sell(graph, instrument_symbol):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderFilled)
                                and node[1]['data'].instrument_str == instrument_symbol
                                and node[1]['data'].is_sell()
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_exchange_node(graph):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: index == "EXCHANGE"
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_opened_sell(graph, instrument_symbol):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderNew)
                                and node[1]['data'].instrument_str == instrument_symbol
                                and node[1]['data'].is_sell()
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_opened_sell(graph):
        """graphet all opened sell orders"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderNew)
                                and node[1]['data'].is_sell()
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def get_instruments(graph):
        """return all instruments"""
        nodes = graph.nodes(data=True)
        instruments = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], Instrument)
        ).to_blocking()
        return list(iter(instruments))

    @staticmethod
    def has_opened_buy(graph, instrument_str):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderNew)
                                and node[1]['data'].instrument_str == instrument_str
                                and node[1]['data'].is_buy()
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_cancelled_buy(graph, instrument_str):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderCancelled)
                                and node[1]['data'].instrument_str == instrument_str
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_cancelled_sell(graph, instrument_str):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderCancelled)
                                and node[1]['data'].x == instrument_str
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_order_request_error(graph, instrument_str):
        """check filled order"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderError)
                                and node[1]['data'].instrument_str == instrument_str
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))

    @staticmethod
    def has_order_request_expired(graph, instrument_str):
        """check request expired"""
        nodes = graph.nodes(data=True)
        orders = Observable.from_(nodes).filter(
            lambda node, index: isinstance(node[1]['data'], OrderExpired)
                                and node[1]['data'].instrument_str == instrument_str
                                and len(nx.descendants(graph, node[0])) == 0
        ).to_blocking()
        return list(iter(orders))


class OrdersStatesBuilder(object):
    """
    Orders States Builder

    """

    def __init__(self, output_directory):
        self.log = Logger(__class__.__name__)
        self.graph = nx.DiGraph()
        self.output_directory = output_directory

    @classmethod
    def create(cls, output_directory):
        """
        Create instance

        """
        return OrdersStatesBuilder(output_directory)

    def add_event(self, event, node_id=None):
        """
        Add new event

        """
        if isinstance(event, Exchange):
            self.add_exchange(event, node_id=node_id)
        elif isinstance(event, Instrument):
            self.add_instrument(event, node_id=node_id)
        elif isinstance(event, BuyLimitOrderRequest):
            self.add_buy_limit_order_request(event, node_id=node_id)
        elif isinstance(event, BuyLimitOrderFilled):
            self.add_buy_limit_order_filled(event, node_id=node_id)
        elif isinstance(event, BuyLimitOrderExpired):
            self.add_buy_limit_order_expired(event, node_id=node_id)
        elif isinstance(event, BuyLimitOrderError):
            self.add_buy_limit_order_error(event, node_id=node_id)
        elif isinstance(event, SellLimitOrderRequest):
            self.add_sell_limit_order_request(event, node_id=node_id)
        elif isinstance(event, SellLimitOrderExpired):
            self.add_sell_limit_order_expired(event, node_id=node_id)
        elif isinstance(event, SellLimitOrderFilled):
            self.add_sell_limit_order_filled(event, node_id=node_id)
        elif isinstance(event, SellLimitOrderError):
            self.add_sell_limit_order_error(event, node_id=node_id)
        elif isinstance(event, SellLimitOrderNew):
            self.add_sell_limit_order_new(event, node_id=node_id)
        elif isinstance(event, CancelOrderRequest):
            self.add_cancel_order_request(event, node_id=node_id)
        elif isinstance(event, CancelOrderFilled):
            self.add_cancel_order_filled(event, node_id=node_id)
        elif isinstance(event, CancelOrderError):
            self.add_cancel_order_error(event, node_id=node_id)
        elif isinstance(event, SellMarketOrderRequest):
            self.add_sell_market_order_request(event, node_id=node_id)
        elif isinstance(event, SellMarketOrderFilled):
            self.add_sell_market_order_filled(event, node_id=node_id)
        else:
            self.log.error("msg='unknown order type' order={}".format(type(event)))
        if self.draw_graph:
            self.draw('strategy_states.png')

    def add_buy_limit_order_request(self, event, node_id=None):
        """add buy limit order request"""
        self.log.info("msg='add buy limit order request' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        has_instrument = SignalStates.has_instrument(self.graph, event.instrument_str)
        if not has_instrument:
            raise InstrumentNodeNotFound("instrument '{}' not found".format(event.instrument_str))
        else:
            self.graph.add_node(node_id, label=label, data=event)
            self.graph.add_edge(event.instrument_str, node_id)

    def add_sell_limit_order_request(self, event, node_id=None):
        """add sell limit order request"""
        self.log.info("msg='add sell limit order request' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        has_instrument = SignalStates.has_instrument(self.graph, event.instrument_str)
        if not has_instrument:
            raise InstrumentNodeNotFound("instrument '{}' not found".format(event.instrument_str))
        else:
            self.graph.add_node(node_id, label=label, data=event)
            if event.order_buy_id:
                self.graph.add_edge(event.order_buy_id, node_id)
            else:
                self.graph.add_edge(event.instrument_str, node_id)

    def add_sell_limit_order_new(self, event, node_id=None):
        """add sell limit order request"""
        self.log.info("msg='add sell limit order new' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        orders = SignalStates().has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        order_node_id, _ = orders[0]
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_buy_limit_order_filled(self, event, node_id=None):
        """add buy limit order request"""
        self.log.info("msg='add buy limit order filled' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates().has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        order_node_id, _ = orders[0]
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_sell_market_order_filled(self, event, node_id=None):
        """add sell market order filled"""
        self.log.info("msg='add sell market order filled' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        order_node_id, _ = orders[0]
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_sell_limit_order_filled(self, event, node_id=None):
        """add sell limit order request"""
        self.log.info("msg='add sell limit order filled' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates().has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        order_node_id, _ = orders[0]
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_cancel_order_error(self, event, node_id=None):
        """add order cancel error"""
        self.log.info("msg='add cancel order error' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_cancel_order_request(self, event, node_id=None):
        """add order cancel request"""
        self.log.info("msg='add cancel order request' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        orders = SignalStates.has_node(self.graph, {"exchange_order_id": event.exchange_order_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.exchange_order_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_sell_market_order_request(self, event, node_id=None):
        """add sell market order request"""
        self.log.info("msg='add sell market order request' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_buy_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_buy_id))
        order_node_id, _ = orders[0]
        # check empty
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_cancel_order_filled(self, event, node_id=None):
        """add order cancel filled"""
        self.log.info("msg='add cancel order filled' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.instrument_str)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_buy_limit_order_expired(self, event, node_id=None):
        """add buy limit order expired"""
        self.log.info("msg='add buy limit order expired' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_sell_limit_order_expired(self, event, node_id=None):
        """add sell limit order expired"""
        self.log.info("msg='add sell limit order expired' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates.has_node(self.graph, {"order_id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_buy_limit_order_error(self, event, node_id=None):
        """add buy limit order expired"""
        self.log.info("msg='add buy limit order error' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_sell_limit_order_error(self, event, node_id=None):
        """add sell limit order expired"""
        self.log.info("msg='add sell limit order error' data={}".format(event))
        # node
        node_id = node_id or event.id
        # label
        label = "{}={}".format(event.__class__.__name__, event.symbol)
        # check parent
        orders = SignalStates.has_node(self.graph, {"id": event.order_request_id})
        if not orders or len(orders) > 1:
            raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))
        order_node_id, _ = orders[0]
        # check empty
        if SignalStates.has_descendants(self.graph, order_node_id):
            raise ExistingDescendant("order request node '{}' has descendants".format(event.order_request_id))
        self.graph.add_node(node_id, label=label, data=event)
        self.graph.add_edge(order_node_id, node_id)

    def add_exchange(self, exchange, node_id=None):
        """add exchange"""
        self.log.info("msg='add new exchange' data={}".format(exchange))
        node_id = node_id or exchange.to_string()
        label = "{}={}".format(exchange.__class__.__name__, exchange.to_string())
        has_exchange = SignalStates.has_exchange(self.graph, exchange.name)
        if not has_exchange:
            self.graph.add_node(node_id, label=label, data=exchange)
            self.log.info("msg='exchange successfully added' node_id='{}'".format(node_id))
        else:
            self.log.warn("msg='exchange already exists' node_id='{}'".format(node_id))

    def add_instrument(self, instrument, node_id=None):
        """add new market order"""
        self.log.info("msg='add new instrument' data={}".format(instrument))
        node_id = node_id or instrument.to_string()
        label = "{}={}".format(instrument.__class__.__name__, instrument.to_string())
        exchange_name = instrument.exchange
        if not SignalStates.has_exchange(self.graph, exchange_name):
            raise ExchangeNodeNotFound("exchange '{}' not found".format(exchange_name))
        if not SignalStates.has_instrument(self.graph, instrument.to_string()):
            self.graph.add_node(node_id, label=label, data=instrument)
            self.graph.add_edge(instrument.exchange, node_id)

    def show(graph):
        """show graph"""
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pyplot as plt
        labels = SignalStates.get_labels()
        graph = nx.relabel_nodes(graph, labels)
        graph_pos = nx.shell_layout(graph)
        nx.draw_networkx_labels(graph, graph_pos, font_size=12, font_family='sans-serif')

        plt.show()

    def draw(self, filename="graph.png"):
        """show graph"""
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pyplot as plt
        labels = SignalStates.get_labels(self.graph)
        graph = nx.relabel_nodes(self.graph, labels)
        f = plt.figure()
        nx.draw_spring(graph, with_labels=True, ax=f.add_subplot(111), font_size=8)
        f.savefig(filename)

    def get_graph(self):
        return self.graph

    ###################################################
    def get_instrument_states(self, instrument_str):
        """
        get instrument states

        """
        nodes = self.graph.nodes(data=True)
        states = Observable.from_(nodes).filter(
            lambda node, index: hasattr(node[1]['data'], 'instrument_str') and node[1][
                'data'].instrument_str == instrument_str
                                and len(nx.descendants(self.graph, node[0])) == 0
        ).to_blocking()
        return list(iter(states))

    def can_send_order(self, instrument_str):
        """
        get instrument states

        """
        states = self.get_instrument_states(instrument_str)
        return all(
            [type(state['data']) not in [BuyLimitOrderRequest, BuyLimitOrderFilled, SellLimitOrderNew,
                                         CancelOrderRequest, CancelOrderFilled] for node, state
             in states])

    def find_ancestror(self, node_id, state_type):
        """
        find ancestrors

        """
        nodes = nx.ancestors(self.graph, node_id)
        result = [self.graph.node[node_id]['data'] for node_id in nodes if
                  type(self.graph.node[node_id]['data']) == state_type]
        if result:
            return result[0]

    def is_state(self, instrument_str, instrument_state):
        """
        instrument state

        """
        states = self.get_instrument_states(instrument_str)
        for x in states:
            node, state = x
            if type(state['data']) == instrument_state:
                return state['data']

    def find_state(self, instrument_str, instrument_state):
        """
        instrument state

        """
        nodes = self.graph.nodes(data=True)
        states = Observable.from_(nodes).filter(lambda node, index:
                                                hasattr(node[1]['data'], 'instrument_str') and
                                                node[1]['data'].instrument_str == instrument_str and
                                                type(node[1]['data']) == instrument_state
                                                ).to_blocking()
        states = list(iter(states))
        if states:
            return states[0][1]["data"]
        else:
            return

    def clear_instrument_states(self, instrument_str):
        """
        Clear instruments states

        """
        self.log.info("msg='cleaning instrument states' instrument={}".format(instrument_str))
        nodes = nx.descendants(self.graph, instrument_str)
        self.graph.remove_nodes_from(nodes)


if __name__ == '__main__':
    pass
