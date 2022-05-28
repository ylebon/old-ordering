import networkx as nx
from logbook import Logger
from rx import Observable

from events.order import *
from ordering.core.orders_states_finder import OrdersStatesFinder


class ExchangeNodeNotFound(Exception):
    pass


class InstrumentNodeNotFound(Exception):
    pass


class OrderRequestNodeNotFound(Exception):
    pass


class ExistingDescendant(Exception):
    pass


class OrdersStatesBuilder(object):
    """
    Orders States Builder

    """

    def __init__(self, output_directory):
        self.log = Logger(__class__.__name__)
        self.graph = nx.DiGraph()
        self.output_directory = output_directory
        self.callback_handlers = dict()
        self.internal_handlers = dict()

    @classmethod
    def create(cls, output_directory):
        """
        Create instance

        """
        return OrdersStatesBuilder(output_directory)

    def execute(self, event):
        """
        Add new event

        """
        for handler in self.internal_handlers.get(event.event, []):
            handler(event)

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

    def load_handlers(self):
        """
        Load handlers

        """

        @self.on_event(Events.BUY_LIMIT_ORDER_REQUEST, internal=True)
        @self.on_event(Events.SELL_LIMIT_ORDER_REQUEST, internal=True)
        def fn(event):
            """
            Add order request

            """
            self.log.info("msg='add order event' data='{}'".format(event))

            # instrument
            instrument_name = event.to_string()

            # label
            label = f"{event.__class__.__name__}={instrument_name}"

            # add instrument
            has_instrument = OrdersStatesFinder.has_instrument(self.graph, instrument_name)
            if has_instrument:
                self.graph.add_node(event.id, label=label, data=event)
                self.graph.add_edge(instrument_name, event.id)
            else:
                raise InstrumentNodeNotFound("instrument '{}' not found".format(instrument_name))

        @self.on_event(Events.BUY_LIMIT_ORDER_FILLED, internal=True)
        @self.on_event(Events.BUY_LIMIT_ORDER_NEW, internal=True)
        @self.on_event(Events.BUY_LIMIT_ORDER_EXPIRED, internal=True)
        @self.on_event(Events.BUY_LIMIT_ORDER_ERROR, internal=True)
        @self.on_event(Events.SELL_LIMIT_ORDER_FILLED, internal=True)
        @self.on_event(Events.SELL_LIMIT_ORDER_NEW, internal=True)
        @self.on_event(Events.SELL_LIMIT_ORDER_EXPIRED, internal=True)
        @self.on_event(Events.SELL_LIMIT_ORDER_ERROR, internal=True)
        @self.on_event(Events.CANCEL_ORDER_ERROR, internal=True)
        @self.on_event(Events.CANCEL_ORDER_FILLED, internal=True)
        @self.on_event(Events.CANCEL_ORDER_EXPIRED, internal=True)
        def fn(event):
            """
            Add sell limit order request

            """
            self.log.info("msg='add order filled' data={}".format(event))

            # label
            label = f"{event.__class__.__name__}={instrument_name}"

            # check parent
            orders = OrdersStatesFinder.has_node(self.graph, {"id": event.order_request_id})

            # add order state
            if orders and len(orders) == 1:
                parent_node = orders[0]
                self.graph.add_node(event.id, label=label, data=event)
                self.graph.add_edge(parent_node.id, event.id)
            elif not orders:
                raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        @self.on_event(Events.CANCEL_ORDER_REQUEST, internal=True)
        def fn(event):
            """
            Add cancel order request

            """
            self.log.info("msg='add cancel order request' data={}".format(event))

            # label
            label = f"{event.__class__.__name__}={instrument_name}"

            # check parent
            orders = OrdersStatesFinder.has_node(self.graph, {"exchange_order_id": event.exchange_order_id})

            # add order state
            if orders and len(orders) == 1:
                parent_node = orders[0]
                self.graph.add_node(event.id, label=label, data=event)
                self.graph.add_edge(parent_node.id, event.id)
            elif not orders:
                raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_request_id))

        @self.on_event(Events.SELL_LIMIT_ORDER_REQUEST, internal=True)
        @self.on_event(Events.SELL_MARKET_ORDER_REQUEST, internal=True)
        def fn(event):
            """
            Add cancel order request

            """
            self.log.info("msg='add cancel order request' data={}".format(event))

            # label
            label = f"{event.__class__.__name__}={instrument_name}"

            # check parent
            orders = OrdersStatesFinder.has_node(self.graph, {"exchange_order_id": event.order_buy_id})

            # add order state
            if orders and len(orders) == 1:
                parent_node = orders[0]
                self.graph.add_node(event.id, label=label, data=event)
                self.graph.add_edge(parent_node.id, event.id)
            elif not orders:
                raise OrderRequestNodeNotFound("order request node '{}' not found".format(event.order_buy_id))

    def add_exchange(self, exchange, node_id=None):
        """
        Add new exchange

        """
        self.log.info("msg='add new exchange' data={}".format(exchange))
        label = f"{exchange.__class__.__name__}={exchange.name}"
        has_exchange = OrdersStatesFinder.has_exchange(self.graph, exchange.name)

        # has exchange
        if has_exchange:
            self.log.warn(f"msg='exchange already exists' node_id='{node_id}'")
        else:
            self.graph.add_node(node_id, label=label, data=exchange)
            self.log.info(f"msg='exchange successfully added' node_id='{node_id}'")

    def add_instrument(self, instrument):
        """
        Add new market order

        """
        self.log.info("msg='add new instrument' data={}".format(instrument))

        label = "{}={}".format(instrument.__class__.__name__, instrument.to_string())
        exchange_name = instrument.exchange

        if not OrdersStatesBuilder.has_exchange(self.graph, exchange_name):
            raise ExchangeNodeNotFound("exchange '{}' not found".format(exchange_name))

        if not OrdersStatesBuilder.has_instrument(self.graph, instrument.to_string()):
            self.graph.add_node(instrument.id, label=label, data=instrument)
            self.graph.add_edge(instrument.exchange, instrument.to_string())

    @classmethod
    def show(cls, graph):
        """
        Show graph

        """
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pyplot as plt
        labels = OrdersStatesFinder.get_labels()
        graph = nx.relabel_nodes(graph, labels)
        graph_pos = nx.shell_layout(graph)
        nx.draw_networkx_labels(graph, graph_pos, font_size=12, font_family='sans-serif')
        plt.show()

    def draw(self, filename="graph.png"):
        """
        Draw

        """
        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pyplot as plt
        labels = OrdersStatesBuilder.get_labels(self.graph)
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
