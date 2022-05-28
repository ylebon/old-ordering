# -*- coding: utf-8 -*-

usage = """Exchanges Routing.

Usage:
  ordering.py [--router=<router>] [--signaler=<signaler>...] [--registry=<registry>] [--loglevel=<loglevel>]
  ordering.py (-h | --help)
  ordering.py --version
Options:
  -k --registry=<registry>      Set registry
  -l --loglevel=<loglevel>      Set log level [default: INFO]
  -r --router=<router>          Set router [default: ROUTER.BITSTAMP]
  -s --signaler=<signaler>      Set signaler.
  -v --version                  Show version.
"""
import os
import sys

lib_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(lib_dir)
from docopt import docopt
from logbook import StreamHandler, set_datetime_format, FileHandler, NestedSetup
from config.core.config_services import ConfigServices
from ordering.core.orders_sender import OrdersSender

from events import Events
from connector.core.connector_listener import ConnectorListener
import asyncio


def main(arguments):
    # setting services
    services_config = ConfigServices.create(registry=arguments['--registry'])

    # set router
    if not arguments['--router'].startswith("ws://"):
        router = services_config.get_value(arguments['--router'])
    else:
        router = arguments['--router']

    if router is None:
        print("Router address '{}' not correct!".format(arguments['--router']))
        sys.exit(-1)

    services_config.set_value('ROUTER.URL', router)

    print("  =============== PARAMETERS ===============   ")
    print("  - Router address   : {}".format(arguments['--router']))
    print("  ==========================================   ")

    # router connector
    connector = ConnectorListener.from_router(router)

    # ordering
    ordering = OrdersSender.create()

    signaler_id_list = arguments['--signaler'] or []

    @ordering.on_event(Events.SELL_LIMIT_ORDER_ERROR)
    @ordering.on_event(Events.BUY_LIMIT_ORDER_ERROR)
    @ordering.on_event(Events.BUY_LIMIT_ORDER_FILLED)
    @ordering.on_event(Events.SELL_LIMIT_ORDER_FILLED)
    @ordering.on_event(Events.BUY_LIMIT_ORDER_NEW)
    @ordering.on_event(Events.SELL_LIMIT_ORDER_NEW)
    @ordering.on_event(Events.BUY_LIMIT_ORDER_EXPIRED)
    @ordering.on_event(Events.SELL_LIMIT_ORDER_EXPIRED)
    @ordering.on_event(Events.ORDER_EXPIRED)
    @ordering.on_event(Events.CANCEL_ORDER_FILLED)
    @ordering.on_event(Events.CANCEL_ORDER_ERROR)
    @ordering.on_event(Events.CANCEL_ORDER_FILLED)
    def fn(event):
        connector.publish(event)

    #@connector.on_event(Events.SESSION_READY)
    #async def on_session_ready():

    for signaler_id in signaler_id_list:
        @connector.on_event(Events.BUY_LIMIT_ORDER_REQUEST, params={'signaler_id': signaler_id})
        @connector.on_event(Events.SELL_LIMIT_ORDER_REQUEST, params={'signaler_id': signaler_id})
        @connector.on_event(Events.CANCEL_ORDER_REQUEST, params={'signaler_id': signaler_id})
        async def on_buy_sell_request(x):
            ordering.execute(x)

    # start connector
    loop = asyncio.get_event_loop()

    # run connector
    loop.run_until_complete(connector.run())

    # loop forever
    loop.run_forever()


if __name__ == '__main__':
    # parse arguments
    arguments = docopt(usage, version='0.0.1')
    set_datetime_format("local")

    # subscribe to instruments
    format_string = (
        '[{record.time:%Y-%m-%d %H:%M:%S.%f%z}] '
        '{record.level_name: <5} - {record.channel: <15}: {record.message}'
    )

    # log file
    log_file = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'exchanges.log')

    # create directory
    logs_dir = os.path.dirname(log_file)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # handlers
    handlers = [
        StreamHandler(sys.stdout, level=arguments['--loglevel'], format_string=format_string, bubble=True),
        FileHandler(log_file, level='INFO', format_string=format_string, bubble=True),
    ]

    # log handlers
    with NestedSetup(handlers):
        main(arguments)
