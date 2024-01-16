from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.window as win

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import EventClockConfig, TumblingWindow
from bytewax.testing import TestingSource

flow = Dataflow("windowing")

align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
inp = [
    {"time": align_to, "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=5), "user": "b", "val": 1},
    {"time": align_to + timedelta(seconds=8), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=14), "user": "b", "val": 1},
]
stream = op.input("input", flow, TestingSource(inp))
keyed_stream = op.key_on("key_on_user", stream, lambda e: e["user"])

ZERO_TD = timedelta(seconds=0)
clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
windower = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

windowed_stream = win.collect_window("add", keyed_stream, clock, windower)

op.output("out", windowed_stream, StdOutSink())