from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.connectors.kafka import KafkaSource
from bytewax.operators.window import EventClockConfig, TumblingWindow

from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow
import json


brokers = ["localhost:9092"]
flow = Dataflow("example")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["purchases_json"]))
processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
keyed_stream = op.key_on("key_on_user", processed, lambda e: e.key.decode('ascii'))

# define windowing
align_to = datetime(2024, 1, 18, tzinfo=timezone.utc)
ZERO_TD = timedelta(seconds=0)
clock = EventClockConfig(lambda e: datetime.utcfromtimestamp(json.loads(e.value.decode('ascii'))["time"]).replace(tzinfo=timezone.utc), wait_for_system_duration=ZERO_TD)
windower = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

windowed_stream = win.collect_window("add", keyed_stream, clock, windower)

op.inspect("kafka_says", windowed_stream)