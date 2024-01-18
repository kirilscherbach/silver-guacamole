from bytewax import operators as op

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")

src_1 = [
    {"user_id": "456", "name": "Rumble"},
    {"user_id": "123", "name": "Bumble"},
    {"user_id": "123", "name": "Dumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))
keyed_inp_1 = op.key_on("key_stream_1", inp1, lambda x: x["user_id"])

#op.inspect("debug", keyed_inp_1)

src_2 = [
    {"user_id": "123", "email": "bee@bytewax.com"},
    {"user_id": "456", "email": "hive@bytewax.com"},
    {"user_id": "123", "email": "bee2@bytewax.com"},
    {"user_id": "123", "email": "bee3@bytewax.com"},
    {"user_id": "123", "email": "bee4@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
keyed_inp_2 = op.key_on("key_stream_2", inp2, lambda x: x["user_id"])

joined = op.join("joined", keyed_inp_1, keyed_inp_2)

out = op.output("out", joined, StdOutSink())