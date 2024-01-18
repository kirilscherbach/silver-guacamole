import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

src_items = [
    {"user_id": 1, "txn_amount": 100.0},
    {"user_id": 1, "txn_amount": 17.0},
    {"user_id": 2, "txn_amount": 30.0},
    {"user_id": 2, "txn_amount": 5.0},
    {"user_id": 1, "txn_amount": 120.0},
    {"user_id": 2, "txn_amount": 45.0},
    {"user_id": 1, "txn_amount": 99.0},
]

flow = Dataflow("stateful_map_eg")
inp = op.input("inp", flow, TestingSource(src_items))
op.inspect("check_inp", inp)

keyed_inp = op.key_on("key", inp, lambda msg: str(msg["user_id"]))
op.inspect("check_keyed", keyed_inp)

keyed_amounts = op.map_value("pick_amount", keyed_inp, lambda msg: msg["txn_amount"])
op.inspect("check_keyed_amount", keyed_amounts)

def running_builder():
    return []


def calc_running_mean(values, new_value):
    print("Before state:", values)

    print("New value:", new_value)
    values.append(new_value)
    while len(values) > 3:
        values.pop(0)

    print("After state:", values)

    running_mean = sum(values) / len(values)
    print("Running mean:", running_mean)
    print()
    return (values, running_mean)


running_means = op.stateful_map(
    "running_mean", keyed_amounts, running_builder, calc_running_mean
)
op.inspect("check_running_mean", running_means)
run_main(flow)