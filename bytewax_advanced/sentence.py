import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("use_def")
s = op.input("inp", flow, TestingSource(["quick brown fox jumped over the lazy dog"]))

s = op.flat_map("split", s, lambda x: x.split(" "))
op.inspect("out", s)
run_main(flow)