import functools
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {
                "user_id": "1",
                "settings": {"dark_mode": True, "autosave": False, "admin": False},
            },
            {
                "user_id": "3",
                "settings": {"dark_mode": False, "autosave": False, "admin": True},
            },
            {
                "user_id": "2",
                "settings": {"dark_mode": True, "autosave": True, "admin": False},
            },
        ]
    ),
)
_ = op.inspect("check_inp", inp)


def key_pick_setting(field: str):
    def picker(msg):
        return (msg["user_id"], msg["settings"][field])

    return picker

def nf_key_pick_setting(field, msg):
    return (msg["user_id"], msg["settings"][field])


dark_modes = op.map("pick_dark_mode", inp, key_pick_setting("dark_mode"))
op.inspect("check_dark_mode", dark_modes)

autosaves = op.map("pick_autosave", inp, functools.partial(nf_key_pick_setting, "autosave"))
op.inspect("check_autosave", autosaves)

admins = op.map("pick_admin", inp, lambda msg: (msg["user_id"], msg["settings"]["admin"]))
op.inspect("check_admin", admins)
run_main(flow)