import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

ICON_TO_URL = {
    "dog_ico": "http://domain.invalid/static/dog_v1.png",
    "cat_ico": "http://domain.invalid/static/cat_v2.png",
    "rabbit_ico": "http://domain.invalid/static/rabbit_v1.png",
}

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {"user_id": "1", "avatar_icon_code": "dog_ico"},
            {"user_id": "3", "avatar_icon_code": "rabbit_ico"},
            {"user_id": "2", "avatar_icon_code": "dog_ico"},
        ]
    ),
)
op.inspect("check_inp", inp)


def icon_code_to_url(msg):
    code = msg.pop("avatar_icon_code")
    msg["avatar_icon_url"] = ICON_TO_URL[code]
    return msg


with_urls = op.map("with_url", inp, icon_code_to_url)
op.inspect("check_with_url", with_urls)
run_main(flow)