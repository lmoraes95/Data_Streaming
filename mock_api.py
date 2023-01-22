from flask import Flask, Response, stream_with_context
import time
import uuid
import random

APP = Flask(__name__)


@APP.route("/data_request/<int:rowcount>", methods=["GET"])
def get_data(rowcount):
    def f():
        for _i in range(rowcount):
            time.sleep(.01)
            txid = uuid.uuid4()
            print(txid)
            expenditure = round(random.uniform(0, 50000), 2)
            yield f"('{txid}', {expenditure})\n"
    return Response(stream_with_context(f()))

if __name__ == "__main__":
    APP.run(debug=True)
