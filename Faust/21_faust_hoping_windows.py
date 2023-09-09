import json
import random
from dataclasses import asdict, dataclass
from datetime import timedelta

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("hoping", broker="kafka://localhost:29092")
clickevents_topic = app.topic("com.sefidian.clickevents", value_type=ClickEvent)

#
# TODO: Define a hopping window of 1 minute with a 10-second step
#
uri_summary_table = app.Table("uri_summary", default=int).hopping(
    size=timedelta(minutes=1), step=timedelta(seconds=20)
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents.group_by(ClickEvent.uri):
        uri_summary_table[ce.uri] += ce.number
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()
