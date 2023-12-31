import json
from dataclasses import asdict, dataclass

import faust


#
# TODO: Define a ClickEvent Record Class
#
@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("deserializer", broker="kafka://localhost:29092")

#
# TODO: Provide the key and value type to the clickevent
#
clickevents_topic = app.topic(
    "com.sefidian.clickevents", key_type=str, value_type=ClickEvent,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=2))


if __name__ == "__main__":
    app.main()
