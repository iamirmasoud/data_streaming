from dataclasses import dataclass

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("filters", broker="kafka://localhost:29092")
clickevents_topic = app.topic("com.sefidian.clickevents", value_type=ClickEvent)
popular_uris_topic = app.topic(
    "com.sefidian.clickevents.popular", key_type=str, value_type=ClickEvent,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # TODO: Filter clickevents to only those with a number higher than or
    #       equal to 100
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#filter-filter-values-to-omit-from-stream
    #
    async for clickevent in clickevents.filter(lambda x: x.number >= 400):
        #
        # TODO: Send the message to the `popular_uris_topic` with a key and value.
        #
        await popular_uris_topic.send(key=clickevent.uri, value=clickevent)


if __name__ == "__main__":
    app.main()
