from dataclasses import dataclass

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int


app = faust.App("serializer", broker="kafka://localhost:29092")
clickevents_topic = app.topic("com.sefidian.clickevents", value_type=ClickEvent)

#
# TODO: Define an output topic for sanitized click events, without the user email
#
sanitized_topic = app.topic(
    "com.sefidian.streams.clickevents.sanitized",
    key_type=str,
    value_type=ClickEventSanitized,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        # TODO: Modify the incoming click event to remove the user email.
        #
        sanitized = ClickEventSanitized(
            timestamp=clickevent.timestamp, uri=clickevent.uri, number=clickevent.number
        )
        #
        # TODO: Send the data to the topic you created above
        #
        await sanitized_topic.send(key=sanitized.uri, value=sanitized)


if __name__ == "__main__":
    app.main()
