import faust

# docker exec -it cli-tools kafka-console-producer --topic=test --bootstrap-server broker0:29092
# TODO: Create the faust app with a name and broker
#
app = faust.App("hello-world-faust", broker="kafka://localhost:9092",)

#
# TODO: Connect Faust to a topic
#
topic = app.topic("test", value_type=str, value_serializer="raw")


# TODO: Provide an app agent to execute this function on topic event retrieval
@app.agent(topic)
async def clickevent(tests):
    async for ce in tests:
        print(ce)


if __name__ == "__main__":
    app.main()
