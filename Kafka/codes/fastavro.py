from fastavro import parse_schema, reader, writer

schema = {
    "doc": "A weather reading.",
    "name": "Weather",
    "namespace": "test",
    "type": "record",
    "fields": [
        {"name": "station", "type": "string"},
        {"name": "time", "type": "long"},
        {"name": "temp", "type": "int"},
    ],
}
parsed_schema = parse_schema(schema)

# 'records' can be an iterable (including generator)
records = [
    {"station": "011990-99999", "temp": 0, "time": 1433269388},
    {"station": "011990-99999", "temp": 22, "time": 1433270389},
    {"station": "011990-99999", "temp": -11, "time": 1433273379},
    {"station": "012650-99999", "temp": 111, "time": 1433275478},
]

# Writing
with open("weather.avro", "wb") as out:
    writer(out, parsed_schema, records)

# Reading
with open("weather.avro", "rb") as fo:
    for record in reader(fo):
        print(record)
