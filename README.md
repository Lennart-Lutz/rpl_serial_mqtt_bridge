# RPL Serial -> MQTT Bridge

This Home Assistant add-on reads newline-delimited JSON messages from a single RPL sink via serial and publishes structured MQTT topics.

Each incoming message is processed in three steps: First, the message is dispatched by its `type` field to the corresponding parser. The parser is registered through a parser-registry. Second, depending on the `version` field, the parser for this message version is selected. Then the correpsonding values from the message are extracted and published to MQTT topics using the message `id`. Each device ID therefore has its own isolated topic namespace.

Each parser is responsible for validating the message structure, building the MQTT topic structure and for publishing the values. This allows for multiple versions of the same message type to coexist, for backward compatibility across firmware versions (not directly corelated to the message version) and for multiple sensors to exist in the same RPL network.

Therefore, the received messages have to contain these fields: `type`, `version` and `id` besides the payload depending on the message type. For statistics, the message should also contain a field `rank`, that indicates the rank in the RPL routing tree.

## Supported Message Types

### Type `0xA3`, Version `1` -> Plant Hub v1

Expected JSON message structure:

- `type`
- `version`
- `id`
- `rank`
- `scon_bitmap`
- `scal_bitmap`
- `sensor_values[12]`

## MQTT Topic Structure

Base topic: `rpl` (default, configurable). All topics include the device `{ID}` from the message.

### Plant Hub Topics

rpl/plant_hub/{ID}/port1  
...  
rpl/plant_hub/{ID}/port12  

rpl/plant_hub/{ID}/conmask  
rpl/plant_hub/{ID}/calmask  

### General Statistics

rpl/stats/{ID}/rank  

## Extensibility

The bridge framework is designed for easy extension. To add support for:

- A new **message type**
- A new **version of an existing type**

You only need to:

1. Implement a new parser class
2. Register it in the parser registry under `(type, version)`