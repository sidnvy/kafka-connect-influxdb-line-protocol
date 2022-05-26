Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

This SMT supports inserting a UUID into the record Key or Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`uuid.field.name`| Field name for UUID | String | `uuid` | High |

Example on how to add to your connector:
```
transforms=insertuuid
transforms.insertuuid.type=com.github.cjmatta.kafka.connect.smt.InsertUuid$Value
transforms.insertuuid.uuid.field.name="uuid"
```


ToDO
* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT

-----------------------------------
# trades-ZB (Kakfa Message)
{"exchange":"ZB","symbol":"QNT-USDC","side":"sell","amount":1.6666,"price":65.7243,"id":"7734503","type":null,"timestamp":1653480224,"receipt_timestamp":1653480225.7766595}

-----------------------------------
# InfluxDB line protocol (VictoriaMetricsStore generated)
trades,exchange=ZB,symbol=QNT-USDC,side=sell  price=65.7234,amount=1.6666,timestamp=1653480224,receipt_timestamp=1653480225.7766595

# Prometheus Metric points
trades_price{exchange="ZB",symbol="QNT-USDC",side="sell"} 65.7243
trades_amount{exchange="ZB",symbol="QNT-USDC",side="sell"} 1.6666
trades_timestamp{exchange="ZB",symbol="QNT-USDC",side="sell"} 1653480224
trades_receipt_timestamp{exchange="ZB",symbol="QNT-USDC",side="sell"} 1653480225.7766595

-----------------------------------
# InfluxDB line protocol (Expected)
trades,exchange=ZB,symbol=QNT-USDC,side=sell  price=65.7234,amount=1.6666 1653480224
trades_receipt,exchange=ZB,symbol=QNT-USDC,side=sell  price=65.7234,amount=1.6666 1653480225.7766595

# Prometheus Metric points (Expected)
trades_price{exchange="ZB",symbol="QNT-USDC",side="sell"} 65.7243
trades_amount{exchange="ZB",symbol="QNT-USDC",side="sell"} 1.6666

trades_receipt_price{exchange="ZB",symbol="QNT-USDC",side="sell"} 65.7243
trades_receipt_amount{exchange="ZB",symbol="QNT-USDC",side="sell"} 1.6666

-----------------------------------
