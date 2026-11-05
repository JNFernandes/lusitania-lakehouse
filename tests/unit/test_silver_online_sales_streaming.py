"""
The online-sales steps on a STREAMING DataFrame, as the Project 3 consumer uses them.

The stream here reads text files from a folder (a streaming source that needs no Kafka);
each line is one order's JSON, exactly what a Kafka message value contains.
"""
import json

import pytest
from pyspark.errors import AnalysisException

from lusitania.silver import online_sales


def order(number, created, sku="sku-00010", status="paid"):
    return json.dumps({
        "orderNumber": number, "orderDate": "2025-06-10T14:32:00Z", "status": status, "currency": "EUR",
        "createdAt": {"$date": created}, "customer": {"code": "C00001", "email": "a@b.pt", "country": "PT"},
        "shipping": {"city": "Porto", "country": "PT", "cost": 0.0}, "payment": {"method": "card"},
        "items": [{"line": 1, "sku": sku, "name": "Mouse", "qty": 2, "unitPrice": 10.0}],
        "schemaVersion": 2, "couponCode": "WELCOME10",
    })


@pytest.fixture
def raw_stream(spark, tmp_path):
    folder = tmp_path / "incoming"
    folder.mkdir()
    (folder / "batch1.json").write_text("\n".join([
        order("WEB00000001", "2025-06-10T14:32:00Z"),
        order("WEB00000001", "2025-06-10T14:32:05Z", status="shipped"),   # duplicate, 5 s later
        order("WEB00000002", "2025-06-10T14:33:00Z"),
    ]))
    raw = (spark.readStream.format("text").load(str(folder))
                .withColumnRenamed("value", "raw_json")
                .selectExpr("raw_json", "'stream' AS _batch_id"))
    assert raw.isStreaming
    return raw


def run_once(df, tmp_path, name):
    query = (df.writeStream.format("memory").queryName(name)
               .option("checkpointLocation", str(tmp_path / f"ckpt_{name}"))
               .trigger(availableNow=True).start())          # process what is there, then stop
    query.awaitTermination()
    return df.sparkSession.table(name).collect()


def test_shared_steps_run_on_a_stream(spark, raw_stream, tmp_path):
    rates = spark.createDataFrame([], "rate_date date, currency string, rate decimal(12,6)")
    orders = (online_sales.parse(raw_stream)
              .withWatermark("created_ts", "10 minutes")
              .dropDuplicatesWithinWatermark(["orderNumber"]))          # the STREAMING dedup
    lines = online_sales.to_eur(online_sales.explode_lines(orders), rates, source_system="kafka")

    rows = {r.order_number: r for r in run_once(lines, tmp_path, "live_lines")}
    assert sorted(rows) == ["WEB00000001", "WEB00000002"]               # the duplicate is gone
    assert rows["WEB00000001"].sku == "SKU-00010"                       # the same cleaning as batch
    assert str(rows["WEB00000001"].net_amount_eur) == "20.00"
    assert rows["WEB00000001"].source_system == "kafka"


def test_parse_keeps_the_new_optional_fields(spark, raw_stream, tmp_path):
    rows = run_once(online_sales.parse(raw_stream).select("orderNumber", "schemaVersion", "couponCode"),
                    tmp_path, "parsed")
    assert {(r.schemaVersion, r.couponCode) for r in rows} == {(2, "WELCOME10")}


def test_batch_dedup_is_refused_on_a_stream(raw_stream, tmp_path):
    # row_number over ALL rows of an order: impossible on a stream, which never has all rows
    with pytest.raises(AnalysisException, match="NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING"):
        run_once(online_sales.dedupe_latest(online_sales.parse(raw_stream)), tmp_path, "refused")
