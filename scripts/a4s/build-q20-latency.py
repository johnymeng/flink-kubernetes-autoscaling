#!/usr/bin/env python3
"""Build the optional Kafka-to-output latency variant of the pinned Q20 workload."""

import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess
import tempfile
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


DDL = "queries/ddl_kafka_unique.sql"
BASE_QUERY = "queries/q20_unique.sql"
LATENCY_QUERY = "queries/q20_latency_unique.sql"
FACTORIES = "META-INF/services/org.apache.flink.table.factories.Factory"
FACTORY = "io.a4s.benchmark.q20.Q20KafkaLatencySinkFactory"
SOURCE = Path(__file__).with_name("q20-latency") / "Q20KafkaLatencySinkFactory.java"


def replace_sql(sql, anchor, replacement):
    if sql.count(anchor) != 1:
        raise ValueError(f"Unexpected SQL at {anchor!r}")
    return sql.replace(anchor, replacement)


def build(base, output, classes, sample_every):
    with ZipFile(base) as source:
        names = source.namelist()
        if len(set(names)) != len(names) or LATENCY_QUERY in names:
            raise ValueError("Expected the original Nexmark JAR with unique entries")
        ddl = source.read(DDL).decode()
        preceding, bid = ddl.split("CREATE TABLE bid_kafka (\n", maxsplit=1)
        bid = replace_sql(bid, "    WATERMARK FOR `dateTime`",
                          "    bid_kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,\n"
                          "    WATERMARK FOR `dateTime`")
        query = source.read(BASE_QUERY).decode()
        query = replace_sql(query, "    auction_extra  VARCHAR\n",
                            "    auction_extra  VARCHAR,\n    bid_kafka_timestamp TIMESTAMP_LTZ(3)\n")
        query = replace_sql(query, "    'connector' = 'blackhole'",
                            f"    'connector' = 'a4s-q20-latency',\n    'sample-every' = '{sample_every}'")
        query = replace_sql(query, "category, A.extra\n", "category, A.extra, B.bid_kafka_timestamp\n")
        query = replace_sql(query, "    bid AS B INNER JOIN", "    bid_kafka AS B INNER JOIN")
        factories = source.read(FACTORIES).decode().rstrip() + "\n" if FACTORIES in names else ""
        additions = {FACTORIES: factories + FACTORY + "\n", LATENCY_QUERY: query}
        additions.update({path.relative_to(classes).as_posix(): path.read_bytes()
                          for path in sorted(classes.rglob("*.class"))})
        if (set(names) & additions.keys()) - {FACTORIES}:
            raise ValueError("The benchmark JAR already contains Q20 latency classes")
        timestamp = source.getinfo(BASE_QUERY).date_time
        with ZipFile(output, "x", ZIP_DEFLATED) as target:
            for entry in source.infolist():
                if entry.filename == DDL:
                    target.writestr(entry, preceding + "CREATE TABLE bid_kafka (\n" + bid)
                elif entry.filename != FACTORIES:
                    target.writestr(entry, source.read(entry))
            for name, contents in additions.items():
                target.writestr(ZipInfo(name, timestamp), contents, compress_type=ZIP_DEFLATED)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("base_jar", type=Path)
    cli.add_argument("output_jar", type=Path)
    cli.add_argument("--base-sha256", required=True, help="expected SHA-256 from the workload receipt")
    cli.add_argument("--flink-classpath", required=True, help="include the packaged Flink lib/*")
    cli.add_argument("--sample-every", type=int, default=1024)
    args = cli.parse_args()
    if not 1 <= args.sample_every <= 2**31 - 1:
        cli.error("--sample-every must be a positive Java integer")
    if not re.fullmatch(r"[a-f0-9]{64}", args.base_sha256):
        cli.error("--base-sha256 must be a lowercase SHA-256 digest")
    digest = hashlib.sha256()
    with args.base_jar.open("rb") as jar:
        for chunk in iter(lambda: jar.read(1 << 20), b""):
            digest.update(chunk)
    if digest.hexdigest() != args.base_sha256:
        cli.error("base Nexmark JAR differs from the expected digest")
    javac = Path(os.environ["JAVA_HOME"]) / "bin/javac" if "JAVA_HOME" in os.environ else "javac"
    with tempfile.TemporaryDirectory(prefix=".q20-latency-", dir=args.output_jar.parent) as scratch:
        classes = Path(scratch) / "classes"
        classes.mkdir()
        subprocess.run([javac, "--release", "11", "-cp", args.flink_classpath,
                        "-d", classes, SOURCE], check=True)
        candidate = Path(scratch) / "workload.jar"
        build(args.base_jar, candidate, classes, args.sample_every)
        os.link(candidate, args.output_jar)


if __name__ == "__main__":
    main()
