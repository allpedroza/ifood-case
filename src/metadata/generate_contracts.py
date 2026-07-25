"""Gera contratos determinísticos a partir dos PDFs oficiais da NYC TLC."""

import argparse
import hashlib
import json
import re
from pathlib import Path

from pypdf import PdfReader


EXTRACTOR_VERSION = "1.0.0"

DATASETS = {
    "yellow": {
        "table": "bronze_yellow_trips",
        "dictionary_slug": "yellow",
        "pdf": "data_dictionary_trip_records_yellow.pdf",
        "columns": {
            "VendorID": "long",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp",
            "passenger_count": "double",
            "trip_distance": "double",
            "RatecodeID": "double",
            "store_and_fwd_flag": "string",
            "PULocationID": "long",
            "DOLocationID": "long",
            "payment_type": "long",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "congestion_surcharge": "double",
            "airport_fee": "double",
            "cbd_congestion_fee": "double",
        },
    },
    "green": {
        "table": "bronze_green_trips",
        "dictionary_slug": "green",
        "pdf": "data_dictionary_trip_records_green.pdf",
        "columns": {
            "VendorID": "long",
            "lpep_pickup_datetime": "timestamp",
            "lpep_dropoff_datetime": "timestamp",
            "store_and_fwd_flag": "string",
            "RatecodeID": "double",
            "PULocationID": "long",
            "DOLocationID": "long",
            "passenger_count": "double",
            "trip_distance": "double",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "payment_type": "long",
            "trip_type": "double",
            "congestion_surcharge": "double",
            "cbd_congestion_fee": "double",
        },
    },
    "fhv": {
        "table": "bronze_fhv_trips",
        "dictionary_slug": "fhv",
        "pdf": "data_dictionary_trip_records_fhv.pdf",
        "columns": {
            "dispatching_base_num": "string",
            "pickup_datetime": "timestamp",
            "dropOff_datetime": "timestamp",
            "PUlocationID": "long",
            "DOlocationID": "long",
            "SR_Flag": "long",
            "Affiliated_base_number": "string",
        },
    },
    "fhvhv": {
        "table": "bronze_fhvhv_trips",
        "dictionary_slug": "hvfhs",
        "pdf": "data_dictionary_trip_records_hvfhs.pdf",
        "columns": {
            "hvfhs_license_num": "string",
            "dispatching_base_num": "string",
            "originating_base_num": "string",
            "request_datetime": "timestamp",
            "on_scene_datetime": "timestamp",
            "pickup_datetime": "timestamp",
            "dropoff_datetime": "timestamp",
            "PULocationID": "long",
            "DOLocationID": "long",
            "trip_miles": "double",
            "trip_time": "long",
            "base_passenger_fare": "double",
            "tolls": "double",
            "bcf": "double",
            "sales_tax": "double",
            "congestion_surcharge": "double",
            "airport_fee": "double",
            "tips": "double",
            "driver_pay": "double",
            "shared_request_flag": "string",
            "shared_match_flag": "string",
            "access_a_ride_flag": "string",
            "wav_request_flag": "string",
            "wav_match_flag": "string",
            "cbd_congestion_fee": "double",
        },
        "governance_note": (
            "The official dictionary uses hvfhs; the monthly dataset uses fhvhv."
        ),
    },
}

ZONE_COLUMNS = {
    "LocationID": ("integer", "Location identifier used by trip records."),
    "Borough": ("string", "New York City borough."),
    "Zone": ("string", "NYC TLC taxi zone name."),
    "service_zone": ("string", "NYC TLC service zone classification."),
}


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def normalize(text: str) -> str:
    text = re.sub(
        r"Data Dictionary\s*[–-].*?Page \d+ of \d+",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", text).strip()


def pdf_text(path: Path) -> str:
    return normalize(" ".join(page.extract_text() or "" for page in PdfReader(path).pages))


def extract_description(text: str) -> str:
    match = re.search(
        r"(This data dictionary describes .*?)(?= For data dictionaries| As of \d{4}| Field Name)",
        text,
    )
    if not match:
        raise ValueError("dataset description not found in PDF")
    return match.group(1).strip()


def extract_columns(text: str, field_names):
    marker = re.search(r"Field Name\s+Description", text)
    if not marker:
        raise ValueError("Field Name / Description marker not found")
    body = text[marker.end() :].strip()
    positions = []
    cursor = 0
    for name in field_names:
        match = re.search(rf"(?<!\w){re.escape(name)}(?!\w)", body[cursor:])
        if not match:
            raise ValueError(f"field not found in official dictionary: {name}")
        start = cursor + match.start()
        end = cursor + match.end()
        positions.append((name, start, end))
        cursor = end

    descriptions = {}
    for index, (name, _, end) in enumerate(positions):
        next_start = positions[index + 1][1] if index + 1 < len(positions) else len(body)
        description = body[end:next_start].strip()
        if not description:
            raise ValueError(f"empty description extracted for field: {name}")
        descriptions[name] = description
    return descriptions


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_contract(path: Path, contract):
    serialized = json.dumps(contract, ensure_ascii=False, indent=2) + "\n"
    path.write_text(serialized, encoding="utf-8")


def main():
    args = arguments()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for dataset, config in DATASETS.items():
        source = args.metadata_dir / config["pdf"]
        text = pdf_text(source)
        descriptions = extract_columns(text, config["columns"].keys())
        contract = {
            "dataset": dataset,
            "table": config["table"],
            "description": extract_description(text),
            "dictionary_slug": config["dictionary_slug"],
            "source_file": config["pdf"],
            "source_sha256": sha256(source),
            "extractor_version": EXTRACTOR_VERSION,
            "language": "en",
            "columns": {
                name: {
                    "description": descriptions[name],
                    "type": data_type,
                }
                for name, data_type in config["columns"].items()
            },
        }
        if config.get("governance_note"):
            contract["governance_note"] = config["governance_note"]
        write_contract(args.output_dir / f"{dataset}.yml", contract)

    zone_contract = {
        "dataset": "taxi_zones",
        "table": "bronze_taxi_zone_lookup",
        "description": "Official NYC TLC taxi zone lookup.",
        "source_file": "taxi_zone_lookup.csv",
        "source_sha256": sha256(args.metadata_dir / "taxi_zone_lookup.csv"),
        "extractor_version": EXTRACTOR_VERSION,
        "language": "en",
        "columns": {
            name: {"type": data_type, "description": description}
            for name, (data_type, description) in ZONE_COLUMNS.items()
        },
    }
    write_contract(args.output_dir / "taxi_zones.yml", zone_contract)
    print(f"contracts generated deterministically in {args.output_dir}")


if __name__ == "__main__":
    main()
