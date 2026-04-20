#!/usr/bin/env python3
"""
Backfill direct PDF URLs into existing OATD metadata.

Usage:
    python -m src.backfill_pdf_urls
    python -m src.backfill_pdf_urls --limit 100
    python -m src.backfill_pdf_urls --force
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
import yaml
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawler.pdf import resolve_pdf_url
from src.storage.writer import MetadataWriter
from src.utils.logger import setup_logger


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_records(metadata_file: Path) -> list[dict]:
    records = []
    with metadata_file.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                record.setdefault("pdf_url", "")
                records.append(record)
    return records


def write_records_atomic(metadata_file: Path, records: list[dict]) -> None:
    tmp_file = metadata_file.with_suffix(metadata_file.suffix + ".tmp")
    with tmp_file.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    tmp_file.replace(metadata_file)


async def backfill_pdf_urls(
    records: list[dict],
    metadata_file: Path,
    concurrency: int,
    timeout: int,
    force: bool,
    limit: int,
    save_every: int,
) -> tuple[int, int]:
    pending_indexes = []
    for idx, record in enumerate(records):
        has_url = bool((record.get("url") or "").strip())
        has_pdf_url = bool((record.get("pdf_url") or "").strip())
        if not has_url:
            continue
        if force or not has_pdf_url:
            pending_indexes.append(idx)

    if limit > 0:
        pending_indexes = pending_indexes[:limit]

    logger.info(
        f"Loaded {len(records)} records, resolving pdf_url for {len(pending_indexes)} records"
    )

    if not pending_indexes:
        return 0, 0

    semaphore = asyncio.Semaphore(concurrency)
    resolved = 0
    failed = 0
    processed = 0

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        proxy=None,
    ) as client:
        async def process(idx: int) -> None:
            nonlocal resolved, failed, processed
            record = records[idx]
            try:
                async with semaphore:
                    pdf_url = await resolve_pdf_url(client, record["url"])
                if pdf_url:
                    record["pdf_url"] = pdf_url
                    resolved += 1
                else:
                    failed += 1
            except Exception as exc:
                logger.debug(f"Failed resolving pdf_url for {record.get('record_id', idx)}: {exc}")
                failed += 1

            processed += 1
            if save_every > 0 and processed % save_every == 0:
                write_records_atomic(metadata_file, records)
                logger.info(f"Checkpoint write: metadata saved after {processed} processed")

            if processed % 50 == 0 or processed == len(pending_indexes):
                logger.info(
                    f"Backfill progress: {processed}/{len(pending_indexes)} processed, "
                    f"{resolved} resolved, {failed} unresolved"
                )

        await asyncio.gather(*(process(idx) for idx in pending_indexes))

    return resolved, failed


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill direct PDF URLs into metadata")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent URL resolutions")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--limit", type=int, default=0, help="Only process the first N pending records")
    parser.add_argument("--force", action="store_true", help="Re-resolve records even if pdf_url already exists")
    parser.add_argument("--save-every", type=int, default=100, help="Persist JSONL after every N processed records")
    args = parser.parse_args()

    config = load_config(args.config)
    output_cfg = config.get("output", {})

    setup_logger(output_cfg.get("log_dir", "logs"))

    metadata_dir = Path(output_cfg.get("metadata_dir", "data/metadata"))
    metadata_file = metadata_dir / "theses.jsonl"
    if not metadata_file.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}")

    records = load_records(metadata_file)
    resolved, failed = asyncio.run(
        backfill_pdf_urls(
            records=records,
            metadata_file=metadata_file,
            concurrency=args.concurrency,
            timeout=args.timeout,
            force=args.force,
            limit=args.limit,
            save_every=args.save_every,
        )
    )

    if resolved or failed:
        write_records_atomic(metadata_file, records)
        writer = MetadataWriter(str(metadata_dir))
        writer.export_csv()

    logger.info(f"Backfill complete. Resolved: {resolved}, unresolved: {failed}")


if __name__ == "__main__":
    main()
