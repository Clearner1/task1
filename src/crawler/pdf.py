"""
PDF downloader — downloads thesis PDFs from external repository URLs.

OATD links to external repositories (doi.org, hdl.handle.net, etc.) which
redirect to university thesis pages. This module follows redirects, locates
PDF download links, and downloads the files.
"""

import asyncio
import json
import os
import re
from urllib.parse import urlparse

import httpx
from loguru import logger

from src.utils.checkpoint import Checkpoint

# Strip proxy env vars so httpx doesn't auto-detect SOCKS proxy
# (user's system has Clash/V2Ray setting ALL_PROXY etc.)
for _proxy_var in [
    "ALL_PROXY", "all_proxy",
    "HTTP_PROXY", "http_proxy",
    "HTTPS_PROXY", "https_proxy",
    "NO_PROXY", "no_proxy",
]:
    os.environ.pop(_proxy_var, None)



async def resolve_pdf_url(client: httpx.AsyncClient, thesis_url: str) -> str | None:
    """
    Follow redirects from a thesis URL (doi.org, hdl.handle.net, etc.)
    and attempt to find a direct PDF download link.
    """
    try:
        # Follow redirects to the landing page
        resp = await client.get(thesis_url, follow_redirects=True)
        final_url = str(resp.url)

        # If it's already a PDF
        content_type = resp.headers.get("content-type", "")
        if "application/pdf" in content_type or final_url.endswith(".pdf"):
            return final_url

        # Parse the landing page for PDF links
        html = resp.text
        pdf_patterns = [
            # Direct PDF links
            r'href=["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']',
            # Common download button patterns
            r'href=["\']([^"\']*download[^"\']*)["\']',
            r'href=["\']([^"\']*bitstream[^"\']*)["\']',
            r'href=["\']([^"\']*fulltext[^"\']*)["\']',
        ]

        for pattern in pdf_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                if match.startswith("http"):
                    return match
                elif match.startswith("/"):
                    parsed = urlparse(final_url)
                    return f"{parsed.scheme}://{parsed.netloc}{match}"

        logger.debug(f"No PDF link found on landing page: {final_url}")
        return None

    except (httpx.HTTPError, httpx.TimeoutException) as e:
        logger.debug(f"Error resolving PDF URL from {thesis_url}: {e}")
        return None


async def download_pdf(
    client: httpx.AsyncClient,
    pdf_url: str,
    output_path: str,
    max_size_mb: int = 100,
) -> bool:
    """Download a PDF file with size limit."""
    try:
        async with client.stream("GET", pdf_url, follow_redirects=True) as resp:
            if resp.status_code != 200:
                return False

            content_type = resp.headers.get("content-type", "")
            content_length = int(resp.headers.get("content-length", 0))

            # Skip if too large
            if content_length > max_size_mb * 1024 * 1024:
                logger.warning(f"PDF too large ({content_length // 1024 // 1024}MB): {pdf_url}")
                return False

            # Download to file
            total = 0
            with open(output_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    total += len(chunk)
                    if total > max_size_mb * 1024 * 1024:
                        logger.warning(f"PDF exceeded size limit during download: {pdf_url}")
                        break
                    f.write(chunk)

            # Verify it looks like a PDF
            with open(output_path, "rb") as f:
                header = f.read(5)
            if header != b"%PDF-":
                os.remove(output_path)
                return False

            return True

    except (httpx.HTTPError, httpx.TimeoutException, IOError) as e:
        logger.debug(f"Download error for {pdf_url}: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return False


async def download_pdfs_batch(
    metadata_file: str,
    output_dir: str,
    checkpoint: Checkpoint,
    concurrency: int = 5,
    timeout: int = 60,
    max_size_mb: int = 100,
    max_downloads: int = 0,
):
    """
    Download PDFs for all collected thesis records.

    Reads the JSONL metadata, resolves PDF URLs, and downloads concurrently.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load all records
    records = []
    with open(metadata_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    logger.info(f"Loaded {len(records)} records for PDF downloading")

    # Filter out already downloaded
    pending = [r for r in records if not checkpoint.is_pdf_done(r.get("record_id", r.get("url", "")))]
    logger.info(f"{len(pending)} PDFs pending download ({len(records) - len(pending)} already done)")

    semaphore = asyncio.Semaphore(concurrency)
    stats = {"success": 0, "failed": 0, "no_pdf": 0}

    async def process_one(record: dict):
        record_id = record.get("record_id", record.get("url", "unknown"))
        thesis_url = record.get("url", "")
        if not thesis_url:
            stats["no_pdf"] += 1
            return

        if max_downloads > 0 and stats["success"] >= max_downloads:
            return

        async with semaphore:
            if max_downloads > 0 and stats["success"] >= max_downloads:
                return
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, proxy=None) as client:
                # Step 1: Resolve PDF URL
                pdf_url = await resolve_pdf_url(client, thesis_url)
                if not pdf_url:
                    stats["no_pdf"] += 1
                    checkpoint.mark_pdf_failed(record_id)
                    return

                # Step 2: Download
                safe_name = re.sub(r'[^\w\-.]', '_', record_id)[:100] + ".pdf"
                output_path = os.path.join(output_dir, safe_name)

                ok = await download_pdf(client, pdf_url, output_path, max_size_mb)
                if ok:
                    stats["success"] += 1
                    checkpoint.mark_pdf_done(record_id)
                    if stats["success"] % 50 == 0:
                        logger.info(f"PDF progress: {stats['success']} downloaded, {stats['failed']} failed")
                else:
                    stats["failed"] += 1
                    checkpoint.mark_pdf_failed(record_id)

            checkpoint.save()

    # Run all downloads with concurrency control
    tasks = [process_one(r) for r in pending]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(
        f"PDF download complete! "
        f"Success: {stats['success']}, Failed: {stats['failed']}, No PDF: {stats['no_pdf']}"
    )

async def download_pdfs_from_queue(
    queue: asyncio.Queue,
    output_dir: str,
    checkpoint: Checkpoint,
    concurrency: int = 5,
    timeout: int = 60,
    max_size_mb: int = 100,
    max_downloads: int = 0,
):
    """
    Consumer that downloads PDFs concurrently as soon as they are added to the queue string list.
    """
    os.makedirs(output_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(concurrency)
    stats = {"success": 0, "failed": 0, "no_pdf": 0}

    async def process_one(record: dict):
        record_id = record.get("record_id", record.get("url", "unknown"))
        if checkpoint.is_pdf_done(record_id):
            return

        thesis_url = record.get("url", "")
        if not thesis_url:
            stats["no_pdf"] += 1
            return

        if max_downloads > 0 and stats["success"] >= max_downloads:
            return

        async with semaphore:
            if max_downloads > 0 and stats["success"] >= max_downloads:
                return
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, proxy=None) as client:
                pdf_url = await resolve_pdf_url(client, thesis_url)
                if not pdf_url:
                    stats["no_pdf"] += 1
                    checkpoint.mark_pdf_failed(record_id)
                    return

                safe_name = re.sub(r'[^\w\-.]', '_', record_id)[:100] + ".pdf"
                output_path = os.path.join(output_dir, safe_name)

                ok = await download_pdf(client, pdf_url, output_path, max_size_mb)
                if ok:
                    stats["success"] += 1
                    checkpoint.mark_pdf_done(record_id)
                    if stats["success"] % 10 == 0:
                        logger.info(f"Background PDF progress: {stats['success']} downloaded, {stats['failed']} failed")
                else:
                    stats["failed"] += 1
                    checkpoint.mark_pdf_failed(record_id)

            checkpoint.save()

    active_tasks = set()

    while True:
        batch = await queue.get()
        if batch is None: # Sentinel value
            queue.task_done()
            break
        
        if max_downloads > 0 and stats["success"] >= max_downloads:
            # We reached the limit, ignore new incoming tasks
            queue.task_done()
            continue

        # batch is a list of records extracted from a page
        for record in batch:
            task = asyncio.create_task(process_one(record))
            active_tasks.add(task)
            task.add_done_callback(active_tasks.discard)
        queue.task_done()

    # Wait for all remaining tasks to finish
    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)

    logger.info(
        f"Background PDF download complete! "
        f"Success: {stats['success']}, Failed: {stats['failed']}, No PDF: {stats['no_pdf']}"
    )
