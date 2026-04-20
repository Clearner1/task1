"""
Search crawler — paginates through OATD search results using opencli browser.

Flow:
  1. opencli browser open <search_url>   → navigate Chrome
  2. opencli browser eval <extract_js>   → extract 30 results as JSON
  3. Save to JSONL + update checkpoint
  4. Sleep random delay
  5. Repeat for next page
"""

import random
import time

from loguru import logger

from src.crawler.browser import eval_js_json, open_url
from src.parser.oatd import EXTRACT_PAGINATION_JS, EXTRACT_SEARCH_RESULTS_JS
from src.storage.writer import MetadataWriter
from src.utils.checkpoint import Checkpoint


def build_search_url(query: str, start: int = 0) -> str:
    """
    Build OATD search URL with pagination.

    OATD quirks discovered during exploration:
    - First page: no 'start' param at all (start=0 means first page)
    - Subsequent pages: start=31, 61, 91, ...
    - 'sort' param MUST be omitted — it silently breaks pagination
    """
    from urllib.parse import quote_plus
    q = quote_plus(query)
    if start <= 0:
        return f"https://oatd.org/oatd/search?q={q}"
    return f"https://oatd.org/oatd/search?q={q}&start={start}"


def crawl_search_results(
    query: str = "analysis",
    max_papers: int = 5500,
    sort: str = "date",
    delay_range: tuple[float, float] = (2.0, 4.0),
    max_retries: int = 3,
    writer: MetadataWriter | None = None,
    checkpoint: Checkpoint | None = None,
    on_batch_extracted=None,
) -> int:
    """
    Crawl OATD search results page by page.

    Returns:
        Total number of records collected.
    """
    page_size = 30
    collected = 0
    start = 0  # 0 = first page (no start param)

    # Resume from checkpoint if available
    if checkpoint and checkpoint.last_start > 0:
        start = checkpoint.last_start + page_size
        collected = checkpoint.total_collected
        logger.info(f"Resuming from start={start}, already collected={collected}")

    total_available = None

    while collected < max_papers:
        url = build_search_url(query, start)
        page_num = 1 if start == 0 else (start - 1) // page_size + 1
        logger.info(f"[Page {page_num}] Fetching start={start} (collected: {collected}/{max_papers})")

        # Navigate to search page with retries
        success = False
        for attempt in range(1, max_retries + 1):
            open_url(url, wait=1.5)

            # Check pagination info first
            page_info = eval_js_json(EXTRACT_PAGINATION_JS)
            if page_info and not page_info.get("has_error") and page_info.get("total", 0) > 0:
                if total_available is None:
                    total_available = page_info["total"]
                    logger.info(f"Total available results: {total_available:,}")
                success = True
                break
            elif page_info and page_info.get("has_error"):
                logger.warning(f"Search error on attempt {attempt}/{max_retries}, retrying in 10s...")
                time.sleep(10)
            else:
                logger.warning(f"No results on attempt {attempt}/{max_retries}, retrying in 5s...")
                time.sleep(5)

        if not success:
            logger.error(f"Failed to load page start={start} after {max_retries} retries")
            if checkpoint:
                checkpoint.add_failed_page(start)
                checkpoint.save()
            start += page_size
            continue

        # Extract results from current page
        results = eval_js_json(EXTRACT_SEARCH_RESULTS_JS)

        if not results:
            logger.warning(f"No results extracted from start={start}")
            if checkpoint:
                checkpoint.add_failed_page(start)
            start += page_size
            time.sleep(random.uniform(*delay_range))
            continue

        logger.info(f"  → Extracted {len(results)} records from this page")

        # Deduplicate against already collected
        if checkpoint:
            existing_ids = set(checkpoint.collected_ids)
            new_results = [r for r in results if r.get("record_id") not in existing_ids]
            if len(new_results) < len(results):
                logger.debug(f"  → Filtered {len(results) - len(new_results)} duplicates")
            results = new_results

        # Save results
        if writer and results:
            writer.write_batch(results)

        if checkpoint and results:
            new_ids = [r.get("record_id", r.get("url", "")) for r in results]
            checkpoint.add_collected_ids(new_ids)
            checkpoint.last_start = start
            checkpoint.save()

        # Call the callback to pass data to consumer
        if on_batch_extracted and results:
            on_batch_extracted(results)

        collected += len(results)

        # Check if we've reached the end of available results
        if page_info and start >= page_info.get("total", float("inf")):
            logger.info("Reached end of available results")
            break

        # Move to next page (0 → 31, 31 → 61, 61 → 91, ...)
        start = 31 if start == 0 else start + page_size

        # Random delay to be polite
        delay = random.uniform(*delay_range)
        logger.debug(f"  → Sleeping {delay:.1f}s before next page")
        time.sleep(delay)

    logger.info(f"Search crawl complete! Total collected: {collected}")
    return collected
