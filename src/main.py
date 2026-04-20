#!/usr/bin/env python3
"""
OATD Thesis Crawler — Main Entry Point

Crawls Open Access Theses and Dissertations (oatd.org) using the user's
own Chrome browser via opencli, bypassing Cloudflare naturally.

Usage:
    python -m src.main                    # Full pipeline: crawl + download PDFs
    python -m src.main --crawl-only       # Only crawl metadata (no PDF download)
    python -m src.main --pdf-only         # Only download PDFs (metadata must exist)
    python -m src.main --export-csv       # Export metadata to CSV
    python -m src.main --stats            # Show current progress stats
"""

import argparse
import asyncio
import json
import os
import sys
import time

import yaml
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawler.search import crawl_search_results
from src.crawler.pdf import download_pdfs_batch
from src.storage.writer import MetadataWriter
from src.utils.checkpoint import Checkpoint
from src.utils.logger import setup_logger
from loguru import logger


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def show_stats(checkpoint: Checkpoint, writer: MetadataWriter):
    """Display current crawl progress."""
    console = Console()
    table = Table(title="🎓 OATD Crawler Status", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Metadata collected", str(checkpoint.total_collected))
    table.add_row("Metadata in file", str(writer.count))
    table.add_row("Last page offset", str(checkpoint.last_start))
    table.add_row("Failed pages", str(len(checkpoint.data.get("failed_pages", []))))
    table.add_row("PDFs downloaded", str(len(checkpoint.data.get("pdf_downloaded", []))))
    table.add_row("PDFs failed", str(len(checkpoint.data.get("pdf_failed", []))))

    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="OATD Thesis Crawler")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--crawl-only", action="store_true", help="Only crawl metadata")
    parser.add_argument("--pdf-only", action="store_true", help="Only download PDFs")
    parser.add_argument("--export-csv", action="store_true", help="Export to CSV")
    parser.add_argument("--stats", action="store_true", help="Show progress stats")
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    search_cfg = config.get("search", {})
    pacing_cfg = config.get("pacing", {})
    pdf_cfg = config.get("pdf", {})
    output_cfg = config.get("output", {})

    # Setup
    setup_logger(output_cfg.get("log_dir", "logs"))
    checkpoint = Checkpoint(output_cfg.get("checkpoint_file", "data/checkpoint.json"))
    writer = MetadataWriter(output_cfg.get("metadata_dir", "data/metadata"))

    # Stats mode
    if args.stats:
        show_stats(checkpoint, writer)
        return

    # CSV export mode
    if args.export_csv:
        writer.export_csv()
        return

    console = Console()
    console.print("\n[bold cyan]🎓 OATD Thesis Crawler[/bold cyan]")
    console.print("[dim]Using your Chrome browser via opencli — no anti-bot worries![/dim]\n")

    start_time = time.time()

    try:
        if not args.pdf_only and not args.crawl_only and pdf_cfg.get("enabled", True):
            # Concurrent Mode
            console.print("[bold yellow]▶ Crawling metadata and downloading PDFs concurrently...[/bold yellow]")
            from src.crawler.pdf import download_pdfs_from_queue
            
            async def run_concurrently():
                queue = asyncio.Queue()
                
                # We need to run the blocking crawler in a thread, sending data to the queue
                loop = asyncio.get_running_loop()
                
                def on_batch(results):
                    # Send results strictly from thread to the asyncio queue safely
                    loop.call_soon_threadsafe(queue.put_nowait, results)
                
                def run_crawler():
                    total_crawled = crawl_search_results(
                        query=search_cfg.get("query", "analysis"),
                        max_papers=search_cfg.get("max_papers", 5500),
                        sort=search_cfg.get("sort", "date"),
                        delay_range=tuple(pacing_cfg.get("delay_range", [2.0, 4.0])),
                        max_retries=pacing_cfg.get("max_retries", 3),
                        writer=writer,
                        checkpoint=checkpoint,
                        on_batch_extracted=on_batch,
                    )
                    # Notify queue that crawler is done
                    loop.call_soon_threadsafe(queue.put_nowait, None)
                    return total_crawled

                # Consumer task
                pdf_consumer = asyncio.create_task(
                    download_pdfs_from_queue(
                        queue=queue,
                        output_dir=output_cfg.get("pdf_dir", "data/pdfs"),
                        checkpoint=checkpoint,
                        concurrency=pdf_cfg.get("concurrency", 5),
                        timeout=pdf_cfg.get("timeout", 60),
                        max_size_mb=pdf_cfg.get("max_size_mb", 100),
                        max_downloads=pdf_cfg.get("max_downloads", 0),
                    )
                )

                # Wait for crawler thread to finish
                total = await asyncio.to_thread(run_crawler)
                
                # Wait for consumer to finish the queue
                await pdf_consumer
                
                return total

            total = asyncio.run(run_concurrently())
            writer.export_csv()
            
        elif not args.pdf_only:
            # Phase 1: Crawl metadata only
            console.print("[bold yellow]▶ Phase 1: Crawling search results...[/bold yellow]")

            total = crawl_search_results(
                query=search_cfg.get("query", "analysis"),
                max_papers=search_cfg.get("max_papers", 5500),
                sort=search_cfg.get("sort", "date"),
                delay_range=tuple(pacing_cfg.get("delay_range", [2.0, 4.0])),
                max_retries=pacing_cfg.get("max_retries", 3),
                writer=writer,
                checkpoint=checkpoint,
            )

            writer.export_csv()

        elif not args.crawl_only and pdf_cfg.get("enabled", True):
            # Phase 2: Download PDFs only
            console.print("\n[bold yellow]▶ Phase 2: Downloading PDFs...[/bold yellow]")

            metadata_file = os.path.join(
                output_cfg.get("metadata_dir", "data/metadata"), "theses.jsonl"
            )

            if not os.path.exists(metadata_file):
                console.print("[red]No metadata file found. Run crawl first.[/red]")
                return

            asyncio.run(
                download_pdfs_batch(
                    metadata_file=metadata_file,
                    output_dir=output_cfg.get("pdf_dir", "data/pdfs"),
                    checkpoint=checkpoint,
                    concurrency=pdf_cfg.get("concurrency", 5),
                    timeout=pdf_cfg.get("timeout", 60),
                    max_size_mb=pdf_cfg.get("max_size_mb", 100),
                    max_downloads=pdf_cfg.get("max_downloads", 0),
                )
            )

        # Final stats
        total_elapsed = time.time() - start_time
        console.print(f"\n[bold green]🎉 All done![/bold green] Total time: {total_elapsed / 60:.1f} minutes")
        show_stats(checkpoint, writer)

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Progress saved to checkpoint.")
        checkpoint.save()
        console.print("\n[yellow]Interrupted. Progress saved — run again to resume.[/yellow]")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        checkpoint.save()
        raise


if __name__ == "__main__":
    main()
