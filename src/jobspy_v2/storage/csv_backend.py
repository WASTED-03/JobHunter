"""CSV storage backend — local file fallback when Google Sheets is unavailable."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from jobspy_v2.storage.base import (
    RUN_STATS_COLUMNS,
    SCRAPED_JOB_COLUMNS,
    SENT_EMAIL_COLUMNS,
)

logger = logging.getLogger(__name__)


def _ensure_csv(path: Path, columns: tuple[str, ...]) -> None:
    """Create the CSV file with headers if it doesn't exist."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=columns).writeheader()
    logger.info("Created CSV file: %s", path)


def _read_csv(path: Path, columns: tuple[str, ...]) -> list[dict[str, str]]:
    """Read all rows from a CSV file as list of dicts."""
    _ensure_csv(path, columns)
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _append_rows(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, str]],
) -> int:
    """Append rows to a CSV file, creating it if needed.

    Returns the starting line number (1-indexed, after header) where the
    first new row was inserted.
    """
    _ensure_csv(path, columns)
    # Count existing lines to determine start row
    with path.open("r", encoding="utf-8") as f:
        existing_lines = sum(1 for _ in f)
    start_row = existing_lines + 1  # 1-indexed (header is line 1)

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)
    return start_row


class CsvBackend:
    """CSV-based storage backend with three separate files."""

    def __init__(self, base_dir: str | Path = ".") -> None:
        self._base = Path(base_dir)
        self._sent_path = self._base / "sent_emails.csv"
        self._jobs_path = self._base / "scraped_jobs.csv"
        self._stats_path = self._base / "run_stats.csv"

    def get_sent_emails(self) -> list[dict[str, str]]:
        """Return all previously sent email records."""
        return _read_csv(self._sent_path, SENT_EMAIL_COLUMNS)

    def add_sent_email(self, record: dict[str, str]) -> None:
        """Append a single sent email record."""
        _append_rows(self._sent_path, SENT_EMAIL_COLUMNS, [record])
        logger.debug("Recorded sent email: %s", record.get("email", "?"))

    def add_scraped_jobs(self, records: list[dict[str, str]]) -> int:
        """Append a batch of scraped job records.

        Returns the starting row number (1-indexed, after header).
        """
        if not records:
            # Return next available row
            _ensure_csv(self._jobs_path, SCRAPED_JOB_COLUMNS)
            with self._jobs_path.open("r", encoding="utf-8") as f:
                return sum(1 for _ in f) + 1

        return _append_rows(self._jobs_path, SCRAPED_JOB_COLUMNS, records)

    def update_scraped_job_status(
        self,
        row_number: int,
        email_sent: str,
        skip_reason: str,
        email_recipient: str,
    ) -> None:
        """Update email_sent, skip_reason, and email_recipient for a scraped job row.

        For CSV, this reads the file, updates the matching row in memory,
        and rewrites the file.
        """
        _ensure_csv(self._jobs_path, SCRAPED_JOB_COLUMNS)
        rows = _read_csv(self._jobs_path, SCRAPED_JOB_COLUMNS)

        # row_number is 1-indexed with header at row 1, so data index = row_number - 2
        data_index = row_number - 2
        if 0 <= data_index < len(rows):
            rows[data_index]["email_sent"] = email_sent
            rows[data_index]["skip_reason"] = skip_reason
            rows[data_index]["email_recipient"] = email_recipient
        else:
            logger.warning(
                "CSV row %d out of range (%d rows), skipping update",
                row_number,
                len(rows),
            )
            return

        # Rewrite the entire CSV
        with self._jobs_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=list(SCRAPED_JOB_COLUMNS), extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)
        logger.debug("Updated CSV row %d: email_sent=%s", row_number, email_sent)

    def get_run_stats(self) -> list[dict[str, str]]:
        """Return all run statistics records."""
        return _read_csv(self._stats_path, RUN_STATS_COLUMNS)

    def add_run_stats(self, stats: dict[str, str]) -> None:
        """Append a single run statistics record."""
        _append_rows(self._stats_path, RUN_STATS_COLUMNS, [stats])
        logger.info("Saved run stats for %s", stats.get("date", "?"))
