"""Base workflow — two-phase pipeline: scrape+save, then process+email+update."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from jobspy_v2.config import Settings
from jobspy_v2.core.dedup import Deduplicator
from jobspy_v2.core.email_gen import generate_email
from jobspy_v2.core.email_sender import send_email
from jobspy_v2.core.reporter import send_report
from jobspy_v2.core.scraper import scrape_jobs
from jobspy_v2.storage import create_storage_backend
from jobspy_v2.utils.email_utils import get_valid_recipients

if TYPE_CHECKING:
    import pandas as pd

    from jobspy_v2.storage.base import StorageBackend

logger = logging.getLogger(__name__)

CONTEXT_DIR = Path("contexts")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _safe_str(value: object) -> str:
    """Convert a value to string, treating NaN/None/lists cleanly."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value if v is not None)
    s = str(value)
    if s in ("nan", "NaN", "None", "NaT"):
        return ""
    return s


def _reason_to_stat_key(reason: str) -> str:
    """Map a dedup.can_send() reason string to its stats-dict key."""
    if "Already sent" in reason:
        return "skipped_dedup_exact"
    if "cooldown" in reason:
        return "skipped_dedup_domain"
    if "already contacted today" in reason:
        return "skipped_dedup_company"
    return "skipped_dedup_exact"  # safe fallback


# ------------------------------------------------------------------
# Workflow
# ------------------------------------------------------------------


class BaseWorkflow:
    """Template-method pipeline — subclasses only set ``mode``."""

    mode: str = ""  # "onsite" or "remote" — set by subclass

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage: StorageBackend = create_storage_backend(settings)
        self.dedup = Deduplicator(self.storage)
        self._context: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Execute the full two-phase pipeline.

        Returns 0 on success, 1 on fatal error.
        """
        start = time.monotonic()

        if self._should_skip_weekend():
            return 0

        self._context = self._load_context()

        stats: dict = {
            "total_scraped": 0,
            "jobs_with_emails": 0,
            "emails_sent": 0,
            "emails_failed": 0,
            "skipped_dedup_exact": 0,
            "skipped_dedup_domain": 0,
            "skipped_dedup_company": 0,
            "skipped_no_recipients": 0,
            "filtered_title": 0,
            "filtered_email": 0,
            "boards_queried": [],
        }

        try:
            result = scrape_jobs(self.settings, self.mode)
            stats["total_scraped"] = len(result.jobs)
            stats["boards_queried"] = result.boards_queried
            stats["filtered_email"] = result.total_raw - result.total_after_email_filter
            stats["filtered_title"] = (
                result.total_after_email_filter - result.total_after_title_filter
            )

            if result.jobs.empty:
                logger.info("[%s] No jobs found after filtering.", self.mode)
                self._send_report(stats, start)
                return 0

            # ── Phase 1: Scrape + Save ────────────────────────────────
            start_row = self._save_scraped_jobs(result.jobs)

            # ── Phase 2: Process + Email + Update ─────────────────────
            max_emails = self._get_max_emails()
            self._process_jobs(result.jobs, start_row, max_emails, stats)

        except Exception:
            logger.exception("[%s] Fatal error in pipeline.", self.mode)
            stats["emails_failed"] = int(stats["emails_failed"]) + 1
            self._send_report(stats, start)
            return 1

        self._send_report(stats, start)
        return 0

    # ------------------------------------------------------------------
    # Phase 1: Save scraped jobs to storage
    # ------------------------------------------------------------------

    def _save_scraped_jobs(self, jobs_df: pd.DataFrame) -> int:
        """Batch-write all scraped jobs to storage.

        Returns the 1-indexed starting row number of the newly written block.
        """
        now = datetime.now().isoformat(timespec="seconds")
        records: list[dict[str, str]] = []

        for _, row in jobs_df.iterrows():
            records.append(
                {
                    "date_scraped": now,
                    "board": _safe_str(row.get("site")),
                    "title": _safe_str(row.get("title")),
                    "company": _safe_str(row.get("company")),
                    "company_url": _safe_str(row.get("company_url")),
                    "location": _safe_str(row.get("location")),
                    "is_remote": _safe_str(row.get("is_remote")),
                    "job_url": _safe_str(row.get("job_url")),
                    "job_type": _safe_str(row.get("job_type")),
                    "date_posted": _safe_str(row.get("date_posted")),
                    "emails": _safe_str(row.get("emails")),
                    "salary_min": _safe_str(row.get("min_amount")),
                    "salary_max": _safe_str(row.get("max_amount")),
                    "salary_currency": _safe_str(row.get("currency")),
                    "salary_interval": _safe_str(row.get("interval")),
                    "skills": _safe_str(row.get("skills")),
                    "experience_range": _safe_str(row.get("experience_range")),
                    "job_level": _safe_str(row.get("job_level")),
                    "company_industry": _safe_str(row.get("company_industry")),
                    "email_sent": "Pending",
                    "skip_reason": "",
                    "email_recipient": "",
                }
            )

        start_row = self.storage.add_scraped_jobs(records)
        logger.info(
            "[%s] Phase 1 complete: %d jobs saved to storage (rows %d-%d)",
            self.mode,
            len(records),
            start_row,
            start_row + len(records) - 1,
        )
        return start_row

    # ------------------------------------------------------------------
    # Phase 2: Process each job — email + update row status
    # ------------------------------------------------------------------

    def _process_jobs(
        self,
        jobs_df: pd.DataFrame,
        start_row: int,
        max_emails: int,
        stats: dict,
    ) -> None:
        """Iterate jobs sequentially: validate -> dedup -> email -> update status."""
        sent_count = 0

        for i, (_, row) in enumerate(jobs_df.iterrows()):
            if sent_count >= max_emails:
                logger.info("[%s] Reached max emails (%d).", self.mode, max_emails)
                break

            row_number = start_row + i
            title = _safe_str(row.get("title"))
            company = _safe_str(row.get("company"))
            job_url = _safe_str(row.get("job_url"))
            description = _safe_str(row.get("description"))
            location = _safe_str(row.get("location"))
            is_remote = bool(row.get("is_remote", False))
            raw_emails = _safe_str(row.get("emails"))

            # ── Step 1: Extract valid recipients ──────────────────────
            recipients = get_valid_recipients(
                raw_emails, self.settings.email_filter_patterns
            )
            if not recipients:
                self._update_row_status(
                    row_number, "Skipped", "no_valid_recipients", ""
                )
                stats["skipped_no_recipients"] += 1
                continue

            stats["jobs_with_emails"] += 1
            primary_email = recipients[0]
            domain = primary_email.split("@")[1] if "@" in primary_email else ""

            # ── Step 2: Dedup check ───────────────────────────────────
            can_send, reason = self.dedup.can_send(primary_email, domain, company)
            if not can_send:
                stat_key = _reason_to_stat_key(reason)
                self._update_row_status(row_number, "Skipped", reason, primary_email)
                stats[stat_key] += 1
                logger.debug("Skipping %s: %s", primary_email, reason)
                continue

            # ── Step 3: Generate email ────────────────────────────────
            email_result = generate_email(
                job_title=title,
                company=company,
                job_description=description,
                settings=self.settings,
                context=self._context,
            )

            # ── Step 4: Send (or dry-run) ─────────────────────────────
            if self.settings.dry_run:
                logger.info(
                    "[DRY RUN] Would send to %s — %s at %s",
                    primary_email,
                    title,
                    company,
                )
                self._update_row_status(row_number, "DryRun", "", primary_email)
            else:
                success, error = send_email(
                    to_email=primary_email,
                    subject=email_result.subject,
                    body=email_result.body,
                    settings=self.settings,
                    resume_path=self.settings.resume_file_path,
                )
                if not success:
                    logger.error("Failed to send to %s: %s", primary_email, error)
                    self._update_row_status(
                        row_number,
                        "Failed",
                        f"smtp_error: {error}",
                        primary_email,
                    )
                    stats["emails_failed"] += 1
                    continue

                # Live success
                self._update_row_status(row_number, "Yes", "", primary_email)

            # ── Step 5: Record success (both live and dry-run) ────────
            self.dedup.mark_sent(
                email=primary_email,
                domain=domain,
                company=company,
                job_title=title,
                job_url=job_url,
                location=location,
                is_remote=is_remote,
                subject=email_result.subject,
                body_preview=email_result.body,
                mode=email_result.mode,
                word_count=email_result.word_count,
            )
            sent_count += 1
            stats["emails_sent"] += 1

            if sent_count < max_emails and not self.settings.dry_run:
                time.sleep(self.settings.email_interval_seconds)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _update_row_status(
        self,
        row_number: int,
        email_sent: str,
        skip_reason: str,
        email_recipient: str,
    ) -> None:
        """Update a single row's status — errors logged, pipeline continues."""
        try:
            self.storage.update_scraped_job_status(
                row_number, email_sent, skip_reason, email_recipient
            )
        except Exception:
            logger.exception(
                "Failed to update row %d status (email_sent=%s)",
                row_number,
                email_sent,
            )

    def _should_skip_weekend(self) -> bool:
        if self.settings.skip_weekends and datetime.now().weekday() >= 5:
            logger.info("[%s] Skipping — weekend.", self.mode)
            return True
        return False

    def _load_context(self) -> str:
        path = CONTEXT_DIR / "profile.md"
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.warning("Context file not found: %s — using empty context.", path)
            return ""

    def _get_max_emails(self) -> int:
        if self.mode == "onsite":
            return self.settings.onsite_max_emails_per_day
        return self.settings.remote_max_emails_per_day

    def _send_report(self, stats: dict, start: float) -> None:
        duration = time.monotonic() - start
        send_report(
            settings=self.settings,
            storage=self.storage,
            mode=self.mode,
            stats=stats,
            duration_seconds=duration,
        )
