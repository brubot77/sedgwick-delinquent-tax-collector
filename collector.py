from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import random
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from playwright.async_api import (
    Browser,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

START_URL = "https://ssc.sedgwickcounty.org/propertytax/delinquencies.aspx"
INTRO_URL = (
    "https://ssc.sedgwickcounty.org/propertytax/"
    "delinquenciesintro.aspx?"
    "returnURL=%2Fpropertytax%2Fdelinquencies.aspx"
)

NO_RESULTS_RE = re.compile(
    r"\b(?:"
    r"0\s+RESULTS?|"
    r"NO\s+PROPERTIES\s+WERE\s+FOUND|"
    r"NO\s+(?:MATCHING\s+)?RESULTS?|"
    r"NO\s+DELINQUENT(?:\s+PROPERTIES)?"
    r")\b",
    re.I,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_owner(value: str) -> str:
    value = normalize_space(value).upper()
    value = value.replace("&AMP;", "&")
    value = re.sub(r"[^A-Z0-9&' -]", " ", value)
    return normalize_space(value)


def normalize_identifier(value: str, width: int | None = None) -> str:
    digits = re.sub(r"\D", "", value or "")
    if width and digits:
        return digits.zfill(width)
    return digits


def safe_slug(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    digest = hashlib.sha1(
        value.encode("utf-8", errors="ignore")
    ).hexdigest()[:8]
    return f"{slug[:max_len]}_{digest}" if slug else digest


@dataclass
class SeedParcel:
    owner: str
    owner_key: str
    tax_account: str
    parcel_id: str
    property_address: str
    published_amount: str


@dataclass
class SearchResult:
    searched_owner: str
    owner_key: str
    search_status: str
    result_owner: str = ""
    tax_account: str = ""
    parcel_id: str = ""
    property_address: str = ""
    property_type: str = ""
    amount_due: str = ""
    delinquent_years: str = ""
    raw_row_text: str = ""
    source_url: str = ""
    searched_at_utc: str = ""
    error: str = ""


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self._create()

    def _create(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS owners (
                owner_key TEXT PRIMARY KEY,
                searched_owner TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                searched_at_utc TEXT
            );

            CREATE TABLE IF NOT EXISTS seed_parcels (
                tax_account TEXT,
                parcel_id TEXT,
                owner_key TEXT NOT NULL,
                owner TEXT NOT NULL,
                property_address TEXT,
                published_amount TEXT,
                PRIMARY KEY (tax_account, parcel_id)
            );

            CREATE TABLE IF NOT EXISTS live_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_key TEXT NOT NULL,
                searched_owner TEXT NOT NULL,
                search_status TEXT NOT NULL,
                result_owner TEXT,
                tax_account TEXT,
                parcel_id TEXT,
                property_address TEXT,
                property_type TEXT,
                amount_due TEXT,
                delinquent_years TEXT,
                raw_row_text TEXT,
                source_url TEXT,
                searched_at_utc TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_live_owner
                ON live_results(owner_key);

            CREATE INDEX IF NOT EXISTS idx_live_tax_account
                ON live_results(tax_account);

            CREATE INDEX IF NOT EXISTS idx_live_parcel
                ON live_results(parcel_id);
            """
        )

        # Allow an existing database from an older collector version to upgrade.
        existing_columns = {
            row[1]
            for row in self.db.execute("PRAGMA table_info(live_results)")
        }
        if "property_type" not in existing_columns:
            self.db.execute(
                "ALTER TABLE live_results ADD COLUMN property_type TEXT"
            )

        self.db.commit()

    def load_seeds(self, seeds: Iterable[SeedParcel]) -> int:
        count = 0

        with self.db:
            for seed in seeds:
                self.db.execute(
                    """
                    INSERT OR IGNORE INTO owners(
                        owner_key,
                        searched_owner
                    )
                    VALUES (?, ?)
                    """,
                    (seed.owner_key, seed.owner),
                )

                self.db.execute(
                    """
                    INSERT OR REPLACE INTO seed_parcels(
                        tax_account,
                        parcel_id,
                        owner_key,
                        owner,
                        property_address,
                        published_amount
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        seed.tax_account,
                        seed.parcel_id,
                        seed.owner_key,
                        seed.owner,
                        seed.property_address,
                        seed.published_amount,
                    ),
                )
                count += 1

        return count

    def pending_owners(
        self,
        limit: int | None = None,
        retry_errors: bool = False,
    ) -> list[tuple[str, str]]:
        statuses = (
            "('pending', 'error')"
            if retry_errors
            else "('pending')"
        )

        sql = (
            "SELECT owner_key, searched_owner "
            f"FROM owners WHERE status IN {statuses} "
            "ORDER BY searched_owner"
        )
        params: tuple[object, ...] = ()

        if limit:
            sql += " LIMIT ?"
            params = (limit,)

        return list(self.db.execute(sql, params))

    def begin_attempt(self, owner_key: str) -> None:
        with self.db:
            self.db.execute(
                """
                UPDATE owners
                SET attempts = attempts + 1
                WHERE owner_key = ?
                """,
                (owner_key,),
            )
            self.db.execute(
                "DELETE FROM live_results WHERE owner_key = ?",
                (owner_key,),
            )

    def save_results(
        self,
        owner_key: str,
        searched_owner: str,
        results: list[SearchResult],
        status: str,
        error: str = "",
    ) -> None:
        now = utc_now()

        with self.db:
            for result in results:
                self.db.execute(
                    """
                    INSERT INTO live_results(
                        owner_key,
                        searched_owner,
                        search_status,
                        result_owner,
                        tax_account,
                        parcel_id,
                        property_address,
                        property_type,
                        amount_due,
                        delinquent_years,
                        raw_row_text,
                        source_url,
                        searched_at_utc,
                        error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        owner_key,
                        searched_owner,
                        result.search_status,
                        result.result_owner,
                        result.tax_account,
                        result.parcel_id,
                        result.property_address,
                        result.property_type,
                        result.amount_due,
                        result.delinquent_years,
                        result.raw_row_text,
                        result.source_url,
                        now,
                        result.error,
                    ),
                )

            self.db.execute(
                """
                UPDATE owners
                SET
                    status = ?,
                    last_error = ?,
                    searched_at_utc = ?
                WHERE owner_key = ?
                """,
                (
                    status,
                    error or None,
                    now,
                    owner_key,
                ),
            )

    def export_csvs(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)

        self._export_query(
            output_dir / "live_search_results.csv",
            """
            SELECT
                searched_owner,
                search_status,
                result_owner,
                tax_account AS live_pin,
                parcel_id AS live_parcel_id,
                property_address,
                property_type,
                amount_due,
                delinquent_years,
                raw_row_text,
                source_url,
                searched_at_utc,
                error
            FROM live_results
            ORDER BY searched_owner, property_address
            """,
        )

        self._export_query(
            output_dir / "owner_search_status.csv",
            """
            SELECT
                searched_owner,
                owner_key,
                status,
                attempts,
                last_error,
                searched_at_utc
            FROM owners
            ORDER BY searched_owner
            """,
        )

        # County live PIN equals the publication's Tax Account field.
        self._export_query(
            output_dir / "matched_2024_parcels.csv",
            """
            SELECT
                s.owner AS published_owner,
                s.tax_account AS published_tax_account,
                s.parcel_id AS published_parcel_id,
                s.property_address AS published_address,
                s.published_amount,
                COALESCE(r.search_status, o.status) AS search_status,
                r.result_owner,
                r.tax_account AS live_pin,
                r.parcel_id AS live_parcel_id,
                r.property_address AS live_address,
                r.property_type,
                r.amount_due,
                r.delinquent_years,
                COALESCE(r.searched_at_utc, o.searched_at_utc)
                    AS searched_at_utc,
                CASE
                    WHEN r.tax_account <> ''
                     AND ltrim(r.tax_account, '0')
                         = ltrim(s.tax_account, '0')
                    THEN 'tax_account_to_live_pin'

                    WHEN r.parcel_id <> ''
                     AND r.parcel_id = s.parcel_id
                    THEN 'parcel_id'

                    WHEN upper(r.property_address) <> ''
                     AND upper(r.property_address)
                         = upper(s.property_address)
                    THEN 'address'

                    WHEN o.status = 'not_found'
                    THEN 'owner_search_not_found'

                    WHEN o.status = 'error'
                    THEN 'owner_search_error'

                    WHEN o.status = 'pending'
                    THEN 'pending'

                    ELSE 'not_matched'
                END AS match_basis
            FROM seed_parcels s
            LEFT JOIN owners o
                ON o.owner_key = s.owner_key
            LEFT JOIN live_results r
                ON r.owner_key = s.owner_key
               AND (
                    (
                        r.tax_account <> ''
                        AND ltrim(r.tax_account, '0')
                            = ltrim(s.tax_account, '0')
                    )
                    OR (
                        r.parcel_id <> ''
                        AND r.parcel_id = s.parcel_id
                    )
                    OR (
                        upper(r.property_address) <> ''
                        AND upper(r.property_address)
                            = upper(s.property_address)
                    )
               )
            ORDER BY s.owner, s.property_address, s.tax_account
            """,
        )

    def _export_query(self, path: Path, query: str) -> None:
        cursor = self.db.execute(query)
        headers = [description[0] for description in cursor.description]

        with path.open(
            "w",
            newline="",
            encoding="utf-8-sig",
        ) as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(cursor)

    def close(self) -> None:
        self.db.close()


def load_seed_csv(path: Path) -> list[SeedParcel]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")

    seeds: list[SeedParcel] = []

    with path.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:
        reader = csv.DictReader(file)

        required = {
            "Owner",
            "Tax Account",
            "Parcel ID",
            "Property Address",
        }
        missing = required.difference(reader.fieldnames or [])

        if missing:
            raise ValueError(
                "Input CSV is missing required columns: "
                + ", ".join(sorted(missing))
            )

        for row in reader:
            owner = normalize_space(row.get("Owner", ""))
            if not owner:
                continue

            seeds.append(
                SeedParcel(
                    owner=owner,
                    owner_key=normalize_owner(owner),
                    tax_account=normalize_identifier(
                        row.get("Tax Account", ""),
                        width=8,
                    ),
                    parcel_id=normalize_identifier(
                        row.get("Parcel ID", "")
                    ),
                    property_address=normalize_space(
                        row.get("Property Address", "")
                    ),
                    published_amount=normalize_space(
                        row.get("2024 Amount Published", "")
                    ),
                )
            )

    return seeds


async def locate_search_input(page: Page):
    candidates = [
        page.get_by_placeholder(
            re.compile(
                r"^\s*name\s+or\s+partial\s+name\s*$",
                re.I,
            )
        ),
        page.get_by_label(
            re.compile(
                r"name\s+or\s+partial\s+name",
                re.I,
            )
        ),
        page.locator('input[placeholder*="partial name" i]'),
        page.locator('input[aria-label*="partial name" i]'),
    ]

    for locator in candidates:
        for index in range(await locator.count()):
            element = locator.nth(index)

            try:
                if (
                    await element.is_visible()
                    and await element.is_enabled()
                ):
                    return element
            except Exception:
                continue

    return None


async def click_intro_if_present(page: Page) -> None:
    if "delinquenciesintro.aspx" not in page.url.lower():
        return

    exact_candidates = [
        page.get_by_role(
            "button",
            name=re.compile(
                r"^(continue|proceed|accept|i accept|agree|"
                r"i agree|enter|view listings)$",
                re.I,
            ),
        ),
        page.get_by_role(
            "link",
            name=re.compile(
                r"^(continue|proceed|accept|i accept|agree|"
                r"i agree|enter|view listings)$",
                re.I,
            ),
        ),
        page.locator(
            'main input[type="submit"][value*="Continue" i], '
            'main input[type="submit"][value*="Accept" i], '
            'main input[type="submit"][value*="Agree" i], '
            'main input[type="submit"][value*="Proceed" i], '
            'main input[type="submit"][value*="Enter" i]'
        ),
        page.locator(
            '#main input[type="submit"][value*="Continue" i], '
            '#main input[type="submit"][value*="Accept" i], '
            '#main input[type="submit"][value*="Agree" i], '
            '#main input[type="submit"][value*="Proceed" i], '
            '#main input[type="submit"][value*="Enter" i]'
        ),
    ]

    for locator in exact_candidates:
        try:
            for index in range(await locator.count()):
                element = locator.nth(index)

                if not await element.is_visible():
                    continue

                await element.click()
                await page.wait_for_load_state(
                    "domcontentloaded",
                    timeout=20_000,
                )
                await page.wait_for_timeout(1_000)

                if await locate_search_input(page) is not None:
                    return
        except Exception:
            continue

    forms = page.locator(
        "main form, #main form, form[action*='delinquenc' i]"
    )

    for form_index in range(await forms.count()):
        form = forms.nth(form_index)

        try:
            if not await form.is_visible():
                continue

            controls = form.locator(
                "button, input[type='submit'], input[type='button']"
            )

            for control_index in range(await controls.count()):
                control = controls.nth(control_index)

                if not await control.is_visible():
                    continue

                label = normalize_space(
                    " ".join(
                        filter(
                            None,
                            [
                                await control.inner_text(),
                                await control.get_attribute("value"),
                                await control.get_attribute("aria-label"),
                                await control.get_attribute("title"),
                            ],
                        )
                    )
                )

                if re.search(
                    r"search site|mobile search|home|cancel|"
                    r"decline|disagree|back",
                    label,
                    re.I,
                ):
                    continue

                await control.click()
                await page.wait_for_load_state(
                    "domcontentloaded",
                    timeout=20_000,
                )
                await page.wait_for_timeout(1_000)

                if await locate_search_input(page) is not None:
                    return

        except Exception:
            continue


async def ensure_search_page(page: Page) -> None:
    await page.goto(
        INTRO_URL,
        wait_until="domcontentloaded",
        timeout=45_000,
    )
    await page.wait_for_timeout(1_000)

    if await locate_search_input(page) is not None:
        return

    await click_intro_if_present(page)

    if await locate_search_input(page) is not None:
        return

    await page.goto(
        START_URL,
        wait_until="domcontentloaded",
        timeout=45_000,
    )
    await page.wait_for_timeout(1_000)

    if await locate_search_input(page) is not None:
        return

    visible_controls: list[str] = []
    controls = page.locator(
        "button, input[type='submit'], input[type='button'], a"
    )

    for index in range(min(await controls.count(), 100)):
        element = controls.nth(index)

        try:
            if not await element.is_visible():
                continue

            label = normalize_space(
                " ".join(
                    filter(
                        None,
                        [
                            await element.inner_text(),
                            await element.get_attribute("value"),
                            await element.get_attribute("aria-label"),
                            await element.get_attribute("title"),
                            await element.get_attribute("href"),
                        ],
                    )
                )
            )

            if label:
                visible_controls.append(label[:200])
        except Exception:
            continue

    raise RuntimeError(
        "Search form not found. "
        f"Current URL: {page.url}. "
        f"Visible controls: {visible_controls[:30]}"
    )


async def submit_search(
    page: Page,
    owner: str,
) -> str:
    search_input = await locate_search_input(page)

    if search_input is None:
        raise RuntimeError(
            "Could not locate the delinquent-tax "
            "'Name or partial name' field"
        )

    await search_input.click()
    await search_input.fill("")
    await search_input.fill(owner)
    await search_input.dispatch_event("input")
    await search_input.dispatch_event("change")

    entered_value = normalize_space(
        await search_input.input_value()
    )
    normalized_owner = normalize_space(owner)

    if not entered_value:
        raise RuntimeError(
            f"Owner field remained blank for {owner!r}"
        )

    # County input currently truncates long owner names. Accept a valid prefix.
    if not normalized_owner.upper().startswith(
        entered_value.upper()
    ):
        raise RuntimeError(
            "Owner value was not entered correctly. "
            f"Expected prefix of={owner!r}; "
            f"actual={entered_value!r}"
        )

    search_form = search_input.locator(
        "xpath=ancestor::form[1]"
    )

    if await search_form.count() == 0:
        raise RuntimeError(
            "Could not locate the form containing the "
            "delinquent-tax owner field"
        )

    search_buttons = [
        search_form.get_by_role(
            "button",
            name=re.compile(r"^\s*search\s*$", re.I),
        ),
        search_form.locator(
            'input[type="submit"][value="SEARCH" i]'
        ),
        search_form.locator(
            'input[type="button"][value="SEARCH" i]'
        ),
        search_form.locator(
            'button:has-text("SEARCH")'
        ),
        search_form.locator(
            'a:has-text("SEARCH")'
        ),
    ]

    search_button = None

    for locator in search_buttons:
        for index in range(await locator.count()):
            element = locator.nth(index)

            try:
                if (
                    await element.is_visible()
                    and await element.is_enabled()
                ):
                    search_button = element
                    break
            except Exception:
                continue

        if search_button is not None:
            break

    if search_button is None:
        raise RuntimeError(
            "Could not locate the SEARCH control inside "
            "the delinquent-tax form"
        )

    before_url = page.url
    before_html = await page.content()

    await search_button.click()

    try:
        await page.wait_for_function(
            r"""
            oldHtml => {
                const body = document.body
                    ? document.body.innerText
                    : "";

                const htmlChanged =
                    document.documentElement.outerHTML !== oldHtml;

                const hasResultCount =
                    /\b\d+\s+RESULTS?\b/i.test(body);

                const hasPayTaxes =
                    /PAY\s+TAXES/i.test(body);

                const hasNoResults =
                    /NO\s+PROPERTIES\s+WERE\s+FOUND/i.test(body) ||
                    /NO\s+(MATCHING\s+)?RESULTS/i.test(body) ||
                    /NO\s+DELINQUENT/i.test(body) ||
                    /0\s+RESULTS?/i.test(body);

                return htmlChanged &&
                    (
                        hasResultCount ||
                        hasPayTaxes ||
                        hasNoResults
                    );
            }
            """,
            arg=before_html,
            timeout=30_000,
        )

    except PlaywrightTimeoutError as exc:
        body_text = normalize_space(
            await page.locator("body").inner_text()
        )

        current_value = ""

        try:
            current_input = await locate_search_input(page)
            if current_input is not None:
                current_value = await current_input.input_value()
        except Exception:
            pass

        raise RuntimeError(
            "The county search did not produce a confirmed "
            "results page. "
            f"Owner={owner!r}; "
            f"submitted_value={entered_value!r}; "
            f"input_value={current_value!r}; "
            f"before_url={before_url!r}; "
            f"after_url={page.url!r}; "
            f"body_preview={body_text[:500]!r}"
        ) from exc

    await page.wait_for_timeout(1_000)
    return entered_value


def build_result_pattern(
    searched_owner: str,
    submitted_owner: str,
) -> re.Pattern[str]:
    # Use either the full owner or county-truncated submitted value.
    alternatives = sorted(
        {
            normalize_space(searched_owner),
            normalize_space(submitted_owner),
        },
        key=len,
        reverse=True,
    )

    owner_pattern = "|".join(
        re.escape(value)
        for value in alternatives
        if value
    )

    return re.compile(
        rf"""
        (?P<owner>(?:{owner_pattern}))
        \s+
        (?P<address>.*?)
        \s+
        (?P<pin>\d{{8}})
        \s+
        (?P<property_type>
            Real|
            Personal|
            Mobile\s+Home
        )
        \s+
        PAY\s+TAXES
        """,
        re.I | re.S | re.X,
    )


async def parse_results_from_page(
    page: Page,
    searched_owner: str,
    submitted_owner: str,
    owner_key: str,
) -> list[SearchResult]:
    results: list[SearchResult] = []
    seen: set[tuple[str, str, str]] = set()
    visited_urls: set[str] = set()

    row_pattern = build_result_pattern(
        searched_owner,
        submitted_owner,
    )

    while True:
        await page.wait_for_timeout(750)

        current_url = page.url
        if current_url in visited_urls:
            break

        visited_urls.add(current_url)

        body_text = normalize_space(
            await page.locator("body").inner_text()
        )

        if NO_RESULTS_RE.search(body_text):
            break

        for match in row_pattern.finditer(body_text):
            result_owner = normalize_space(
                match.group("owner")
            )
            property_address = normalize_space(
                match.group("address")
            )
            live_pin = normalize_identifier(
                match.group("pin"),
                width=8,
            )
            property_type = normalize_space(
                match.group("property_type")
            )

            # Remove headers/pagination accidentally included before first row.
            property_address = re.sub(
                r"""
                ^.*?
                Owner\s+Address\s+PIN\s+Type
                \s*
                """,
                "",
                property_address,
                flags=re.I | re.S | re.X,
            )
            property_address = normalize_space(
                property_address
            )

            if not property_address:
                continue

            dedupe_key = (
                normalize_owner(result_owner),
                live_pin,
                property_address.upper(),
            )

            if dedupe_key in seen:
                continue

            seen.add(dedupe_key)

            results.append(
                SearchResult(
                    searched_owner=searched_owner,
                    owner_key=owner_key,
                    search_status="found",
                    result_owner=result_owner,
                    # The county labels this eight-digit identifier PIN.
                    # It matches the publication's Tax Account field.
                    tax_account=live_pin,
                    parcel_id="",
                    property_address=property_address,
                    property_type=property_type,
                    raw_row_text=(
                        f"{result_owner} | "
                        f"{property_address} | "
                        f"{live_pin} | "
                        f"{property_type} | "
                        "PAY TAXES"
                    ),
                    source_url=current_url,
                    searched_at_utc=utc_now(),
                )
            )

        next_candidates = [
            page.get_by_role(
                "link",
                name=re.compile(
                    r"^\s*Next Page\s*$",
                    re.I,
                ),
            ),
            page.locator(
                'a[aria-label="Next Page" i], '
                'a[title="Next Page" i]'
            ),
        ]

        next_button = None

        for locator in next_candidates:
            for index in range(await locator.count()):
                candidate = locator.nth(index)

                try:
                    if not await candidate.is_visible():
                        continue

                    href = (
                        await candidate.get_attribute("href")
                        or ""
                    )
                    aria_disabled = (
                        await candidate.get_attribute(
                            "aria-disabled"
                        )
                        or ""
                    ).lower()
                    classes = (
                        await candidate.get_attribute("class")
                        or ""
                    ).lower()

                    if (
                        not href
                        or href in {
                            "#",
                            "javascript:void(0)",
                        }
                        or aria_disabled == "true"
                        or "disabled" in classes
                    ):
                        continue

                    next_button = candidate
                    break
                except Exception:
                    continue

            if next_button is not None:
                break

        if next_button is None:
            break

        previous_url = page.url
        previous_body = body_text

        try:
            await next_button.click()

            await page.wait_for_function(
                r"""
                ([oldUrl, oldBody]) => {
                    const currentBody =
                        document.body
                            ? document.body.innerText
                            : "";

                    return window.location.href !== oldUrl
                        || currentBody !== oldBody;
                }
                """,
                arg=[previous_url, previous_body],
                timeout=30_000,
            )

            await page.wait_for_timeout(750)

        except Exception:
            break

    return results


async def return_to_search_page(page: Page) -> None:
    # Directly load a clean search page before each owner. This prevents a
    # prior result pagination URL from affecting the next query.
    await page.goto(
        START_URL,
        wait_until="domcontentloaded",
        timeout=45_000,
    )
    await page.wait_for_timeout(750)

    if await locate_search_input(page) is None:
        await ensure_search_page(page)


async def run(args: argparse.Namespace) -> int:
    seed_path = Path(args.input).resolve()
    output_dir = Path(args.output_dir).resolve()

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    debug_dir = output_dir / "debug"
    raw_dir = output_dir / "raw_html"

    debug_dir.mkdir(exist_ok=True)
    raw_dir.mkdir(exist_ok=True)

    seeds = load_seed_csv(seed_path)
    store = Store(
        output_dir / "sedgwick_delinquent.sqlite3"
    )
    store.load_seeds(seeds)

    owners = store.pending_owners(
        args.limit,
        args.retry_errors,
    )

    print(
        f"Loaded {len(seeds):,} seed parcels and "
        f"{len(set(seed.owner_key for seed in seeds)):,} "
        "unique owners"
    )
    print(f"Owners queued this run: {len(owners):,}")

    try:
        async with async_playwright() as playwright:
            browser: Browser = await playwright.chromium.launch(
                headless=not args.headed
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/150.0.0.0 Safari/537.36"
                ),
                viewport={
                    "width": 1440,
                    "height": 1000,
                },
            )

            page = await context.new_page()
            page.set_default_timeout(args.timeout_ms)

            await ensure_search_page(page)

            for index, (owner_key, owner) in enumerate(
                owners,
                start=1,
            ):
                store.begin_attempt(owner_key)
                slug = safe_slug(owner)

                try:
                    await return_to_search_page(page)

                    submitted_owner = await submit_search(
                        page,
                        owner,
                    )

                    results = await parse_results_from_page(
                        page,
                        owner,
                        submitted_owner,
                        owner_key,
                    )

                    # Save the final rendered page after pagination completes.
                    html = await page.content()
                    (
                        raw_dir / f"{slug}.html"
                    ).write_text(
                        html,
                        encoding="utf-8",
                    )

                    if results:
                        store.save_results(
                            owner_key,
                            owner,
                            results,
                            "found",
                        )

                        print(
                            f"[{index}/{len(owners)}] "
                            f"FOUND {owner!r}: "
                            f"{len(results)} row(s)"
                        )
                    else:
                        body_text = normalize_space(
                            await page.locator(
                                "body"
                            ).inner_text()
                        )

                        if NO_RESULTS_RE.search(body_text):
                            store.save_results(
                                owner_key,
                                owner,
                                [
                                    SearchResult(
                                        searched_owner=owner,
                                        owner_key=owner_key,
                                        search_status="not_found",
                                        source_url=page.url,
                                    )
                                ],
                                "not_found",
                            )

                            print(
                                f"[{index}/{len(owners)}] "
                                f"NOT FOUND {owner!r}"
                            )
                        else:
                            raise RuntimeError(
                                "Search completed but no rows "
                                "were parsed and the county did "
                                "not display an explicit "
                                "no-results message. "
                                f"Owner={owner!r}; "
                                f"submitted_owner="
                                f"{submitted_owner!r}; "
                                f"URL={page.url!r}; "
                                f"body_preview="
                                f"{body_text[:500]!r}"
                            )

                except Exception as exc:
                    error = (
                        f"{type(exc).__name__}: {exc}"
                    )

                    try:
                        await page.screenshot(
                            path=str(
                                debug_dir / f"{slug}.png"
                            ),
                            full_page=True,
                        )
                        (
                            debug_dir / f"{slug}.html"
                        ).write_text(
                            await page.content(),
                            encoding="utf-8",
                        )
                    except Exception:
                        pass

                    store.save_results(
                        owner_key,
                        owner,
                        [
                            SearchResult(
                                searched_owner=owner,
                                owner_key=owner_key,
                                search_status="error",
                                error=error,
                                source_url=page.url,
                            )
                        ],
                        "error",
                        error,
                    )

                    print(
                        f"[{index}/{len(owners)}] "
                        f"ERROR {owner!r}: {error}",
                        file=sys.stderr,
                    )

                    try:
                        await ensure_search_page(page)
                    except Exception:
                        pass

                if index % args.export_every == 0:
                    store.export_csvs(output_dir)

                await asyncio.sleep(
                    random.uniform(
                        args.min_delay,
                        args.max_delay,
                    )
                )

            store.export_csvs(output_dir)

            await context.storage_state(
                path=str(
                    output_dir / "browser_state.json"
                )
            )
            await browser.close()

    finally:
        store.close()

    print(f"Finished. Outputs: {output_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Query the Sedgwick County delinquent-tax "
            "owner search using the 2024 publication CSV."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the 2024 delinquent-list CSV",
    )
    parser.add_argument(
        "--output-dir",
        default="Output",
        help="Directory for SQLite, CSV, HTML, and debug files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only N pending unique owners",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show the Chromium browser window",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry owners currently marked error",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=2.5,
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=5.5,
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=30_000,
    )
    parser.add_argument(
        "--export-every",
        type=int,
        default=10,
    )

    return parser


if __name__ == "__main__":
    raise SystemExit(
        asyncio.run(
            run(
                build_parser().parse_args()
            )
        )
    )