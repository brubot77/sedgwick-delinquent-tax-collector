from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup
from playwright.async_api import Browser, Page, TimeoutError as PlaywrightTimeoutError, async_playwright

START_URL = "https://ssc.sedgwickcounty.org/propertytax/delinquencies.aspx"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_owner(value: str) -> str:
    value = normalize_space(value).upper()
    value = value.replace("&AMP;", "&")
    value = re.sub(r"[^A-Z0-9&' -]", " ", value)
    return normalize_space(value)


def safe_slug(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    digest = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:8]
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
                amount_due TEXT,
                delinquent_years TEXT,
                raw_row_text TEXT,
                source_url TEXT,
                searched_at_utc TEXT,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_live_owner ON live_results(owner_key);
            CREATE INDEX IF NOT EXISTS idx_live_tax_account ON live_results(tax_account);
            CREATE INDEX IF NOT EXISTS idx_live_parcel ON live_results(parcel_id);
            """
        )
        self.db.commit()

    def load_seeds(self, seeds: Iterable[SeedParcel]) -> int:
        count = 0
        with self.db:
            for s in seeds:
                self.db.execute(
                    "INSERT OR IGNORE INTO owners(owner_key,searched_owner) VALUES (?,?)",
                    (s.owner_key, s.owner),
                )
                self.db.execute(
                    """INSERT OR REPLACE INTO seed_parcels
                    (tax_account,parcel_id,owner_key,owner,property_address,published_amount)
                    VALUES (?,?,?,?,?,?)""",
                    (s.tax_account, s.parcel_id, s.owner_key, s.owner, s.property_address, s.published_amount),
                )
                count += 1
        return count

    def pending_owners(self, limit: int | None = None, retry_errors: bool = False) -> list[tuple[str, str]]:
        statuses = "('pending')" if not retry_errors else "('pending','error')"
        sql = f"SELECT owner_key,searched_owner FROM owners WHERE status IN {statuses} ORDER BY searched_owner"
        params: tuple = ()
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        return list(self.db.execute(sql, params))

    def begin_attempt(self, owner_key: str) -> None:
        with self.db:
            self.db.execute("UPDATE owners SET attempts=attempts+1 WHERE owner_key=?", (owner_key,))
            self.db.execute("DELETE FROM live_results WHERE owner_key=?", (owner_key,))

    def save_results(self, owner_key: str, searched_owner: str, results: list[SearchResult], status: str, error: str = "") -> None:
        now = utc_now()
        with self.db:
            for r in results:
                self.db.execute(
                    """INSERT INTO live_results
                    (owner_key,searched_owner,search_status,result_owner,tax_account,parcel_id,
                     property_address,amount_due,delinquent_years,raw_row_text,source_url,searched_at_utc,error)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        owner_key, searched_owner, r.search_status, r.result_owner, r.tax_account,
                        r.parcel_id, r.property_address, r.amount_due, r.delinquent_years,
                        r.raw_row_text, r.source_url, now, r.error,
                    ),
                )
            self.db.execute(
                "UPDATE owners SET status=?,last_error=?,searched_at_utc=? WHERE owner_key=?",
                (status, error or None, now, owner_key),
            )

    def export_csvs(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        self._export_query(
            output_dir / "live_search_results.csv",
            "SELECT searched_owner,search_status,result_owner,tax_account,parcel_id,property_address,amount_due,delinquent_years,raw_row_text,source_url,searched_at_utc,error FROM live_results ORDER BY searched_owner,property_address",
        )
        self._export_query(
            output_dir / "owner_search_status.csv",
            "SELECT searched_owner,owner_key,status,attempts,last_error,searched_at_utc FROM owners ORDER BY searched_owner",
        )
        self._export_query(
            output_dir / "matched_2024_parcels.csv",
            """
            SELECT s.owner AS published_owner,s.tax_account AS published_tax_account,
                   s.parcel_id AS published_parcel_id,s.property_address AS published_address,
                   s.published_amount,r.search_status,r.result_owner,r.tax_account AS live_tax_account,
                   r.parcel_id AS live_parcel_id,r.property_address AS live_address,r.amount_due,
                   r.delinquent_years,r.searched_at_utc,
                   CASE
                     WHEN r.tax_account<>'' AND ltrim(r.tax_account,'0')=ltrim(s.tax_account,'0') THEN 'tax_account'
                     WHEN r.parcel_id<>'' AND r.parcel_id=s.parcel_id THEN 'parcel_id'
                     WHEN upper(r.property_address)<>'' AND upper(r.property_address)=upper(s.property_address) THEN 'address'
                     WHEN r.owner_key IS NOT NULL THEN 'owner_only_review'
                     ELSE 'not_found'
                   END AS match_basis
            FROM seed_parcels s
            LEFT JOIN live_results r ON r.owner_key=s.owner_key
              AND (
                (r.tax_account<>'' AND ltrim(r.tax_account,'0')=ltrim(s.tax_account,'0')) OR
                (r.parcel_id<>'' AND r.parcel_id=s.parcel_id) OR
                (upper(r.property_address)<>'' AND upper(r.property_address)=upper(s.property_address))
              )
            LEFT JOIN owners o ON o.owner_key=s.owner_key
            ORDER BY s.owner,s.property_address
            """,
        )

    def _export_query(self, path: Path, query: str) -> None:
        cur = self.db.execute(query)
        headers = [d[0] for d in cur.description]
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(headers)
            w.writerows(cur)


def load_seed_csv(path: Path) -> list[SeedParcel]:
    seeds: list[SeedParcel] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            owner = normalize_space(row.get("Owner", ""))
            if not owner:
                continue
            seeds.append(
                SeedParcel(
                    owner=owner,
                    owner_key=normalize_owner(owner),
                    tax_account=normalize_space(row.get("Tax Account", "")),
                    parcel_id=normalize_space(row.get("Parcel ID", "")),
                    property_address=normalize_space(row.get("Property Address", "")),
                    published_amount=normalize_space(row.get("2024 Amount Published", "")),
                )
            )
    return seeds


async def click_intro_if_present(page: Page) -> None:
    """
    Pass the Sedgwick County delinquent-tax introduction page.

    Do not use a generic input[type=submit] selector here because the page
    contains site-wide search/navigation controls that can redirect to the
    main county homepage.
    """
    if "delinquenciesintro.aspx" not in page.url.lower():
        return

    exact_candidates = [
        page.get_by_role(
            "button",
            name=re.compile(
                r"^(continue|proceed|accept|i accept|agree|i agree|enter|view listings)$",
                re.I,
            ),
        ),
        page.get_by_role(
            "link",
            name=re.compile(
                r"^(continue|proceed|accept|i accept|agree|i agree|enter|view listings)$",
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
                await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                await page.wait_for_timeout(1_000)

                if await locate_search_input(page) is not None:
                    return
        except Exception:
            continue

    # Inspect forms in the page content, excluding header and navigation forms.
    forms = page.locator("main form, #main form, form[action*='delinquenc' i]")

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

                # Explicitly avoid site navigation/search controls.
                if re.search(
                    r"search site|mobile search|home|cancel|decline|disagree|back",
                    label,
                    re.I,
                ):
                    continue

                await control.click()
                await page.wait_for_load_state("domcontentloaded", timeout=20_000)
                await page.wait_for_timeout(1_000)

                if await locate_search_input(page) is not None:
                    return

        except Exception:
            continue


async def locate_search_input(page: Page):
    """
    Locate only the delinquent-tax owner search field.

    Avoid generic text inputs because the county page includes site search,
    Google Translate, and other unrelated controls.
    """
    candidates = [
        page.get_by_placeholder(
            re.compile(r"^\s*name\s+or\s+partial\s+name\s*$", re.I)
        ),
        page.get_by_label(
            re.compile(r"name\s+or\s+partial\s+name", re.I)
        ),
        page.locator(
            'input[placeholder*="partial name" i]'
        ),
        page.locator(
            'input[aria-label*="partial name" i]'
        ),
    ]

    for locator in candidates:
        for index in range(await locator.count()):
            element = locator.nth(index)

            try:
                if await element.is_visible() and await element.is_enabled():
                    return element
            except Exception:
                continue

    return None


async def submit_search(page: Page, owner: str) -> None:
    search_input = await locate_search_input(page)

    if search_input is None:
        raise RuntimeError(
            "Could not locate the delinquent-tax 'Name or partial name' field"
        )

    # Clear and fill the exact delinquent-tax owner field.
    await search_input.click()
    await search_input.fill("")
    await search_input.fill(owner)
    await search_input.dispatch_event("input")
    await search_input.dispatch_event("change")

    entered_value = await search_input.input_value()

    if normalize_space(entered_value) != normalize_space(owner):
        raise RuntimeError(
            "Owner value was not entered correctly. "
            f"Expected={owner!r}; actual={entered_value!r}"
        )

    # Scope the SEARCH button to the same form/container as the owner field.
    search_form = search_input.locator("xpath=ancestor::form[1]")

    if await search_form.count() == 0:
        raise RuntimeError(
            "Could not locate the form containing the delinquent-tax owner field"
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
                if await element.is_visible() and await element.is_enabled():
                    search_button = element
                    break
            except Exception:
                continue

        if search_button is not None:
            break

    if search_button is None:
        raise RuntimeError(
            "Could not locate the SEARCH control inside the delinquent-tax form"
        )

    before_url = page.url
    before_html = await page.content()

    await search_button.click()

    # Do not rely solely on networkidle. The county may update the results
    # asynchronously while leaving the URL unchanged.
    try:
        await page.wait_for_function(
            """
            ([owner, oldHtml]) => {
                const body = document.body
                    ? document.body.innerText
                    : "";

                const htmlChanged =
                    document.documentElement.outerHTML !== oldHtml;

                const hasResultCount =
                    /\\b\\d+\\s+RESULTS?\\b/i.test(body);

                const hasPayTaxes =
                    /PAY\\s+TAXES/i.test(body);

                const hasNoResults =
                    /NO\\s+(MATCHING\\s+)?RESULTS/i.test(body) ||
                    /NO\\s+DELINQUENT/i.test(body) ||
                    /0\\s+RESULTS?/i.test(body);

                return htmlChanged &&
                    (hasResultCount || hasPayTaxes || hasNoResults);
            }
            """,
            [owner, before_html],
            timeout=30_000,
        )
    except PlaywrightTimeoutError:
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
            "The county search did not produce a confirmed results page. "
            f"Owner={owner!r}; "
            f"input_value={current_value!r}; "
            f"before_url={before_url!r}; "
            f"after_url={page.url!r}; "
            f"body_preview={body_text[:500]!r}"
        )

    await page.wait_for_timeout(1_000)



def find_value_by_header(headers: list[str], cells: list[str], patterns: list[str]) -> str:
    for i, h in enumerate(headers):
        if any(re.search(p, h, re.I) for p in patterns) and i < len(cells):
            return cells[i]
    return ""


def parse_results(html: str, searched_owner: str, owner_key: str, source_url: str) -> list[SearchResult]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[SearchResult] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        headers = [normalize_space(c.get_text(" ", strip=True)) for c in header_cells]
        joined_headers = " | ".join(headers).lower()
        if not any(k in joined_headers for k in ("owner", "address", "parcel", "pin", "account", "amount", "tax")):
            continue

        for row in rows[1:]:
            cells = [normalize_space(c.get_text(" ", strip=True)) for c in row.find_all(["td", "th"])]
            if not cells or not any(cells):
                continue
            raw = " | ".join(cells)
            # Skip pager, total, and instructional rows.
            if re.search(r"^(page|total|no records|no delinquent)", raw, re.I):
                continue
            result = SearchResult(
                searched_owner=searched_owner,
                owner_key=owner_key,
                search_status="found",
                result_owner=find_value_by_header(headers, cells, [r"owner", r"name"]),
                tax_account=find_value_by_header(headers, cells, [r"tax.*account", r"account", r"pin"]),
                parcel_id=find_value_by_header(headers, cells, [r"parcel"]),
                property_address=find_value_by_header(headers, cells, [r"address", r"situs", r"property"]),
                amount_due=find_value_by_header(headers, cells, [r"amount", r"balance", r"total.*due"]),
                delinquent_years=find_value_by_header(headers, cells, [r"year"]),
                raw_row_text=raw,
                source_url=source_url,
                searched_at_utc=utc_now(),
            )
            results.append(result)

    # Fallback: capture likely result rows even if headers are unusual.
    if not results:
        page_text = normalize_space(soup.get_text(" ", strip=True))
        if re.search(r"no (matching|delinquent|records)|0 records", page_text, re.I):
            return []
        for tr in soup.find_all("tr"):
            cells = [normalize_space(c.get_text(" ", strip=True)) for c in tr.find_all("td")]
            raw = " | ".join(cells)
            if len(cells) >= 3 and re.search(r"\$\s*[\d,]+\.\d{2}", raw):
                results.append(
                    SearchResult(
                        searched_owner=searched_owner,
                        owner_key=owner_key,
                        search_status="found_unstructured",
                        raw_row_text=raw,
                        source_url=source_url,
                        searched_at_utc=utc_now(),
                    )
                )
    return results

async def parse_results_from_page(
    page: Page,
    searched_owner: str,
    owner_key: str,
) -> list[SearchResult]:
    """
    Parse Sedgwick County's rendered result grid.

    The current county page is not a conventional HTML table. Each result
    contains an owner, property-address link, PIN link, type, and PAY TAXES
    button. Parse the rendered browser DOM rather than relying on <table>/<tr>.
    """
    results: list[SearchResult] = []
    seen: set[tuple[str, str, str]] = set()

    while True:
        await page.wait_for_timeout(750)

        # Each actual property row contains a PAY TAXES control.
        pay_controls = page.get_by_text(
            re.compile(r"^\s*PAY\s+TAXES\s*$", re.I),
            exact=False,
        )

        control_count = await pay_controls.count()

        for index in range(control_count):
            pay_control = pay_controls.nth(index)

            try:
                if not await pay_control.is_visible():
                    continue

                # Walk upward until we find the smallest container that contains
                # the owner, address, PIN and property type for this result.
                row = pay_control.locator(
                    "xpath=ancestor::*["
                    ".//a and "
                    "contains(translate(normalize-space(.), "
                    "'abcdefghijklmnopqrstuvwxyz', "
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'PAY TAXES')"
                    "][1]"
                )

                if await row.count() == 0:
                    continue

                row = row.first
                raw_text = normalize_space(await row.inner_text())

                if not raw_text:
                    continue

                links = row.locator("a")
                link_values: list[tuple[str, str]] = []

                for link_index in range(await links.count()):
                    link = links.nth(link_index)

                    try:
                        text = normalize_space(await link.inner_text())
                        href = await link.get_attribute("href") or ""

                        if text:
                            link_values.append((text, href))
                    except Exception:
                        continue

                property_address = ""
                parcel_id = ""
                result_owner = ""
                property_type = ""

                # PIN appears as an 8-digit linked value in the current page.
                for text, _href in link_values:
                    compact = re.sub(r"\D", "", text)

                    if len(compact) == 8:
                        parcel_id = compact
                        break

                # The property address is another link in the same row.
                for text, _href in link_values:
                    if text.upper() in {"PAY TAXES", "HOME"}:
                        continue

                    if re.fullmatch(r"\d{8}", re.sub(r"\D", "", text)):
                        continue

                    if re.search(
                        r"\b(?:AVE|AVENUE|ST|STREET|RD|ROAD|LN|LANE|DR|DRIVE|"
                        r"CT|COURT|BLVD|BOULEVARD|PL|PLACE|PKWY|HWY|CIR|WAY|"
                        r"TER|TRAIL|WICHITA|HAYSVILLE|DERBY|MAIZE|GODDARD|"
                        r"VALLEY CENTER|KECHI|CLEARWATER|MULVANE|CHENEY)\b",
                        text,
                        re.I,
                    ):
                        property_address = text
                        break

                # Remove known values from the full row to identify owner/type.
                text_lines = [
                    normalize_space(line)
                    for line in (await row.inner_text()).splitlines()
                    if normalize_space(line)
                ]

                for line in text_lines:
                    if re.fullmatch(r"PAY\s+TAXES", line, re.I):
                        continue

                    if parcel_id and re.sub(r"\D", "", line) == parcel_id:
                        continue

                    if property_address and line == property_address:
                        continue

                    if re.fullmatch(r"REAL|PERSONAL|MOBILE HOME", line, re.I):
                        property_type = line
                        continue

                    if not result_owner:
                        result_owner = line

                key = (
                    normalize_owner(result_owner),
                    parcel_id,
                    normalize_space(property_address).upper(),
                )

                if key in seen:
                    continue

                seen.add(key)

                results.append(
                    SearchResult(
                        searched_owner=searched_owner,
                        owner_key=owner_key,
                        search_status="found",
                        result_owner=result_owner,
                        parcel_id=parcel_id,
                        property_address=property_address,
                        raw_row_text=raw_text,
                        source_url=page.url,
                        searched_at_utc=utc_now(),
                    )
                )

            except Exception:
                continue

        # Locate the enabled next-page arrow. The current page shows single and
        # double right arrows after the numbered page controls.
        next_candidates = [
            page.get_by_role(
                "link",
                name=re.compile(r"^(next|>)$", re.I),
            ),
            page.get_by_role(
                "button",
                name=re.compile(r"^(next|>)$", re.I),
            ),
            page.locator(
                "a[aria-label*='next' i], "
                "button[aria-label*='next' i], "
                "a[title*='next' i], "
                "button[title*='next' i]"
            ),
        ]

        next_control = None

        for candidate in next_candidates:
            for candidate_index in range(await candidate.count()):
                element = candidate.nth(candidate_index)

                try:
                    if not await element.is_visible():
                        continue

                    disabled = (
                        await element.get_attribute("disabled") is not None
                        or (await element.get_attribute("aria-disabled") or "").lower()
                        == "true"
                        or "disabled"
                        in (await element.get_attribute("class") or "").lower()
                    )

                    if not disabled:
                        next_control = element
                        break
                except Exception:
                    continue

            if next_control is not None:
                break

        if next_control is None:
            break

        old_first_pin = results[-1].parcel_id if results else ""

        try:
            await next_control.click()
            await page.wait_for_load_state("networkidle", timeout=30_000)
            await page.wait_for_timeout(750)

            # Prevent an endless loop if the page control did not advance.
            current_text = normalize_space(await page.locator("body").inner_text())

            if old_first_pin and current_text.count(old_first_pin) > 0:
                # It may still be present elsewhere, so only stop when no new
                # PAY TAXES result can be identified on another pass.
                current_count = len(results)
                await page.wait_for_timeout(300)

                if len(results) == current_count:
                    break

        except Exception:
            break

    return results

async def ensure_search_page(page: Page) -> None:
    intro_url = (
        "https://ssc.sedgwickcounty.org/propertytax/"
        "delinquenciesintro.aspx?"
        "returnURL=%2Fpropertytax%2Fdelinquencies.aspx"
    )

    await page.goto(
        intro_url,
        wait_until="domcontentloaded",
        timeout=45_000,
    )
    await page.wait_for_timeout(1_000)

    # The county may occasionally load the search page directly.
    if await locate_search_input(page) is not None:
        return

    await click_intro_if_present(page)

    if await locate_search_input(page) is not None:
        return

    # One direct navigation attempt after the introduction/session step.
    await page.goto(
        START_URL,
        wait_until="domcontentloaded",
        timeout=45_000,
    )
    await page.wait_for_timeout(1_000)

    if await locate_search_input(page) is not None:
        return

    # Produce useful diagnostic information.
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

async def run(args: argparse.Namespace) -> int:
    seed_path = Path(args.input).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = output_dir / "debug"
    raw_dir = output_dir / "raw_html"
    debug_dir.mkdir(exist_ok=True)
    raw_dir.mkdir(exist_ok=True)

    seeds = load_seed_csv(seed_path)
    store = Store(output_dir / "sedgwick_delinquent.sqlite3")
    store.load_seeds(seeds)
    owners = store.pending_owners(args.limit, args.retry_errors)
    print(f"Loaded {len(seeds):,} seed parcels and {len(set(s.owner_key for s in seeds)):,} unique owners")
    print(f"Owners queued this run: {len(owners):,}")

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=not args.headed)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/150 Safari/537.36",
            viewport={"width": 1440, "height": 1000},
        )
        page = await context.new_page()
        page.set_default_timeout(args.timeout_ms)
        await ensure_search_page(page)

        for index, (owner_key, owner) in enumerate(owners, 1):
            store.begin_attempt(owner_key)
            slug = safe_slug(owner)
            try:
                # Return to a clean search form before each owner.
                if await locate_search_input(page) is None:
                    await ensure_search_page(page)
                await submit_search(page, owner)
                html = await page.content()
                (raw_dir / f"{slug}.html").write_text(html, encoding="utf-8")

                results = await parse_results_from_page(
                    page,
                    owner,
                    owner_key,
                )

                if results:
                    store.save_results(owner_key, owner, results, "found")
                    print(
                        f"[{index}/{len(owners)}] "
                        f"FOUND {owner!r}: {len(results)} row(s)"
                    )
                else:
                    body_text = normalize_space(
                        await page.locator("body").inner_text()
                    )

                    explicit_no_results = bool(
                        re.search(
                            r"\b(?:0\s+RESULTS?|"
                            r"NO\s+(?:MATCHING\s+)?RESULTS?|"
                            r"NO\s+DELINQUENT(?:\s+PROPERTIES)?)\b",
                            body_text,
                            re.I,
                        )
                    )

                    if explicit_no_results:
                        store.save_results(
                            owner_key,
                            owner,
                            [
                                SearchResult(
                                    owner,
                                    owner_key,
                                    "not_found",
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
                            "Search completed but no rows were parsed and the county "
                            "did not display an explicit no-results message. "
                            f"Owner={owner!r}; URL={page.url!r}; "
                            f"body_preview={body_text[:500]!r}"
                        )
                    
            except (PlaywrightTimeoutError, Exception) as exc:
                error = f"{type(exc).__name__}: {exc}"
                try:
                    await page.screenshot(path=str(debug_dir / f"{slug}.png"), full_page=True)
                    (debug_dir / f"{slug}.html").write_text(await page.content(), encoding="utf-8")
                except Exception:
                    pass
                store.save_results(owner_key, owner, [SearchResult(owner, owner_key, "error", error=error, source_url=page.url)], "error", error)
                print(f"[{index}/{len(owners)}] ERROR {owner!r}: {error}", file=sys.stderr)
                try:
                    await ensure_search_page(page)
                except Exception:
                    pass

            if index % args.export_every == 0:
                store.export_csvs(output_dir)
            await asyncio.sleep(random.uniform(args.min_delay, args.max_delay))

        store.export_csvs(output_dir)
        await context.storage_state(path=str(output_dir / "browser_state.json"))
        await browser.close()
    print(f"Finished. Outputs: {output_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Query Sedgwick County delinquent-tax search from a 2024 publication CSV.")
    p.add_argument("--input", required=True, help="Path to 2024 delinquent-list CSV")
    p.add_argument("--output-dir", default="sedgwick_live_output")
    p.add_argument("--limit", type=int, default=None, help="Process only N unique owners for testing")
    p.add_argument("--headed", action="store_true", help="Show browser window (recommended for first test)")
    p.add_argument("--retry-errors", action="store_true")
    p.add_argument("--min-delay", type=float, default=2.5)
    p.add_argument("--max-delay", type=float, default=5.5)
    p.add_argument("--timeout-ms", type=int, default=30000)
    p.add_argument("--export-every", type=int, default=10)
    return p


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(build_parser().parse_args())))
