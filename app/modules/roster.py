from __future__ import annotations

import os
from urllib.parse import quote


def get_beta_roster_site_url() -> str:
    return (os.getenv(
        'BETA_ROSTER_SITE_URL') or 'https://starcrafttmgbeta.web.app/').strip() or 'https://starcrafttmgbeta.web.app/'


def get_beta_roster_pdf_url_template() -> str:
    return (os.getenv('BETA_ROSTER_PDF_URL_TEMPLATE') or '').strip()

def _clean_roster_id(value: str | None) -> str:
    raw_value = str(value or '').strip()
    safe_chars = []
    for char in raw_value:
        if char.isalnum() or char in ('-', '_'):
            safe_chars.append(char)
    return ''.join(safe_chars)[:80]

def _build_beta_roster_pdf_url(roster_id: str) -> str | None:
    clean_roster_id = _clean_roster_id(roster_id)
    if not clean_roster_id:
        return None
    template = get_beta_roster_pdf_url_template()
    if not template:
        return None
    return template.format(roster_id=quote(clean_roster_id, safe=''))

def _download_roster_pdf_via_browser(roster_id: str) -> tuple[bytes, str]:
    clean_roster_id = _clean_roster_id(roster_id)
    if not clean_roster_id:
        raise ValueError('Roster ID is empty.')

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            'Playwright is not installed. Run: pip install playwright and then playwright install chromium'
        ) from exc

    beta_url = get_beta_roster_site_url()
    load_selectors = [
        'button:has(i.fa-solid.fa-cloud-arrow-down)',
        'button:has(i.fa-cloud-arrow-down)',
        'button:has(.fa-cloud-arrow-down)',
    ]
    pdf_selectors = [
        'button.ab-add-btn:has-text("PDF")',
        'button:has-text("PDF")',
        '[role="button"]:has-text("PDF")',
    ]

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            page.goto(beta_url, wait_until='domcontentloaded', timeout=45000)
            seed_input = page.locator('#ab-seed-input')
            seed_input.wait_for(state='visible', timeout=20000)
            seed_input.fill(clean_roster_id)

            load_clicked = False
            for selector in load_selectors:
                locator = page.locator(selector)
                if locator.count() > 0:
                    locator.first.click(timeout=10000)
                    load_clicked = True
                    break
            if not load_clicked:
                try:
                    seed_input.press('Enter')
                except Exception:
                    pass

            page.wait_for_timeout(1200)

            pdf_button = None
            for selector in pdf_selectors:
                locator = page.locator(selector)
                if locator.count() > 0:
                    pdf_button = locator.first
                    break
            if pdf_button is None:
                raise RuntimeError('Could not find the PDF button on the beta roster site.')

            try:
                pdf_button.wait_for(state='visible', timeout=20000)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError('The PDF button did not become available in time.') from exc

            with page.expect_download(timeout=30000) as download_info:
                pdf_button.click(timeout=10000)
            download = download_info.value
            pdf_bytes = download.path().read_bytes()
            filename = download.suggested_filename or f'roster-{clean_roster_id}.pdf'
            return pdf_bytes, filename
        finally:
            context.close()
            browser.close()
