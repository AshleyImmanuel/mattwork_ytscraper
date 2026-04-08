import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import scraper


@pytest.fixture(autouse=True)
def clear_dns_safety_cache():
    scraper._DNS_SAFETY_CACHE.clear()
    yield
    scraper._DNS_SAFETY_CACHE.clear()


def test_is_safe_external_url_allows_public_http_urls(monkeypatch):
    resolved_hosts = []

    def fake_resolve(host):
        resolved_hosts.append(host)
        return {"93.184.216.34"}

    monkeypatch.setattr(scraper, "_resolve_host_addresses", fake_resolve)

    assert scraper._is_safe_external_url("https://example.com/path")
    assert scraper._is_safe_external_url("http://8.8.8.8/contact")
    assert resolved_hosts == ["example.com"]


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/about",
        "https://127.0.0.1/contact",
        "https://192.168.1.10",
        "https://10.0.0.5",
        "https://example.local",
        "https://example.internal",
        "https://example.lan",
    ],
)
def test_is_safe_external_url_blocks_local_private_and_internal_urls(url):
    assert not scraper._is_safe_external_url(url)


def test_is_safe_external_url_blocks_hostnames_resolving_to_private_ips(monkeypatch):
    monkeypatch.setattr(scraper, "_resolve_host_addresses", lambda host: {"10.0.0.12"})

    assert not scraper._is_safe_external_url("https://unsafe.example/contact")


def test_is_safe_external_url_blocks_when_dns_resolution_fails(monkeypatch):
    def fake_resolve(host):
        raise OSError("dns failure")

    monkeypatch.setattr(scraper, "_resolve_host_addresses", fake_resolve)

    assert not scraper._is_safe_external_url("https://broken.example/contact")


def test_is_safe_external_url_uses_dns_cache(monkeypatch):
    calls = {"count": 0}

    def fake_resolve(host):
        calls["count"] += 1
        return {"93.184.216.34"}

    monkeypatch.setattr(scraper, "_resolve_host_addresses", fake_resolve)

    assert scraper._is_safe_external_url("https://cached.example/one")
    assert scraper._is_safe_external_url("https://cached.example/two")
    assert calls["count"] == 1


def test_is_safe_external_url_blocks_when_dns_lookup_times_out(monkeypatch):
    class FakeFuture:
        def result(self, timeout=None):
            raise scraper.FutureTimeoutError()

        def cancel(self):
            return True

    class FakeResolver:
        def submit(self, fn, host):
            return FakeFuture()

    monkeypatch.setattr(scraper, "_DNS_RESOLVER", FakeResolver())

    assert not scraper._is_safe_external_url("https://slow.example/contact")


def test_extract_emails_from_text_filters_blacklist_entries():
    text = (
        "Reach us at noreply@youtube.com, support@google.com, and "
        "Team@Example.com. A real contact is Sales@Acme.io."
    )

    assert scraper._extract_emails_from_text(text) == [
        "Team@Example.com",
        "Sales@Acme.io",
    ]


def test_format_exception_handles_empty_messages_and_truncation():
    class CustomError(Exception):
        pass

    assert scraper._format_exception(CustomError()) == "CustomError"

    long_message = "x" * 100
    formatted = scraper._format_exception(CustomError(long_message), max_len=40)

    assert formatted == f"CustomError: {'x' * 37}..."


@pytest.mark.anyio
async def test_extract_email_from_channel_uses_links_when_about_fails_same_attempt(monkeypatch):
    calls = {"about": 0, "links": 0, "sleep": 0}

    async def fake_about(page, channel_url, on_log=None):
        calls["about"] += 1
        raise RuntimeError("about failed")

    async def fake_links(page, channel_url):
        calls["links"] += 1
        return "contact@site.test"

    async def fake_sleep(delay):
        calls["sleep"] += 1

    monkeypatch.setattr(scraper, "MAX_RETRIES", 3)
    monkeypatch.setattr(scraper, "RETRY_DELAY_MS", 1)
    monkeypatch.setattr(scraper, "_try_extract_from_about", fake_about)
    monkeypatch.setattr(scraper, "_try_extract_from_links", fake_links)
    monkeypatch.setattr(scraper.asyncio, "sleep", fake_sleep)

    result = await scraper._extract_email_from_channel(object(), "https://youtube.com/channel/abc")

    assert result == "contact@site.test"
    assert calls == {"about": 1, "links": 1, "sleep": 0}


@pytest.mark.anyio
async def test_extract_email_from_channel_returns_none_after_retry_failures(monkeypatch):
    calls = {"about": 0, "links": 0, "sleep": 0}

    async def fake_about(page, channel_url, on_log=None):
        calls["about"] += 1
        raise RuntimeError("about failed")

    async def fake_links(page, channel_url):
        calls["links"] += 1
        raise RuntimeError("links failed")

    async def fake_sleep(delay):
        calls["sleep"] += 1

    monkeypatch.setattr(scraper, "MAX_RETRIES", 2)
    monkeypatch.setattr(scraper, "RETRY_DELAY_MS", 1)
    monkeypatch.setattr(scraper, "_try_extract_from_about", fake_about)
    monkeypatch.setattr(scraper, "_try_extract_from_links", fake_links)
    monkeypatch.setattr(scraper.asyncio, "sleep", fake_sleep)

    result = await scraper._extract_email_from_channel(object(), "https://youtube.com/channel/xyz")

    assert result is None
    assert calls == {"about": 2, "links": 2, "sleep": 1}
