# tests/test_dns_cache.py
import importlib
from types import SimpleNamespace

import pytest

# import the module under test
db = importlib.import_module("jobmon.server.web.db.dns")


# a tiny FakeAnswers that mimics dnspython's Answer
class FakeAnswers(list):
    def __init__(self, records, ttl):
        super().__init__(records)
        # dnspython puts TTL on the rrset
        self.rrset = SimpleNamespace(ttl=ttl)


@pytest.fixture(autouse=True)
def clear_cache():
    # clear before and after each test
    db._DNS_CACHE.clear()
    yield
    db._DNS_CACHE.clear()


def test_initial_resolution_and_caching(monkeypatch):
    host = "db.example.local"
    # stub resolver.resolve → one record, TTL=120
    fake = FakeAnswers([SimpleNamespace(address="1.2.3.4")], ttl=120)
    monkeypatch.setattr(db.resolver, "resolve", lambda hostname, qtype, lifetime: fake)
    # stub time to a fixed point
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)

    # first call must do a DNS lookup
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "1.2.3.4"
    assert ttl == 120

    # check that cache now contains our entry
    assert host in db._DNS_CACHE

    # advance time by 30s → still cached, remaining TTL = 90
    monkeypatch.setattr(db.time, "time", lambda: t0 + 30)
    ip2, ttl2 = db.get_ip_with_ttl(host)
    assert ip2 == "1.2.3.4"
    assert ttl2 == 90


def test_cache_expiry_triggers_new_dns(monkeypatch):
    host = "db.example.local"

    # first resolve returns IP A, TTL 60
    ans1 = FakeAnswers([SimpleNamespace(address="1.2.3.4")], ttl=60)
    # second resolve returns IP B, TTL 30
    ans2 = FakeAnswers([SimpleNamespace(address="5.6.7.8")], ttl=30)

    # count how many times resolver.resolve is called
    calls = {"n": 0}

    def fake_resolve(hostname, qtype, lifetime):
        calls["n"] += 1
        return ans1 if calls["n"] == 1 else ans2

    monkeypatch.setattr(db.resolver, "resolve", fake_resolve)
    t0 = 2_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)

    # initial lookup
    ip1, ttl1 = db.get_ip_with_ttl(host)
    assert ip1 == "1.2.3.4"
    assert ttl1 == 60

    # simulate expiration (t0 + 61 > t0+60)
    monkeypatch.setattr(db.time, "time", lambda: t0 + 61)
    ip2, ttl2 = db.get_ip_with_ttl(host)
    assert ip2 == "5.6.7.8"
    assert ttl2 == 30


def test_dns_failure_uses_fallback_and_short_ttl(monkeypatch):
    host = "db.example.local"

    # pre-seed cache with a "known-good" IP that's already expired
    # so we go past the cache-hit check and into the retry/fallback logic
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("9.9.9.9", t0 - 1)  # expired by 1 second

    # make resolver.resolve always blow up
    monkeypatch.setattr(
        db.resolver,
        "resolve",
        lambda hostname, qtype, lifetime: (_ for _ in ()).throw(Exception("boom")),
    )

    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "9.9.9.9"
    # on failure we hard-code a 30-second retry window
    assert ttl == 30


def test_dns_failure_without_cached_ip_raises_exception(monkeypatch):
    """Test that DNS failure without a cached IP raises the original exception"""
    host = "new.example.local"
    
    # No pre-seeded cache - fresh lookup should fail
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    
    # make resolver.resolve always blow up
    monkeypatch.setattr(
        db.resolver,
        "resolve",
        lambda hostname, qtype, lifetime: (_ for _ in ()).throw(Exception("DNS totally broken")),
    )
    
    # Should raise the original exception since there's no cached IP to fall back to
    with pytest.raises(Exception, match="DNS totally broken"):
        db.get_ip_with_ttl(host)


def test_dns_failure_with_nxdomain_fallback(monkeypatch):
    """Test specific NXDOMAIN handling that was causing production issues"""
    from dns.resolver import NXDOMAIN
    
    host = "mock-azure-mysql.mysql.database.azure.com"
    
    # pre-seed cache with valid IP (simulating previous successful resolution)
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("10.4.34.197", t0 - 5)  # expired 5 seconds ago
    
    # simulate NXDOMAIN error (the specific error from production)
    def nxdomain_error(hostname, qtype, lifetime):
        raise NXDOMAIN(qnames=[hostname], responses={})
    
    monkeypatch.setattr(db.resolver, "resolve", nxdomain_error)
    
    # Should use cached IP with grace period, not crash
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "10.4.34.197"
    assert ttl == 30  # grace period


def test_cached_ip_variable_scope_bug_regression(monkeypatch):
    """Regression test for the variable scope bug where 'ip' vs 'cached_ip' was confused"""
    host = "scope-bug.example.local"
    
    # Set up initial cache entry
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("192.168.1.100", t0 - 1)  # expired
    
    # Track what variables are defined when exception handler runs
    scope_check = {}
    
    def failing_resolve(hostname, qtype, lifetime):
        # This simulates the original bug where 'ip' would be undefined
        # when the exception is raised, but 'cached_ip' should still be available
        scope_check['before_exception'] = True
        raise Exception("DNS failed")
    
    monkeypatch.setattr(db.resolver, "resolve", failing_resolve)
    
    # Should successfully use cached IP despite DNS failure
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "192.168.1.100"
    assert ttl == 30
    assert scope_check['before_exception']  # Verify the exception path was taken


def test_grace_period_logging(monkeypatch, caplog):
    """Test that fallback logging works correctly"""
    import logging
    caplog.set_level(logging.INFO)
    
    host = "log-test.example.local"
    
    # pre-seed cache with expired IP
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("1.1.1.1", t0 - 10)  # expired
    
    # make DNS fail
    monkeypatch.setattr(
        db.resolver,
        "resolve",
        lambda hostname, qtype, lifetime: (_ for _ in ()).throw(Exception("Network error")),
    )
    
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "1.1.1.1"
    assert ttl == 30
    
    # Check that informative log message was generated
    assert "Using cached IP 1.1.1.1 for log-test.example.local with 30s grace period" in caplog.text
