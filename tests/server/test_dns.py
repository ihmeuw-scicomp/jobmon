# tests/test_dns_cache.py
import importlib
import pytest
from types import SimpleNamespace

# import the module under test
db = importlib.import_module("jobmon.server.web.db_admin")

# a tiny FakeAnswers that mimics dnspython’s Answer
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
    fake = FakeAnswers([ SimpleNamespace(address="1.2.3.4") ], ttl=120)
    monkeypatch.setattr(db.resolver, "resolve",
                        lambda hostname, qtype, lifetime: fake)
    # stub time to a fixed point
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)

    # first call must do a DNS lookup
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip  == "1.2.3.4"
    assert ttl == 120

    # check that cache now contains our entry
    assert host in db._DNS_CACHE

    # advance time by 30s → still cached, remaining TTL = 90
    monkeypatch.setattr(db.time, "time", lambda: t0 + 30)
    ip2, ttl2 = db.get_ip_with_ttl(host)
    assert ip2  == "1.2.3.4"
    assert ttl2 == 90

def test_cache_expiry_triggers_new_dns(monkeypatch):
    host = "db.example.local"

    # first resolve returns IP A, TTL 60
    ans1 = FakeAnswers([ SimpleNamespace(address="1.2.3.4") ], ttl=60)
    # second resolve returns IP B, TTL 30
    ans2 = FakeAnswers([ SimpleNamespace(address="5.6.7.8") ], ttl=30)

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
    assert ip1  == "1.2.3.4"
    assert ttl1 == 60

    # simulate expiration (t0 + 61 > t0+60)
    monkeypatch.setattr(db.time, "time", lambda: t0 + 61)
    ip2, ttl2 = db.get_ip_with_ttl(host)
    assert ip2  == "5.6.7.8"
    assert ttl2 == 30

def test_dns_failure_uses_fallback_and_short_ttl(monkeypatch):
    host = "db.example.local"

    # pre-seed cache with a “known-good” IP that’s already expired
    # so we go past the cache-hit check and into the retry/fallback logic
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("9.9.9.9", t0 - 1)  # expired by 1 second

    # make resolver.resolve always blow up
    monkeypatch.setattr(db.resolver, "resolve",
                        lambda hostname, qtype, lifetime: (_ for _ in ()).throw(Exception("boom")))

    ip, ttl = db.get_ip_with_ttl(host)
    assert ip  == "9.9.9.9"
    # on failure we hard-code a 30-second retry window
    assert ttl == 30