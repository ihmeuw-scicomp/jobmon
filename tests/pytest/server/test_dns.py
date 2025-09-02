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
    db.clear_dns_cache()  # Use the proper clear function
    yield
    db.clear_dns_cache()


def test_initial_resolution_and_caching(monkeypatch):
    host = "db.example.local"
    # stub resolver.resolve → one record, TTL=120
    fake = FakeAnswers([SimpleNamespace(address="1.2.3.4")], ttl=120)

    # Patch Resolver.resolve (instance method) to return our fake answers
    def fake_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        return fake

    monkeypatch.setattr(db.resolver.Resolver, "resolve", fake_resolve)
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

    def fake_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        calls["n"] += 1
        return ans1 if calls["n"] == 1 else ans2

    monkeypatch.setattr(db.resolver.Resolver, "resolve", fake_resolve)
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
    db._DNS_CACHE[host] = ("9.9.9.9", t0 - 1, 0)  # expired by 1 second, no failures

    # make resolver.resolve always blow up
    monkeypatch.setattr(
        db.resolver.Resolver,
        "resolve",
        lambda self, hostname, qtype, lifetime=None, search=True, **kwargs: (
            _ for _ in ()
        ).throw(Exception("boom")),
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
        db.resolver.Resolver,
        "resolve",
        lambda self, hostname, qtype, lifetime=None, search=True, **kwargs: (
            _ for _ in ()
        ).throw(Exception("DNS totally broken")),
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
    db._DNS_CACHE[host] = ("10.4.34.197", t0 - 5, 0)  # expired 5 seconds ago, no failures

    # simulate NXDOMAIN error (the specific error from production)
    def nxdomain_error(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        raise NXDOMAIN(qnames=[hostname], responses={})

    monkeypatch.setattr(db.resolver.Resolver, "resolve", nxdomain_error)

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
    db._DNS_CACHE[host] = ("192.168.1.100", t0 - 1, 0)  # expired, no failures

    # Track what variables are defined when exception handler runs
    scope_check = {}

    def failing_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        # This simulates the original bug where 'ip' would be undefined
        # when the exception is raised, but 'cached_ip' should still be available
        scope_check["before_exception"] = True
        raise Exception("DNS failed")

    monkeypatch.setattr(db.resolver.Resolver, "resolve", failing_resolve)

    # Should successfully use cached IP despite DNS failure
    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "192.168.1.100"
    assert ttl == 30
    assert scope_check["before_exception"]  # Verify the exception path was taken


def test_max_retries_configuration(monkeypatch):
    """Test that max_retries parameter is respected"""
    host = "max-retries-test.example.local"
    
    # Track retry attempts
    attempts = {"count": 0}
    
    def always_failing_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        attempts["count"] += 1
        raise Exception(f"DNS failure #{attempts['count']}")
    
    monkeypatch.setattr(db.resolver.Resolver, "resolve", always_failing_resolve)
    
    # Should try exactly max_retries times, then give up
    with pytest.raises(Exception, match="DNS failure #2"):
        db.get_ip_with_ttl(host, dns_max_retries=2)
    
    assert attempts["count"] == 2


def test_grace_period_logging(monkeypatch, json_log_file):
    """Test that fallback logging works correctly"""
    import json

    # Set up JSON logging for the DNS module
    log_file_path = json_log_file(
        loggers={"jobmon.server.web.db.dns": "INFO"}, filename_suffix="dns_test"
    )

    host = "log-test.example.local"

    # pre-seed cache with expired IP
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    db._DNS_CACHE[host] = ("1.1.1.1", t0 - 10, 0)  # expired, no failures

    # make DNS fail
    monkeypatch.setattr(
        db.resolver.Resolver,
        "resolve",
        lambda self, hostname, qtype, lifetime=None, search=True, **kwargs: (
            _ for _ in ()
        ).throw(Exception("Network error")),
    )

    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "1.1.1.1"
    assert ttl == 30

    # Check that informative log message was generated by reading the log file
    expected_message = (
        "Using cached IP 1.1.1.1 for log-test.example.local with 30s grace period"
    )
    found_message = False

    with open(log_file_path, "r") as log_file:
        for line in log_file:
            if line.strip():
                try:
                    log_dict = json.loads(line.strip())
                    if "event" in log_dict and expected_message in log_dict["event"]:
                        found_message = True
                        break
                except json.JSONDecodeError:
                    # Skip non-JSON lines
                    continue

    assert (
        found_message
    ), f"Expected message '{expected_message}' not found in log file. File contents: {log_file_path.read_text() if log_file_path.exists() else 'File not found'}"


def test_resolver_called_with_disabled_search_and_default_timeout(monkeypatch):
    host = "db.search.test"

    captured = {"search": None, "lifetime": None}

    def fake_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        captured["search"] = search
        captured["lifetime"] = lifetime
        # Return a valid answer with TTL
        return FakeAnswers([SimpleNamespace(address="8.8.8.8")], ttl=120)

    monkeypatch.setattr(db.resolver.Resolver, "resolve", fake_resolve)

    ip, ttl = db.get_ip_with_ttl(host)
    assert ip == "8.8.8.8"
    assert ttl == 120
    # Ensure search domains are disabled and default timeout is passed through
    assert captured["search"] is False
    assert captured["lifetime"] == 12


def test_retry_logic_with_eventual_success(monkeypatch):
    """Test that retry logic works and eventually succeeds"""
    host = "retry-test.example.local"
    
    # Track retry attempts
    attempts = {"count": 0}
    
    def failing_then_succeeding_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:  # Fail first 2 attempts
            raise Exception(f"DNS failure #{attempts['count']}")
        # Succeed on 3rd attempt
        return FakeAnswers([SimpleNamespace(address="8.8.8.8")], ttl=120)
    
    monkeypatch.setattr(db.resolver.Resolver, "resolve", failing_then_succeeding_resolve)
    
    # Should succeed after retries
    ip, ttl = db.get_ip_with_ttl(host, dns_max_retries=3)
    assert ip == "8.8.8.8"
    assert ttl == 120
    assert attempts["count"] == 3  # Should have tried 3 times


def test_extended_grace_period_on_repeated_failures(monkeypatch):
    """Test that grace period extends on repeated failures"""
    host = "failing-host.example.local"
    
    t0 = 1_000_000.0
    monkeypatch.setattr(db.time, "time", lambda: t0)
    
    # Pre-seed cache with IP and 2 previous failures
    db._DNS_CACHE[host] = ("1.2.3.4", t0 - 1, 2)  # expired, 2 previous failures
    
    # Make DNS continue to fail
    monkeypatch.setattr(
        db.resolver.Resolver,
        "resolve",
        lambda self, hostname, qtype, lifetime=None, search=True, **kwargs: (
            _ for _ in ()
        ).throw(Exception("Still failing")),
    )
    
    # Should use cached IP with extended grace period
    ip, ttl = db.get_ip_with_ttl(host, dns_grace_ttl=30, dns_extend_grace=True)
    assert ip == "1.2.3.4"
    # Grace period should be extended: 30 * (2^2) = 120 seconds
    assert ttl == 120
    
    # Check that failure count was incremented
    cached_ip, exp, failure_count = db._DNS_CACHE[host]
    assert failure_count == 3


def test_thread_local_resolver_reuse(monkeypatch):
    """Test that thread-local resolvers are reused properly"""
    host = "thread-test.example.local"
    
    # Track resolver creation
    resolver_instances = []
    original_resolver_init = db.resolver.Resolver.__init__
    
    def tracking_init(self, *args, **kwargs):
        resolver_instances.append(self)
        return original_resolver_init(self, *args, **kwargs)
    
    monkeypatch.setattr(db.resolver.Resolver, "__init__", tracking_init)
    
    # Mock resolve to return valid answers
    def fake_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        return FakeAnswers([SimpleNamespace(address="1.2.3.4")], ttl=120)
    
    monkeypatch.setattr(db.resolver.Resolver, "resolve", fake_resolve)
    
    # Multiple calls should reuse the same resolver instance
    db.get_ip_with_ttl(host)
    # Clear only the DNS cache, not the thread-local resolver
    with db._CACHE_LOCK:
        db._DNS_CACHE.clear()
    db.get_ip_with_ttl(host)
    
    # Should have created only one resolver instance per thread
    assert len(resolver_instances) == 1


def test_fallback_nameservers(monkeypatch):
    """Test that custom nameservers are used when specified"""
    host = "nameserver-test.example.local"
    
    # Track what nameservers were set
    nameserver_configs = []
    
    def tracking_resolver_init(self, *args, **kwargs):
        # Store the original init behavior
        self.nameservers = ["8.8.8.8"]  # Default
        nameserver_configs.append("default")
    
    def tracking_nameserver_setter(self, value):
        nameserver_configs.append(value)
        self._nameservers = value
    
    def tracking_nameserver_getter(self):
        return getattr(self, '_nameservers', ["8.8.8.8"])
    
    monkeypatch.setattr(db.resolver.Resolver, "__init__", tracking_resolver_init)
    monkeypatch.setattr(db.resolver.Resolver, "nameservers", property(tracking_nameserver_getter, tracking_nameserver_setter))
    
    # Mock resolve to return valid answers
    def fake_resolve(self, hostname, qtype, lifetime=None, search=True, **kwargs):
        return FakeAnswers([SimpleNamespace(address="1.2.3.4")], ttl=120)
    
    monkeypatch.setattr(db.resolver.Resolver, "resolve", fake_resolve)
    
    # Test with custom nameservers
    custom_nameservers = ["1.1.1.1", "8.8.4.4"]
    db.get_ip_with_ttl(host, dns_nameservers=custom_nameservers)
    
    # Should have set custom nameservers
    assert custom_nameservers in nameserver_configs
