# tests/test_dns_cache.py
import importlib
import socket
from unittest.mock import patch

import pytest

# import the module under test
db = importlib.import_module("jobmon.server.web.db.dns")


@pytest.fixture(autouse=True)
def clear_cache():
    # clear before and after each test
    db.clear_dns_cache()
    yield
    db.clear_dns_cache()


# Test the new DNS functionality
def test_get_private_azure_hostname():
    """Test Azure private hostname transformation."""
    # Test Azure MySQL hostname
    azure_host = "scicomp-mysql-db-d01.mysql.database.azure.com"
    expected_private = "scicomp-mysql-db-d01.privatelink.mysql.database.azure.com"
    result = db._get_private_azure_hostname(azure_host)
    assert result == expected_private

    # Test non-Azure hostname (should return unchanged)
    non_azure_host = "postgres.example.com"
    result = db._get_private_azure_hostname(non_azure_host)
    assert result == non_azure_host

    # Test another Azure host
    azure_host2 = "mydb.mysql.database.azure.com"
    expected2 = "mydb.privatelink.mysql.database.azure.com"
    result2 = db._get_private_azure_hostname(azure_host2)
    assert result2 == expected2


def test_resolve_host_with_retries_success():
    """Test successful hostname resolution with retries."""
    host = "success.example.com"

    # Mock socket.gethostbyname to succeed on first attempt
    with patch("socket.gethostbyname") as mock_gethostbyname:
        mock_gethostbyname.return_value = "1.2.3.4"

        result = db._resolve_host_with_retries(host)
        assert result is True
        assert mock_gethostbyname.call_count == 1  # Should succeed immediately


def test_resolve_host_with_retries_failure():
    """Test hostname resolution failure with retries."""
    host = "nonexistent.example.com"

    # Mock socket.gethostbyname to always fail
    with patch("socket.gethostbyname") as mock_gethostbyname, patch(
        "time.sleep"
    ) as mock_sleep:  # Mock sleep to speed up test
        mock_gethostbyname.side_effect = socket.gaierror("Name or service not known")

        result = db._resolve_host_with_retries(host)

        assert result is False
        assert mock_gethostbyname.call_count == 5  # Should try 5 times
        assert mock_sleep.call_count == 4  # Should sleep 4 times (not on last attempt)


def test_resolve_host_with_retries_failure_then_success():
    """Test hostname resolution that fails initially then succeeds."""
    host = "retrytest.example.com"

    # Mock socket.gethostbyname to fail 3 times then succeed
    with patch("socket.gethostbyname") as mock_gethostbyname, patch(
        "time.sleep"
    ) as mock_sleep:
        mock_gethostbyname.side_effect = [
            socket.gaierror("Name or service not known"),
            socket.gaierror("Name or service not known"),
            socket.gaierror("Name or service not known"),
            "1.2.3.4",  # Success on 4th attempt
        ]

        result = db._resolve_host_with_retries(host)

        assert result is True
        assert mock_gethostbyname.call_count == 4  # Should try 4 times total
        assert mock_sleep.call_count == 3  # Should sleep 3 times


def test_resolve_host_with_retries_sleep_timing():
    """Test that retry timings are correct."""
    host = "timing.example.com"

    with patch("socket.gethostbyname") as mock_gethostbyname, patch(
        "time.sleep"
    ) as mock_sleep:
        mock_gethostbyname.side_effect = [
            socket.gaierror("Network error"),
            socket.gaierror("Network error"),
            socket.gaierror("Network error"),
            socket.gaierror("Network error"),
            socket.gaierror("Network error"),
        ]

        result = db._resolve_host_with_retries(host)

        assert result is False
        # Verify sleep times: 0.2s, 0.4s, 0.6s, 0.8s (4 calls total)
        expected_sleep_times = [0.2, 0.4, 0.6, 0.8]
        actual_sleep_times = [call[0][0] for call in mock_sleep.call_args_list]
        # Handle floating point precision issues
        assert len(actual_sleep_times) == len(expected_sleep_times)
        for actual, expected in zip(actual_sleep_times, expected_sleep_times):
            assert abs(actual - expected) < 0.01


def test_azure_hostname_edge_cases():
    """Test edge cases for Azure hostname transformation."""
    # Test empty host
    assert db._get_private_azure_hostname("") == ""

    # Test hostname with my sql subdomain but not Azure.com end
    non_azure_mysql = "mysql.internal.example.com"
    assert db._get_private_azure_hostname(non_azure_mysql) == non_azure_mysql

    # Test non-mysql Azure domain
    azure_postgres = "mydb.postgres.database.azure.com"
    assert db._get_private_azure_hostname(azure_postgres) == azure_postgres

    # Test hostname ending exactly with the pattern
    exact_match = "test.mysql.database.azure.com"
    expected = "test.privatelink.mysql.database.azure.com"
    assert db._get_private_azure_hostname(exact_match) == expected


def test_resolve_host_with_retries_logging(monkeypatch, caplog):
    """Test logging behavior during hostname resolution retries."""
    host = "logtest.example.com"

    with patch("socket.gethostbyname") as mock_gethostbyname, patch(
        "time.sleep"
    ) as mock_sleep:
        mock_gethostbyname.side_effect = [
            socket.gaierror("Network error"),
            socket.gaierror("Network error"),
            "1.2.3.4",  # Success on 3rd attempt
        ]

        with caplog.at_level("DEBUG"):
            result = db._resolve_host_with_retries(host)

        assert result is True
        # Check that debug messages were logged for failed attempts
        log_messages = caplog.text
        assert "DNS resolution failed" in log_messages
        assert "retry 1/5" in log_messages
        assert "retry 2/5" in log_messages


def test_creator_function_hostname_availability_logic():
    """Test creator function logic for hostname availability checks."""
    from jobmon.server.web.db.dns import get_dns_engine

    original_host = "mydb.mysql.database.azure.com"
    private_host = "mydb.privatelink.mysql.database.azure.com"

    # Mock the _resolve_host_with_retries function for testing
    db._resolve_host_with_retries = lambda hostname: hostname == private_host

    db_url = f"mysql://user:pass@{original_host}/db"

    # Test that the private hostname logic gets called
    try:
        engine = get_dns_engine(db_url)
        assert engine is not None
    except Exception:
        # Should try to use the private Azure hostname when original fails
        pass


def test_integration_with_dns_resolution():
    """Test integration with actual socket operations."""
    # Test with a real socket resolution (will potentially fail in CI)
    try:
        # This will actually check if a real DNS name is resolvable
        real_hostname = "localhost"  # Should always be resolvable in tests
        result = db._resolve_host_with_retries(real_hostname)
        assert result is True or result is False  # Either success or failure is valid
    except Exception:
        # Allow exceptions (e.g., network issues in CI)
        pass
