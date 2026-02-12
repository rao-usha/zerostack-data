"""
Unit tests for MX verifier.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import importlib.util
spec = importlib.util.spec_from_file_location(
    "mx_verifier",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "sources", "people_collection", "mx_verifier.py")
)
mx_verifier_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mx_verifier_module)

MXVerifier = mx_verifier_module.MXVerifier
MXVerificationResult = mx_verifier_module.MXVerificationResult


class TestMXVerifier:
    """Tests for MXVerifier class."""

    @pytest.fixture
    def verifier(self):
        return MXVerifier(cache_ttl=60)

    @pytest.mark.unit
    def test_empty_domain(self, verifier):
        """Empty domain returns has_mx=False."""
        result = verifier.verify_domain("")
        assert result.has_mx is False
        assert result.error == "Empty domain"

    @pytest.mark.unit
    def test_domain_normalization(self, verifier):
        """Domain is lowercased and stripped."""
        with patch.object(verifier, '_dns_lookup') as mock_lookup:
            mock_lookup.return_value = MXVerificationResult(has_mx=True, mx_records=["10 mail.example.com"])
            verifier.verify_domain("  Example.COM  ")
            mock_lookup.assert_called_once_with("example.com")

    @pytest.mark.unit
    def test_cache_hit(self, verifier):
        """Second call for same domain returns cached result."""
        expected = MXVerificationResult(has_mx=True, mx_records=["10 mail.example.com"])
        with patch.object(verifier, '_dns_lookup', return_value=expected) as mock_lookup:
            result1 = verifier.verify_domain("example.com")
            result2 = verifier.verify_domain("example.com")

            assert mock_lookup.call_count == 1
            assert result1.has_mx is True
            assert result2.has_mx is True

    @pytest.mark.unit
    def test_cache_expiry(self, verifier):
        """Expired cache entries trigger new lookup."""
        import time
        verifier._cache_ttl = 0  # Expire immediately

        expected = MXVerificationResult(has_mx=True, mx_records=["10 mail.example.com"])
        with patch.object(verifier, '_dns_lookup', return_value=expected) as mock_lookup:
            verifier.verify_domain("example.com")
            time.sleep(0.01)
            verifier.verify_domain("example.com")

            assert mock_lookup.call_count == 2

    @pytest.mark.unit
    def test_dns_success(self, verifier):
        """Successful DNS lookup returns MX records."""
        mock_answer = MagicMock()
        mock_record = MagicMock()
        mock_record.preference = 10
        mock_record.exchange = MagicMock()
        mock_record.exchange.__str__ = lambda self: "mail.example.com."
        mock_answer.__iter__ = lambda self: iter([mock_record])

        with patch.dict('sys.modules', {'dns': MagicMock(), 'dns.resolver': MagicMock()}):
            import dns.resolver
            dns.resolver.resolve = MagicMock(return_value=mock_answer)

            result = verifier._dns_lookup("example.com")
            assert result.has_mx is True
            assert len(result.mx_records) == 1

    @pytest.mark.unit
    def test_dns_import_error_graceful(self, verifier):
        """Missing dnspython falls back to assuming valid."""
        # Simulate ImportError by patching the dns import
        with patch.dict('sys.modules', {'dns': None, 'dns.resolver': None}):
            result = verifier._dns_lookup("example.com")
            # Should gracefully assume valid
            assert result.has_mx is True

    @pytest.mark.unit
    def test_clear_cache(self, verifier):
        """Cache can be cleared."""
        verifier._cache["test.com"] = (MXVerificationResult(has_mx=True), 0)
        assert verifier.cache_size == 1

        cleared = verifier.clear_cache()
        assert cleared == 1
        assert verifier.cache_size == 0

    @pytest.mark.unit
    def test_cache_size(self, verifier):
        """Cache size reports correctly."""
        assert verifier.cache_size == 0

        with patch.object(verifier, '_dns_lookup', return_value=MXVerificationResult(has_mx=True)):
            verifier.verify_domain("a.com")
            verifier.verify_domain("b.com")

        assert verifier.cache_size == 2
