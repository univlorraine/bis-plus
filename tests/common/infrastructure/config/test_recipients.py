"""Tests pour parse_recipients."""
import logging

from common.infrastructure.config.recipients import parse_recipients


class TestParseRecipients:
    def test_parses_comma_separated_list(self):
        assert parse_recipients("a@x.com, b@x.com") == ["a@x.com", "b@x.com"]

    def test_strips_whitespace(self):
        assert parse_recipients(" a@x.com ,  b@x.com  ") == ["a@x.com", "b@x.com"]

    def test_filters_empty_entries(self):
        assert parse_recipients("a@x.com,,b@x.com,") == ["a@x.com", "b@x.com"]

    def test_none_returns_empty_list(self):
        assert parse_recipients(None) == []

    def test_empty_string_returns_empty_list(self):
        assert parse_recipients("") == []

    def test_single_recipient(self):
        assert parse_recipients("a@x.com") == ["a@x.com"]

    def test_logs_warning_when_empty_and_message_given(self, caplog):
        with caplog.at_level(logging.WARNING):
            parse_recipients(None, warning_message="aucun destinataire")
        assert "aucun destinataire" in caplog.text

    def test_no_warning_when_message_not_given(self, caplog):
        with caplog.at_level(logging.WARNING):
            parse_recipients(None)
        assert caplog.text == ""

    def test_no_warning_when_list_non_empty(self, caplog):
        with caplog.at_level(logging.WARNING):
            parse_recipients("a@x.com", warning_message="ne devrait pas apparaître")
        assert caplog.text == ""
