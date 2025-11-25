"""Centralised selectors for the search page.

Update these constants with resilient locators (role-based, test IDs, etc.).
"""
from __future__ import annotations


class SearchSelectors:
    query_input = "input[name='q']"
    submit_button = "button[type='submit']"
    results_container = "div.search-results"
    result_items = "div.search-result"
    result_title = "h2"
    result_link = "a"
