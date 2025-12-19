from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import quote_plus

import feedparser

logger = logging.getLogger(__name__)


@dataclass
class RssNewsItem:
    title: str
    publisher: Optional[str]
    link: Optional[str]
    published: Optional[str]


def _fmt_published(struct_time) -> Optional[str]:
    try:
        if not struct_time:
            return None
        dt = datetime(*struct_time[:6], tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return None


def google_news_rss_url(query: str, *, hl: str = "en-IN", gl: str = "IN", ceid: str = "IN:en") -> str:
    q = quote_plus(query)
    return f"https://news.google.com/rss/search?q={q}&hl={hl}&gl={gl}&ceid={ceid}"


def fetch_google_news(query: str, limit: int = 5) -> List[RssNewsItem]:
    """Fetch news headlines via Google News RSS.

    We only store title + link + publisher + published time (no article scraping).
    """
    url = google_news_rss_url(query)
    try:
        feed = feedparser.parse(url)
        items: List[RssNewsItem] = []
        for e in (feed.entries or [])[: max(0, int(limit))]:
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", None)
            source_title = None
            src = getattr(e, "source", None)
            if isinstance(src, dict):
                source_title = src.get("title")
            elif isinstance(src, list) and src and isinstance(src[0], dict):
                source_title = src[0].get("title")

            publisher = source_title or getattr(e, "author", None)
            published = _fmt_published(getattr(e, "published_parsed", None))
            items.append(RssNewsItem(title=title, publisher=publisher, link=link, published=published))
        return items
    except Exception:
        logger.exception("RSS fetch failed", extra={"query": query})
        return []
