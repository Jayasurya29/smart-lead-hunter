"""Quick debug: See exactly what URLs each search engine returns."""

import asyncio
import base64
import httpx
import re
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urlparse, quote_plus


def decode_gnews_url(gnews_url):
    """Decode actual article URL from Google News base64-encoded redirect."""
    if "/rss/articles/" not in gnews_url:
        return None
    try:
        encoded = gnews_url.split("/rss/articles/")[-1].split("?")[0]
        padded = encoded + "=" * (4 - len(encoded) % 4)
        decoded = base64.urlsafe_b64decode(padded)
        decoded_str = decoded.decode("utf-8", errors="ignore")
        match = re.search(r'https?://[^\s\x00-\x1f"]+', decoded_str)
        if match:
            return match.group(0).rstrip("/")
    except Exception:
        pass
    return None


async def search_ddg(query, client):
    """DuckDuckGo HTML search (deduplicated by domain)"""
    resp = await client.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        headers={"User-Agent": "Mozilla/5.0"},
    )
    if resp.status_code != 200:
        return [f"⚠️  DDG returned {resp.status_code} (rate limited)"]

    urls = []
    seen = set()
    for match in re.finditer(r'uddg=([^&"]+)', resp.text):
        url = unquote(match.group(1)).replace("&amp;", "&").split("&")[0]
        if not url.startswith("http"):
            continue
        domain = urlparse(url).netloc.replace("www.", "")
        if domain in seen:
            continue
        seen.add(domain)
        urls.append(url)
    return urls


async def search_google_news(query, client):
    """Google News RSS with base64 URL decoding"""
    resp = await client.get(
        f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    if resp.status_code != 200:
        return [f"⚠️  Google News returned {resp.status_code}"]

    items = []
    try:
        root = ET.fromstring(resp.text)
        for item in root.findall(".//item")[:15]:
            title = item.findtext("title", "")
            gnews_link = item.findtext("link", "")
            source = item.find("source")
            source_domain = source.get("url", "") if source is not None else ""

            decoded = decode_gnews_url(gnews_link)

            items.append(
                {
                    "title": title,
                    "decoded_url": decoded,
                    "source_domain": source_domain,
                }
            )
    except Exception as e:
        return [f"⚠️  Parse error: {e}"]

    return items


async def main():
    queries = [
        "new hotel opening 2026 United States",
        "new luxury hotel opening 2026",
    ]

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for i, query in enumerate(queries, 1):
            print(f"\n{'='*90}")
            print(f"  QUERY {i}: {query}")
            print(f"{'='*90}")

            # DuckDuckGo
            print(f"\n  🦆 DuckDuckGo ({10} unique max):")
            ddg = await search_ddg(query, client)
            for j, url in enumerate(ddg, 1):
                if url.startswith("⚠️"):
                    print(f"     {url}")
                else:
                    domain = urlparse(url).netloc
                    short = url[:120] + "..." if len(url) > 120 else url
                    print(f"     {j:2d}. [{domain}] {short}")

            await asyncio.sleep(3)

            # Google News
            print("\n  📰 Google News (base64 decoded):")
            gn = await search_google_news(query, client)
            decoded_count = 0
            fallback_count = 0
            for j, item in enumerate(gn, 1):
                if isinstance(item, str):
                    print(f"     {item}")
                    continue
                title = item["title"][:70]
                decoded = item["decoded_url"]
                domain_only = item["source_domain"]

                if decoded:
                    decoded_count += 1
                    rdomain = urlparse(decoded).netloc
                    short = decoded[:120] + "..." if len(decoded) > 120 else decoded
                    print(f"     {j:2d}. ✅ [{rdomain}] {title}")
                    print(f"         {short}")
                else:
                    fallback_count += 1
                    ddomain = urlparse(domain_only).netloc if domain_only else "?"
                    print(f"     {j:2d}. ❌ [{ddomain}] {title}")
                    print(f"         FALLBACK: {domain_only}")

            print(
                f"\n     📊 Decoded: {decoded_count}/{decoded_count + fallback_count}"
            )

            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
