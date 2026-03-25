"""
Fetches market data from Polymarket, Kalshi, and PredictIt.
Attempts to match similar markets across platforms and outputs markets.json.
"""

import json
import re
import requests
from datetime import datetime, timezone


def fetch_polymarket():
    """Fetch active events from Polymarket Gamma API (events endpoint for correct URLs)."""
    markets = []
    url = "https://gamma-api.polymarket.com/events"
    params = {"active": "true", "closed": "false", "limit": 100, "order": "volume24hr", "ascending": "false"}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for ev in data:
            event_slug = ev.get("slug", "")
            event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""
            # Each event can have multiple markets
            event_markets = ev.get("markets", [])
            if not event_markets:
                continue
            for m in event_markets:
                if not m.get("question"):
                    continue
                try:
                    prices = json.loads(m.get("outcomePrices", "[]"))
                    yes_price = round(float(prices[0]) * 100) if len(prices) > 0 else None
                    no_price = round(float(prices[1]) * 100) if len(prices) > 1 else None
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue
                if yes_price is None:
                    continue
                markets.append({
                    "question": m.get("question", ev.get("title", "")),
                    "yes": yes_price,
                    "no": no_price or (100 - yes_price),
                    "volume": m.get("volume24hr", 0),
                    "category": m.get("category", ""),
                    "url": event_url,
                })
    except Exception as e:
        print(f"Polymarket fetch error: {e}")
    return markets


def fetch_kalshi():
    """Fetch active markets from Kalshi API."""
    markets = []
    base = "https://api.elections.kalshi.com/trade-api/v2"

    # Fetch events for categories
    event_cats = {}
    try:
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(f"{base}/events", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for ev in data.get("events", []):
                event_cats[ev["event_ticker"]] = {
                    "category": ev.get("category", ""),
                    "slug": ev.get("event_ticker", ""),
                }
            cursor = data.get("cursor")
            if not cursor or not data.get("events"):
                break
    except Exception as e:
        print(f"Kalshi events fetch error: {e}")

    # Fetch markets
    try:
        cursor = None
        while True:
            params = {"limit": 200, "status": "open"}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(f"{base}/markets", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for m in data.get("markets", []):
                title = m.get("title", "")
                subtitle = m.get("yes_sub_title", "")
                question = f"{title}: {subtitle}" if subtitle else title
                try:
                    yes_price = round(float(m.get("last_price_dollars", 0)) * 100)
                    no_price = 100 - yes_price
                except (ValueError, TypeError):
                    continue
                if yes_price <= 0 or yes_price >= 100:
                    continue
                ev_info = event_cats.get(m.get("event_ticker", ""), {})
                cat = ev_info.get("category", "") if isinstance(ev_info, dict) else ev_info
                event_ticker = m.get("event_ticker", "")
                market_url = f"https://kalshi.com/markets/{event_ticker}" if event_ticker else ""
                markets.append({
                    "question": question,
                    "yes": yes_price,
                    "no": no_price,
                    "category": cat,
                    "url": market_url,
                })
            cursor = data.get("cursor")
            if not cursor or not data.get("markets"):
                break
    except Exception as e:
        print(f"Kalshi markets fetch error: {e}")
    return markets


def fetch_predictit():
    """Fetch active markets from PredictIt API."""
    markets = []
    url = "https://www.predictit.org/api/marketdata/all/"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for mkt in data.get("markets", []):
            for contract in mkt.get("contracts", []):
                if contract.get("status") != "Open":
                    continue
                yes_price = contract.get("lastTradePrice")
                if yes_price is None:
                    continue
                yes_cents = round(yes_price * 100)
                if yes_cents <= 0 or yes_cents >= 100:
                    continue
                question = mkt.get("name", "")
                cname = contract.get("name", "")
                if cname and cname != question:
                    question = f"{question}: {cname}"
                mkt_id = mkt.get("id", "")
                market_url = f"https://www.predictit.org/markets/detail/{mkt_id}" if mkt_id else ""
                markets.append({
                    "question": question,
                    "yes": yes_cents,
                    "no": 100 - yes_cents,
                    "category": "",
                    "url": market_url,
                })
    except Exception as e:
        print(f"PredictIt fetch error: {e}")
    return markets


def normalize(text):
    """Normalize text for matching."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_keywords(text):
    """Extract meaningful keywords from a question."""
    stop = {'will', 'the', 'be', 'in', 'of', 'a', 'an', 'to', 'by', 'on', 'for',
            'and', 'or', 'is', 'it', 'this', 'that', 'at', 'from', 'with', 'as',
            'has', 'have', 'do', 'does', 'did', 'not', 'no', 'yes', 'before', 'after',
            'than', 'more', 'most', 'any', 'all', 'each', 'every', 'if', 'then',
            'what', 'who', 'when', 'where', 'how', 'which', 'there', 'here', 'above',
            'below', 'up', 'down', 'over', 'under', 'between', 'through', 'during'}
    words = normalize(text).split()
    return set(w for w in words if w not in stop and len(w) > 2)


def match_markets(poly, kalshi, predictit):
    """Try to match similar markets across platforms using keyword overlap."""
    matched = []
    used_kalshi = set()
    used_predictit = set()

    for i, pm in enumerate(poly):
        pk = get_keywords(pm["question"])
        if len(pk) < 2:
            continue

        best_kalshi = None
        best_kalshi_score = 0
        for j, km in enumerate(kalshi):
            if j in used_kalshi:
                continue
            kk = get_keywords(km["question"])
            overlap = len(pk & kk)
            score = overlap / max(len(pk | kk), 1)
            if score > best_kalshi_score and score > 0.35:
                best_kalshi = j
                best_kalshi_score = score

        best_pi = None
        best_pi_score = 0
        for j, pim in enumerate(predictit):
            if j in used_predictit:
                continue
            pik = get_keywords(pim["question"])
            overlap = len(pk & pik)
            score = overlap / max(len(pk | pik), 1)
            if score > best_pi_score and score > 0.35:
                best_pi = j
                best_pi_score = score

        if best_kalshi is not None or best_pi is not None:
            platforms = {"polymarket": {"yes": pm["yes"], "no": pm["no"], "url": pm.get("url", "")}}
            if best_kalshi is not None:
                platforms["kalshi"] = {"yes": kalshi[best_kalshi]["yes"], "no": kalshi[best_kalshi]["no"], "url": kalshi[best_kalshi].get("url", "")}
                used_kalshi.add(best_kalshi)
            if best_pi is not None:
                platforms["predictit"] = {"yes": predictit[best_pi]["yes"], "no": predictit[best_pi]["no"], "url": predictit[best_pi].get("url", "")}
                used_predictit.add(best_pi)

            cat = pm.get("category") or ""
            if not cat and best_kalshi is not None:
                cat = kalshi[best_kalshi].get("category", "")
            cat = categorize(cat, pm["question"])

            matched.append({
                "category": cat,
                "question": pm["question"],
                "platforms": platforms,
            })

    # Add unmatched Kalshi markets that have high volume/interest
    for j, km in enumerate(kalshi):
        if j not in used_kalshi:
            cat = categorize(km.get("category", ""), km["question"])
            matched.append({
                "category": cat,
                "question": km["question"],
                "platforms": {"kalshi": {"yes": km["yes"], "no": km["no"], "url": km.get("url", "")}},
            })

    # Add unmatched PredictIt markets
    for j, pim in enumerate(predictit):
        if j not in used_predictit:
            cat = categorize("", pim["question"])
            matched.append({
                "category": cat,
                "question": pim["question"],
                "platforms": {"predictit": {"yes": pim["yes"], "no": pim["no"], "url": pim.get("url", "")}},
            })

    # Add unmatched Polymarket markets
    poly_matched = set()
    for m in matched:
        if "polymarket" in m["platforms"]:
            poly_matched.add(m["question"])
    for pm in poly:
        if pm["question"] not in poly_matched:
            cat = categorize(pm.get("category", ""), pm["question"])
            matched.append({
                "category": cat,
                "question": pm["question"],
                "platforms": {"polymarket": {"yes": pm["yes"], "no": pm["no"], "url": pm.get("url", "")}},
            })

    return matched


def categorize(raw_cat, question):
    """Map raw category strings to clean categories."""
    raw = (raw_cat or "").lower()
    q = question.lower()

    cat_map = {
        "politics": "Politics", "us-current-affairs": "Politics", "elections": "Politics",
        "world": "Politics", "us politics": "Politics",
        "sports": "Sports", "nfl": "Sports", "nba": "Sports", "mlb": "Sports",
        "crypto": "Crypto", "bitcoin": "Crypto", "ethereum": "Crypto",
        "economics": "Economics", "financials": "Economics", "fed": "Economics",
        "entertainment": "Entertainment", "culture": "Entertainment",
        "tech": "Tech", "science and technology": "Tech", "ai": "Tech",
        "climate": "Climate", "climate and weather": "Climate", "weather": "Climate",
    }

    for key, val in cat_map.items():
        if key in raw:
            return val

    # Guess from question text
    if any(w in q for w in ["president", "congress", "senate", "election", "biden", "trump", "government", "shutdown"]):
        return "Politics"
    if any(w in q for w in ["bitcoin", "btc", "ethereum", "eth", "crypto", "token"]):
        return "Crypto"
    if any(w in q for w in ["nfl", "nba", "mlb", "super bowl", "world series", "championship", "playoffs"]):
        return "Sports"
    if any(w in q for w in ["fed", "rate", "gdp", "inflation", "unemployment", "recession", "economy"]):
        return "Economics"
    if any(w in q for w in ["movie", "oscar", "grammy", "album", "game", "release", "gta"]):
        return "Entertainment"
    if any(w in q for w in ["apple", "google", "ai", "openai", "ipo", "tiktok", "app"]):
        return "Tech"
    if any(w in q for w in ["hurricane", "temperature", "climate", "weather", "hottest"]):
        return "Climate"

    return "Other"


def main():
    print("Fetching Polymarket...")
    poly = fetch_polymarket()
    print(f"  Got {len(poly)} markets")

    print("Fetching Kalshi...")
    kalshi = fetch_kalshi()
    print(f"  Got {len(kalshi)} markets")

    print("Fetching PredictIt...")
    predictit = fetch_predictit()
    print(f"  Got {len(predictit)} markets")

    print("Matching markets...")
    matched = match_markets(poly, kalshi, predictit)

    # Sort: multi-platform matches first, then by spread
    def sort_key(m):
        n_plats = len(m["platforms"])
        spread = 0
        if n_plats > 1:
            yeses = [p["yes"] for p in m["platforms"].values()]
            spread = max(yeses) - min(yeses)
        return (-n_plats, -spread)

    matched.sort(key=sort_key)

    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "markets": matched,
    }

    with open("markets.json", "w") as f:
        json.dump(output, f, indent=2)

    multi = sum(1 for m in matched if len(m["platforms"]) > 1)
    print(f"Done! {len(matched)} total markets, {multi} matched across platforms.")


if __name__ == "__main__":
    main()
