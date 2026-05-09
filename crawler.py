import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = "ModelGridRadar/0.1 (+local hobby news monitor)"

DEFAULT_SOURCES = [
    {
        "name": "Stone Model Car",
        "kind": "shopify_products",
        "url": "https://www.stonemodelcar.com/products.json?limit=250",
        "base_url": "https://www.stonemodelcar.com",
        "brand_hint": "Stone Model",
        "enabled": True,
    },
    {
        "name": "Bburago Official F1",
        "kind": "shopify_products",
        "url": "https://www.bburago.com/en/collections/formula-1/products.json?limit=250",
        "base_url": "https://www.bburago.com",
        "brand_hint": "Bburago",
        "enabled": True,
    },
    {
        "name": "Looksmart Official Formula 1",
        "kind": "generic_links",
        "url": "https://looksmartmodels.com/product-tag/formula-1/",
        "brand_hint": "Looksmart",
        "exclude_terms": ["Ferrari 1:18", "Ferrari 1:43", "Ferrari F1 History", "Ferrari Le Mans History"],
        "enabled": True,
    },
    {
        "name": "Spark Japan Official News",
        "kind": "generic_links",
        "url": "https://sparkmodel.co.jp/",
        "brand_hint": "Spark",
        "include_terms": ["Preorder Information", "New Item Information", "再生産品予約注文", "FORMULA 1"],
        "exclude_terms": ["Spark Gallery Tokyo"],
        "enabled": True,
    },
    {
        "name": "Solido Official Formula 1",
        "kind": "generic_links",
        "url": "https://www.solido.com/en/theme/formula-1/",
        "brand_hint": "Solido",
        "enabled": True,
    },
    {
        "name": "BBR Models Official",
        "kind": "generic_links",
        "url": "https://www.bbrmodels.it/",
        "brand_hint": "BBR",
        "include_terms": ["F1", "Ferrari SF", "Leclerc", "Hamilton", "Gran Premio", "GP"],
        "exclude_terms": ["ABOUT", "CONTATTI", "NEWSLETTER", "BBR BUILT", "BBR Classic"],
        "enabled": True,
    },
    {
        "name": "GPworld F1 1:43 Spark Modern",
        "kind": "gpworld_lines",
        "url": "https://www.gpworld.nl/en/formula-1-143-spark-modern/?switch_lang=en",
        "enabled": True,
    },
    {
        "name": "GPworld F1 1:18",
        "kind": "gpworld_lines",
        "url": "https://www.gpworld.nl/en/formula-1-118/?switch_lang=en",
        "enabled": True,
    },
    {
        "name": "GPworld F1 1:43",
        "kind": "gpworld_lines",
        "url": "https://www.gpworld.nl/en/formula-1-143/?switch_lang=en",
        "enabled": True,
    },
]

STATUS_WORDS = {
    "announced",
    "aangekondigd",
    "available soon",
    "verwacht",
    "in stock",
    "direct leverbaar",
    "pre-order",
    "pre order",
    "new",
}

MODEL_BRANDS = [
    "Spark",
    "Minichamps",
    "Bburago",
    "LookSmart",
    "Looksmart",
    "BBR",
    "GP Replicas",
    "GPreplicas",
    "TecnoModel",
    "Solido",
    "Werk83",
    "Amalgam",
    "Hot Wheels",
    "TSM",
    "Bell Sports",
    "Top Marques",
    "Tarmac Works",
    "Make Up",
]

TEAMS = [
    "Ferrari",
    "McLaren",
    "Mclaren",
    "Red Bull",
    "Mercedes",
    "Aston Martin",
    "Williams",
    "Alpine",
    "Haas",
    "Sauber",
    "Racing Bulls",
    "AlphaTauri",
    "Toro Rosso",
]

DRIVERS = [
    "Max Verstappen",
    "Lando Norris",
    "Oscar Piastri",
    "Lewis Hamilton",
    "Charles Leclerc",
    "Carlos Sainz",
    "George Russell",
    "Kimi Antonelli",
    "Fernando Alonso",
    "Lance Stroll",
    "Alex Albon",
    "Alexander Albon",
    "Yuki Tsunoda",
    "Liam Lawson",
    "Isack Hadjar",
    "Pierre Gasly",
    "Franco Colapinto",
    "Oliver Bearman",
    "Esteban Ocon",
    "Nico Hulkenberg",
    "Gabriel Bortoleto",
    "Valtteri Bottas",
    "Sergio Perez",
    "Ayrton Senna",
    "Michael Schumacher",
]


def load_sources(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(DEFAULT_SOURCES, ensure_ascii=False, indent=2), encoding="utf-8")

    sources = json.loads(path.read_text(encoding="utf-8"))
    return [source for source in sources if source.get("enabled", True)]


def fetch_all_sources(sources_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sources = load_sources(sources_path)
    items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for source in sources:
        try:
            items.extend(fetch_source(source))
        except Exception as exc:
            errors.append({"source": source.get("name", "Unknown"), "error": str(exc)})
    return items, errors


def fetch_source(source: dict[str, Any]) -> list[dict[str, Any]]:
    kind = source.get("kind")
    if kind == "shopify_products":
        return fetch_shopify_products(source)
    if kind == "gpworld_lines":
        return fetch_gpworld_lines(source)
    if kind == "generic_links":
        return fetch_generic_links(source)
    raise ValueError(f"Unsupported source kind: {kind}")


def fetch_json(url: str) -> Any:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    response.raise_for_status()
    return response.json()


def fetch_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    response.raise_for_status()
    return response.text


def fetch_shopify_products(source: dict[str, Any]) -> list[dict[str, Any]]:
    payload = fetch_json(source["url"])
    products = payload.get("products", [])
    base_url = source.get("base_url") or source["url"]
    items = []
    for product in products[: source.get("limit", 80)]:
        title = normalize(product.get("title", ""))
        text = f"{title} {product.get('vendor', '')} {source.get('brand_hint', '')}"
        if not is_relevant_for_source(text, source):
            continue
        handle = product.get("handle", "")
        product_url = urljoin(base_url, f"/products/{handle}") if handle else source["url"]
        image_url = ""
        images = product.get("images") or []
        if images:
            image_url = images[0].get("src", "")
        item = build_item(
            source_name=source["name"],
            source_url=product_url,
            title=title,
            detail=product.get("vendor") or "",
            image_url=image_url,
            raw_status=title,
            fetched_from=source["url"],
        )
        item["published_at"] = trim_date(product.get("updated_at") or product.get("created_at"))
        item.update(infer_fields(text))
        if source.get("brand_hint") and not item["model_brand"]:
            item["model_brand"] = source["brand_hint"]
        items.append(item)
    return items


def fetch_gpworld_lines(source: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    card_items = parse_gpworld_product_cards(soup, source)
    if card_items:
        return card_items

    lines = [normalize(line) for line in soup.get_text("\n").splitlines()]
    lines = [line for line in lines if line and not is_page_noise(line)]
    items = []
    for index, line in enumerate(lines):
        if line.lower() not in STATUS_WORDS:
            continue
        title = next_content_line(lines, index + 1)
        detail = next_content_line(lines, index + 2)
        text = f"{title} {detail} {source.get('brand_hint', '')}"
        if not title or not is_relevant_for_source(text, source):
            continue
        item = build_item(
            source_name=source["name"],
            source_url=source["url"],
            title=title,
            detail=detail,
            image_url="",
            raw_status=line,
            fetched_from=source["url"],
        )
        item.update(infer_fields(text))
        if source.get("brand_hint") and not item["model_brand"]:
            item["model_brand"] = source["brand_hint"]
        items.append(item)
        if len(items) >= 80:
            break
    return items


def parse_gpworld_product_cards(soup: BeautifulSoup, source: dict[str, Any]) -> list[dict[str, Any]]:
    items = []
    for box in soup.select(".product_box"):
        status_node = box.select_one(".stock-status")
        title_node = box.select_one(".product_head")
        detail_node = box.select_one(".product_copy")
        if not title_node:
            continue
        title = normalize(title_node.get_text(" "))
        detail = normalize(detail_node.get_text(" ")) if detail_node else ""
        status = normalize(status_node.get_text(" ")) if status_node else ""
        text = f"{title} {detail} {source.get('brand_hint', '')}"
        if not is_relevant_for_source(text, source):
            continue
        link = box.find("a", href=True)
        image = find_best_image(box, source["url"])
        item = build_item(
            source_name=source["name"],
            source_url=urljoin(source["url"], link["href"]) if link else source["url"],
            title=title,
            detail=detail,
            image_url=image,
            raw_status=status,
            fetched_from=source["url"],
        )
        item.update(infer_fields(text))
        if source.get("brand_hint") and not item["model_brand"]:
            item["model_brand"] = source["brand_hint"]
        items.append(item)
        if len(items) >= source.get("limit", 80):
            break
    return items


def fetch_generic_links(source: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for link in soup.find_all("a"):
        title = normalize(link.get_text(" "))
        if len(title) < 6 or is_page_noise(title) or "@" in title or not is_relevant_for_source(title, source):
            continue
        if is_excluded_for_source(title, source):
            continue
        href = link.get("href")
        if not href:
            continue
        image_url = find_image_near_link(link, source["url"])
        item = build_item(
            source_name=source["name"],
            source_url=urljoin(source["url"], href),
            title=title,
            detail="",
            image_url=image_url,
            raw_status=title,
            fetched_from=source["url"],
        )
        item.update(infer_fields(f"{title} {source.get('brand_hint', '')}"))
        if source.get("brand_hint") and not item["model_brand"]:
            item["model_brand"] = source["brand_hint"]
        items.append(item)
        if len(items) >= source.get("limit", 80):
            break
    return items


def find_image_near_link(link, base_url: str) -> str:
    image = find_best_image(link, base_url)
    if image:
        return image

    node = link
    for _ in range(5):
        node = node.parent
        if not node:
            return ""
        image = find_best_image(node, base_url)
        if image:
            return image
    return ""


def find_best_image(node, base_url: str) -> str:
    images = node.find_all("img") if hasattr(node, "find_all") else []
    for img in images:
        src = (
            img.get("src")
            or img.get("data-src")
            or img.get("data-lazy-src")
            or img.get("data-original")
            or img.get("data-large_image")
        )
        srcset = img.get("srcset") or img.get("data-srcset")
        if not src and srcset:
            src = srcset.split(",")[0].strip().split(" ")[0]
        if src:
            absolute = urljoin(base_url, src)
            if is_likely_content_image(absolute):
                return absolute
    return ""


def is_likely_content_image(url: str) -> bool:
    lowered = url.lower()
    if any(bad in lowered for bad in ["logo", "flag-", "sprite", "placeholder", "loader"]):
        return False
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"])


def build_item(
    source_name: str,
    source_url: str,
    title: str,
    detail: str,
    image_url: str,
    raw_status: str,
    fetched_from: str,
) -> dict[str, Any]:
    title = normalize(title)
    detail = normalize(detail)
    raw_status = normalize(raw_status)
    return {
        "source_name": source_name,
        "source_url": source_url,
        "source_key": stable_key(source_name, title, detail, source_url),
        "title": title,
        "summary": make_summary(source_name, title, detail, raw_status),
        "image_url": image_url,
        "category": classify_category(f"{raw_status} {title} {detail}"),
        "release_status": classify_status(f"{raw_status} {title} {detail}"),
        "model_brand": "",
        "team": "",
        "driver": "",
        "scale": "",
        "tags": "",
        "published_at": datetime.utcnow().date().isoformat(),
        "fetched_from": fetched_from,
        "raw_text": normalize(f"{raw_status} {title} {detail}"),
    }


def make_summary(source_name: str, title: str, detail: str, raw_status: str) -> str:
    parts = [f"{source_name} 发现新的 F1 车模相关消息。"]
    if raw_status:
        parts.append(f"状态：{translate_status(raw_status)}。")
    if detail:
        parts.append(f"规格：{detail}。")
    parts.append(f"原始标题：{title}")
    return "".join(parts)


def infer_fields(text: str) -> dict[str, str]:
    scale_match = re.search(r"\b1\s*[/,:]\s*(2|5|8|12|18|20|24|43|64)\b", text, re.IGNORECASE)
    brand = first_match(MODEL_BRANDS, text)
    team = first_match(TEAMS, text)
    driver = first_match(DRIVERS, text)
    tags = [value for value in [brand, team, driver, scale_match.group(0).replace(" ", "") if scale_match else ""] if value]
    return {
        "model_brand": brand,
        "team": normalize_team(team),
        "driver": normalize_driver(driver),
        "scale": scale_match.group(0).replace(" ", "").replace(":", "/").replace(",", "/") if scale_match else "",
        "tags": ",".join(dict.fromkeys(tags)),
    }


def first_match(options: list[str], text: str) -> str:
    lowered = text.lower()
    for option in options:
        if option.lower() in lowered:
            return option
    return ""


def normalize_team(team: str) -> str:
    if team == "Mclaren":
        return "McLaren"
    return team


def normalize_driver(driver: str) -> str:
    if driver == "Alexander Albon":
        return "Alex Albon"
    return driver


def classify_category(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ["pre-order", "pre order", "preorder"]):
        return "preorder"
    if any(word in lowered for word in ["announced", "aangekondigd", "available soon", "verwacht", "expected"]):
        return "new"
    if any(word in lowered for word in ["in stock", "direct leverbaar", "available"]):
        return "available"
    if "restock" in lowered or "back in stock" in lowered:
        return "restock"
    return "new"


def classify_status(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ["limited", "exclusive", "special edition", "winner", "world champion"]):
        return "hot"
    if any(word in lowered for word in ["pre-order", "announced", "aangekondigd", "available soon", "verwacht"]):
        return "watch"
    return "normal"


def translate_status(status: str) -> str:
    lowered = status.lower()
    mapping = {
        "announced": "已公布",
        "aangekondigd": "已公布",
        "available soon": "即将到货",
        "verwacht": "预计到货",
        "in stock": "现货",
        "direct leverbaar": "现货",
        "pre-order": "预售",
        "pre order": "预售",
    }
    return mapping.get(lowered, status)


def looks_relevant(text: str) -> bool:
    lowered = text.lower()
    if any(word in lowered for word in ["formula 1", "formula one", "f1", "grand prix"]):
        return True
    return any(name.lower() in lowered for name in MODEL_BRANDS + TEAMS + DRIVERS)


def is_relevant_for_source(text: str, source: dict[str, Any]) -> bool:
    include_terms = [str(term).lower() for term in source.get("include_terms", [])]
    lowered = text.lower()
    if include_terms and any(term in lowered for term in include_terms):
        return True
    return looks_relevant(text)


def is_excluded_for_source(text: str, source: dict[str, Any]) -> bool:
    exclude_terms = [str(term).lower() for term in source.get("exclude_terms", [])]
    lowered = text.lower()
    return any(term in lowered for term in exclude_terms)


def is_page_noise(line: str) -> bool:
    lowered = line.lower()
    return lowered in {
        "☰",
        "home",
        "search",
        "cart",
        "login",
        "log in",
        "contact",
        "view all",
        "add",
        "toggle dropdown",
    }


def next_content_line(lines: list[str], start: int) -> str:
    for line in lines[start : start + 5]:
        if line and line.lower() not in STATUS_WORDS and not is_page_noise(line):
            return line
    return ""


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def stable_key(*parts: str) -> str:
    raw = "|".join(normalize(part).lower() for part in parts if part)
    return re.sub(r"[^a-z0-9]+", "-", raw)[:240]


def trim_date(value: str | None) -> str:
    if not value:
        return datetime.utcnow().date().isoformat()
    return value[:10]
