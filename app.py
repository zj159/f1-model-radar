import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from urllib.parse import unquote, urlencode, urlparse
from xml.sax.saxutils import escape

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from crawler import fetch_all_sources, load_sources, stable_key


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
SOURCES_PATH = DATA_DIR / "sources.json"
DB_PATH = Path(os.getenv("F1_RADAR_DB_PATH", str(DATA_DIR / "radar.sqlite3")))
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("F1_RADAR_DATABASE_URL", "")
ADMIN_TOKEN = os.getenv("F1_RADAR_ADMIN_TOKEN", "")
ADMIN_COOKIE = "f1_radar_admin"
ADMIN_COOKIE_SECURE = os.getenv("F1_RADAR_COOKIE_SECURE", "").lower() in {"1", "true", "yes", "on"}
AUTO_FETCH_INTERVAL_MINUTES = int(os.getenv("F1_RADAR_AUTO_FETCH_INTERVAL_MINUTES", "360") or "0")
PAGE_SIZE = 60
ALLOWED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
AUTO_FETCH_STARTED = False
AUTO_FETCH_LOCK = threading.Lock()
FETCH_RUN_LOCK = threading.Lock()

CATEGORY_LABELS = {
    "new": "新品发布",
    "preorder": "预售开启",
    "restock": "补货到货",
    "available": "现货在售",
    "rumor": "传闻线索",
}

STATUS_LABELS = {
    "watch": "关注",
    "hot": "重点",
    "limited": "限量",
    "normal": "普通",
}

GENERIC_TITLES = {
    "preorder information",
    "pre-order information",
    "new item information",
    "【new item information】",
    "new release",
    "news",
    "announcement",
}

DISPLAY_BRANDS = [
    "LookSmart",
    "Looksmart",
    "Spark",
    "Minichamps",
    "Bburago",
    "BBR",
    "GP Replicas",
    "TecnoModel",
    "Solido",
    "Werk83",
    "Amalgam",
    "Hot Wheels",
]

app = FastAPI(title="F1 Model Radar")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
templates.env.globals["category_label"] = lambda key: CATEGORY_LABELS.get(key or "", key or "未分类")
templates.env.globals["status_label"] = lambda key: STATUS_LABELS.get(key or "", key or "普通")


class HybridRow(dict):
    def __init__(self, columns: list[str], values: tuple):
        super().__init__(zip(columns, values))
        self._values = values

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


class PostgresCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.rowcount = cursor.rowcount
        self.columns = [column.name for column in cursor.description] if cursor.description else []

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return None
        return HybridRow(self.columns, row)

    def fetchall(self):
        return [HybridRow(self.columns, row) for row in self.cursor.fetchall()]


class PostgresConnection:
    def __init__(self, database_url: str):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("DATABASE_URL is set, but psycopg is not installed") from exc

        self.conn = psycopg.connect(database_url)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()

    def execute(self, sql: str, params=None) -> PostgresCursor:
        cursor = self.conn.cursor()
        cursor.execute(to_postgres_sql(sql), params or ())
        return PostgresCursor(cursor)


def to_postgres_sql(sql: str) -> str:
    converted = sql
    converted = converted.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    converted = converted.replace("INSERT OR IGNORE INTO discovered_items", "INSERT INTO discovered_items")
    converted = converted.replace("date(published_at)", "CAST(published_at AS DATE)")
    converted = converted.replace("?", "%s")
    if "INSERT INTO discovered_items" in converted and "ON CONFLICT" not in converted:
        converted = f"{converted} ON CONFLICT (source_key) DO NOTHING"
    return converted


def get_db() -> sqlite3.Connection:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if DATABASE_URL:
        return PostgresConnection(DATABASE_URL)
    DATA_DIR.mkdir(exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def storage_status() -> dict[str, str | bool]:
    if DATABASE_URL:
        return {"engine": "Postgres", "persistent": True, "location": "DATABASE_URL"}
    return {
        "engine": "SQLite",
        "persistent": not str(DB_PATH).startswith("/tmp/"),
        "location": str(DB_PATH),
    }


def table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    if isinstance(db, PostgresConnection):
        rows = db.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table,),
        ).fetchall()
        return {row["column_name"] for row in rows}
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    return {row["name"] for row in rows}


def ensure_column(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    if column not in table_columns(db, table):
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def migrate_db(db: sqlite3.Connection) -> None:
    ensure_column(db, "posts", "source_key", "TEXT")
    ensure_column(db, "posts", "discovered_item_id", "INTEGER")
    db.execute("DELETE FROM posts WHERE source_name = 'Demo Source'")
    backfill_post_source_keys(db)
    backfill_generic_post_titles(db)
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_source_key
        ON posts(source_key)
        WHERE source_key IS NOT NULL AND source_key != ''
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT NOT NULL DEFAULT 'manual',
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            inserted INTEGER NOT NULL DEFAULT 0,
            seen INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            error_details TEXT
        )
        """
    )


def backfill_post_source_keys(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT id, title_cn, summary_cn, source_name, source_url
        FROM posts
        WHERE source_key IS NULL OR source_key = ''
        """
    ).fetchall()
    used = {
        row["source_key"]
        for row in db.execute("SELECT source_key FROM posts WHERE source_key IS NOT NULL AND source_key != ''")
    }
    for row in rows:
        key = stable_key(row["source_name"], row["title_cn"], row["summary_cn"], row["source_url"])
        if not key:
            key = f"post-{row['id']}"
        if key in used:
            key = f"{key}-{row['id']}"
        used.add(key)
        db.execute("UPDATE posts SET source_key = ? WHERE id = ?", (key, row["id"]))


def backfill_generic_post_titles(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT * FROM posts
        WHERE lower(trim(title_cn)) IN (
            'preorder information',
            'pre-order information',
            'new item information',
            '【new item information】',
            'new release',
            'news',
            'announcement'
        )
           OR length(trim(title_cn)) <= 4
        """
    ).fetchall()
    for row in rows:
        data = polished_post_data(row)
        if data["title_cn"] == row["title_cn"]:
            continue
        xhs_title, xhs_body = make_xhs_copy(data)
        db.execute(
            """
            UPDATE posts
            SET title_cn = ?,
                summary_cn = ?,
                model_brand = ?,
                scale = ?,
                tags = ?,
                xhs_title = ?,
                xhs_body = ?
            WHERE id = ?
            """,
            (
                data["title_cn"],
                data["summary_cn"],
                data["model_brand"],
                data["scale"],
                data["tags"],
                xhs_title,
                xhs_body,
                row["id"],
            ),
        )


def init_db() -> None:
    with get_db() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title_cn TEXT NOT NULL,
                summary_cn TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_url TEXT,
                image_url TEXT,
                category TEXT NOT NULL DEFAULT 'new',
                model_brand TEXT,
                team TEXT,
                driver TEXT,
                scale TEXT,
                release_status TEXT NOT NULL DEFAULT 'normal',
                published_at TEXT NOT NULL,
                tags TEXT,
                xhs_title TEXT,
                xhs_body TEXT,
                source_key TEXT,
                discovered_item_id INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                note TEXT,
                contact TEXT,
                reviewed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS discovered_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                source_url TEXT,
                source_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                image_url TEXT,
                category TEXT NOT NULL DEFAULT 'new',
                model_brand TEXT,
                team TEXT,
                driver TEXT,
                scale TEXT,
                release_status TEXT NOT NULL DEFAULT 'normal',
                published_at TEXT NOT NULL,
                tags TEXT,
                fetched_from TEXT,
                raw_text TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        migrate_db(db)
        load_sources(SOURCES_PATH)
        count = db.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        if count == 0 and os.getenv("F1_RADAR_SEED_DEMO") == "1":
            seed_posts(db)


def seed_posts(db: sqlite3.Connection) -> None:
    today = date.today().isoformat()
    examples = [
        {
            "title_cn": "示例：McLaren 2024 迈阿密涂装 1:43 预售信息",
            "summary_cn": "用于演示的信息卡。正式上线前请在后台替换成真实来源、真实图片和真实链接。",
            "source_name": "Demo Source",
            "source_url": "https://example.com",
            "image_url": "",
            "category": "preorder",
            "model_brand": "Spark",
            "team": "McLaren",
            "driver": "Lando Norris",
            "scale": "1:43",
            "release_status": "hot",
            "tags": "McLaren,Norris,1:43,Spark",
            "published_at": today,
        },
        {
            "title_cn": "示例：Ferrari SF-24 1:18 新品线索",
            "summary_cn": "用于占位展示的 Ferrari 车模情报。后台发布真实消息后，这条可以删除或覆盖。",
            "source_name": "Demo Source",
            "source_url": "https://example.com",
            "image_url": "",
            "category": "new",
            "model_brand": "Looksmart",
            "team": "Ferrari",
            "driver": "Charles Leclerc",
            "scale": "1:18",
            "release_status": "watch",
            "tags": "Ferrari,Leclerc,1:18,Looksmart",
            "published_at": today,
        },
        {
            "title_cn": "示例：Red Bull RB20 1:64 补货提醒",
            "summary_cn": "用于测试补货标签和列表筛选。请替换为 Stone Model Car、GPworld 或品牌官方真实消息。",
            "source_name": "Demo Source",
            "source_url": "https://example.com",
            "image_url": "",
            "category": "restock",
            "model_brand": "Bburago",
            "team": "Red Bull Racing",
            "driver": "Max Verstappen",
            "scale": "1:64",
            "release_status": "normal",
            "tags": "Red Bull,Verstappen,1:64,Bburago",
            "published_at": today,
        },
    ]
    for item in examples:
        xhs_title, xhs_body = make_xhs_copy(item)
        db.execute(
            """
            INSERT INTO posts (
                title_cn, summary_cn, source_name, source_url, image_url, category,
                model_brand, team, driver, scale, release_status, published_at,
                tags, xhs_title, xhs_body, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["title_cn"],
                item["summary_cn"],
                item["source_name"],
                item["source_url"],
                item["image_url"],
                item["category"],
                item["model_brand"],
                item["team"],
                item["driver"],
                item["scale"],
                item["release_status"],
                item["published_at"],
                item["tags"],
                xhs_title,
                xhs_body,
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )


def make_xhs_copy(data: dict) -> tuple[str, str]:
    category = CATEGORY_LABELS.get(data.get("category", ""), "新品情报")
    brand = data.get("model_brand") or "车模品牌"
    team = data.get("team") or "F1"
    driver = data.get("driver") or "车手"
    scale = data.get("scale") or "比例待确认"
    title = data.get("title_cn") or f"{team} {driver} {scale} 车模情报"
    summary = data.get("summary_cn") or "海外来源发布了新的 F1 车模相关消息。"
    source = data.get("source_name") or "海外来源"

    xhs_title = f"{category}｜{team} {driver} {scale} {brand}"
    xhs_body = "\n".join(
        [
            f"海外 F1 车模情报：{title}",
            "",
            f"来源：{source}",
            f"品牌/比例：{brand} / {scale}",
            f"车队/车手：{team} / {driver}",
            "",
            summary,
            "",
            "我会继续整理海外新品、预售和补货消息，国内车模玩家不用到处翻。",
            "",
            "#F1车模 #F1周边 #车模收藏 #赛车模型",
        ]
    )
    return xhs_title, xhs_body


def is_generic_title(title: str | None) -> bool:
    normalized = re.sub(r"\s+", " ", (title or "").strip()).lower()
    return normalized in GENERIC_TITLES or len(normalized) <= 4


templates.env.globals["is_generic_title"] = is_generic_title


def image_filename_text(image_url: str | None) -> str:
    if not image_url:
        return ""
    parsed = urlparse(image_url)
    stem = Path(unquote(parsed.path)).stem
    stem = re.sub(r"-\d+x\d+$", "", stem)
    stem = re.sub(r"[_-]+", " ", stem)
    stem = re.sub(r"\b(form|image|img|photo|copy)\b", " ", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem


def title_hint_from_image(image_url: str | None) -> dict[str, str]:
    text = image_filename_text(image_url)
    if not text:
        return {"brand": "", "scale": "", "label": ""}

    brand = first_display_brand(text)
    scale_match = re.search(r"\b1\s*[/\-\s]\s*(12|18|43|64)\b", text, re.IGNORECASE)
    scale = f"1/{scale_match.group(1)}" if scale_match else ""
    month_match = re.search(r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[\s-]*(20\d{2})\b", text, re.IGNORECASE)
    month_label = month_match.group(0).replace("-", " ").upper() if month_match else ""
    label_parts = [part for part in [brand, scale, month_label] if part]
    return {"brand": brand, "scale": scale, "label": " ".join(label_parts)}


def first_display_brand(text: str) -> str:
    lowered = text.lower()
    for brand in DISPLAY_BRANDS:
        if brand.lower().replace(" ", "") in lowered.replace(" ", ""):
            return "LookSmart" if brand.lower() == "looksmart" else brand
    return ""


def polished_discovered_data(item: sqlite3.Row) -> dict[str, str]:
    return polish_item_data(
        title=item["title"],
        summary=item["summary"],
        source_name=item["source_name"],
        source_url=item["source_url"],
        image_url=item["image_url"],
        category=item["category"],
        model_brand=item["model_brand"],
        team=item["team"],
        driver=item["driver"],
        scale=item["scale"],
        release_status=item["release_status"],
        published_at=item["published_at"],
        tags=item["tags"],
        source_key=item["source_key"],
    )


def polished_post_data(row: sqlite3.Row) -> dict[str, str]:
    return polish_item_data(
        title=row["title_cn"],
        summary=row["summary_cn"],
        source_name=row["source_name"],
        source_url=row["source_url"],
        image_url=row["image_url"],
        category=row["category"],
        model_brand=row["model_brand"],
        team=row["team"],
        driver=row["driver"],
        scale=row["scale"],
        release_status=row["release_status"],
        published_at=row["published_at"],
        tags=row["tags"],
        source_key=row["source_key"],
    )


def polish_item_data(
    title: str,
    summary: str,
    source_name: str,
    source_url: str,
    image_url: str,
    category: str,
    model_brand: str,
    team: str,
    driver: str,
    scale: str,
    release_status: str,
    published_at: str,
    tags: str,
    source_key: str,
) -> dict[str, str]:
    title = title or ""
    summary = summary or ""
    model_brand = model_brand or ""
    team = team or ""
    driver = driver or ""
    scale = scale or ""
    image_hint = title_hint_from_image(image_url)
    if image_hint["brand"] and (not model_brand or model_brand == "Spark"):
        model_brand = image_hint["brand"]
    if image_hint["scale"] and not scale:
        scale = image_hint["scale"]

    if is_generic_title(title):
        category_label_text = CATEGORY_LABELS.get(category, "车模情报")
        primary_hint = image_hint["label"] or " ".join(part for part in [model_brand, scale, team, driver] if part)
        title = f"{primary_hint} {category_label_text}".strip() if primary_hint else f"{source_name} {category_label_text}"
        summary_bits = [
            f"{source_name} 抓到一条新的{category_label_text}。",
            "标题来自来源页的通用栏目名，具体车型以原始图片和来源链接为准。",
        ]
        if model_brand or scale or team or driver:
            facts = " / ".join(part for part in [model_brand, scale, team, driver] if part)
            summary_bits.append(f"已识别信息：{facts}。")
        summary = "".join(summary_bits)

    return {
        "title_cn": title,
        "summary_cn": summary,
        "source_name": source_name,
        "source_url": source_url,
        "image_url": image_url,
        "category": category,
        "model_brand": model_brand,
        "team": team,
        "driver": driver,
        "scale": scale,
        "release_status": release_status,
        "published_at": published_at,
        "tags": ",".join(dict.fromkeys(part for part in [tags, model_brand, scale] if part)),
        "source_key": source_key,
    }


def valid_admin_token(token: str | None) -> bool:
    return bool(ADMIN_TOKEN) and token == ADMIN_TOKEN


def require_admin(request: Request) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="Admin token is not configured")
    if not valid_admin_token(request.cookies.get(ADMIN_COOKIE)):
        raise HTTPException(status_code=401, detail="Invalid admin token")


def set_admin_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        ADMIN_COOKIE,
        token,
        httponly=True,
        secure=ADMIN_COOKIE_SECURE,
        samesite="strict",
        max_age=60 * 60 * 24 * 7,
    )


def save_uploaded_image(image_file: UploadFile | None) -> str:
    if not image_file or not image_file.filename:
        return ""

    suffix = Path(image_file.filename).suffix.lower()
    if suffix not in ALLOWED_IMAGE_SUFFIXES:
        raise HTTPException(status_code=400, detail="Only jpg, png, webp, and gif images are supported")

    safe_stem = re.sub(r"[^a-zA-Z0-9_-]+", "-", Path(image_file.filename).stem).strip("-")[:48]
    filename = f"{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:10]}-{safe_stem or 'image'}{suffix}"
    target = UPLOAD_DIR / filename
    contents = image_file.file.read()
    if len(contents) > 6 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image must be smaller than 6 MB")
    target.write_bytes(contents)
    return f"/static/uploads/{filename}"


def distinct_values(db: sqlite3.Connection, column: str) -> list[str]:
    allowed_columns = {"source_name", "team", "driver", "model_brand", "scale"}
    if column not in allowed_columns:
        raise ValueError(f"Unsupported filter column: {column}")
    rows = db.execute(
        f"SELECT DISTINCT {column} AS value FROM posts WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column}"
    ).fetchall()
    return [row["value"] for row in rows]


def build_page_url(query: dict[str, str | int], page: int) -> str:
    cleaned = {
        key: value
        for key, value in query.items()
        if key != "page" and value not in ("", None)
    }
    if page > 1:
        cleaned["page"] = page
    encoded = urlencode(cleaned)
    return f"/?{encoded}" if encoded else "/"


def public_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def absolute_url(request: Request, url: str | None) -> str:
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{public_base_url(request)}{url}"
    return f"{public_base_url(request)}/{url}"


def group_posts_by_date(posts: list[sqlite3.Row]) -> list[dict]:
    groups: list[dict] = []
    current_date = None
    for post in posts:
        date_key = post["published_at"] or "未标日期"
        if date_key != current_date:
            groups.append({"date": date_key, "posts": []})
            current_date = date_key
        groups[-1]["posts"].append(post)
    return groups


def run_fetch_sources(mode: str = "manual") -> dict[str, int]:
    if not FETCH_RUN_LOCK.acquire(blocking=False):
        return {"inserted": 0, "seen": 0, "errors": 0, "busy": 1}
    started_at = datetime.utcnow().isoformat(timespec="seconds")
    items: list[dict] = []
    errors: list[dict] = []
    inserted = 0
    seen = 0
    try:
        init_db()
        items, errors = fetch_all_sources(SOURCES_PATH)
        inserted, seen = save_discovered_items(items)
    except Exception as exc:
        errors.append({"source": "crawler", "error": str(exc)})
    finally:
        finished_at = datetime.utcnow().isoformat(timespec="seconds")
        try:
            with get_db() as db:
                db.execute(
                    """
                    INSERT INTO crawl_runs (mode, started_at, finished_at, inserted, seen, errors, error_details)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mode,
                        started_at,
                        finished_at,
                        inserted,
                        seen,
                        len(errors),
                        json.dumps(errors, ensure_ascii=False),
                    ),
                )
        finally:
            FETCH_RUN_LOCK.release()
    return {"inserted": inserted, "seen": seen, "errors": len(errors), "busy": 0}


def auto_fetch_loop() -> None:
    interval_seconds = max(AUTO_FETCH_INTERVAL_MINUTES, 1) * 60
    time.sleep(interval_seconds)
    while True:
        try:
            run_fetch_sources("auto")
        except Exception:
            pass
        time.sleep(interval_seconds)


def start_auto_fetcher() -> None:
    global AUTO_FETCH_STARTED
    if AUTO_FETCH_INTERVAL_MINUTES <= 0:
        return
    with AUTO_FETCH_LOCK:
        if AUTO_FETCH_STARTED:
            return
        AUTO_FETCH_STARTED = True
        thread = threading.Thread(target=auto_fetch_loop, name="f1-radar-auto-fetch", daemon=True)
        thread.start()


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    start_auto_fetcher()


@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    q: str = "",
    source_name: str = "",
    category: str = "",
    team: str = "",
    driver: str = "",
    model_brand: str = "",
    scale: str = "",
    date_from: str = "",
    date_to: str = "",
    sort: str = "newest",
    page: int = 1,
) -> HTMLResponse:
    init_db()
    clauses = []
    params: list[str] = []
    if q:
        like = f"%{q}%"
        clauses.append(
            "(title_cn LIKE ? OR summary_cn LIKE ? OR source_name LIKE ? OR tags LIKE ? OR team LIKE ? OR driver LIKE ?)"
        )
        params.extend([like, like, like, like, like, like])
    for key, value in [
        ("category", category),
        ("source_name", source_name),
        ("team", team),
        ("driver", driver),
        ("model_brand", model_brand),
        ("scale", scale),
    ]:
        if value:
            clauses.append(f"{key} = ?")
            params.append(value)
    if date_from:
        clauses.append("published_at >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("published_at <= ?")
        params.append(date_to)

    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    order_direction = "ASC" if sort == "oldest" else "DESC"
    today_key = date.today().isoformat()
    week_start_key = (date.today() - timedelta(days=6)).isoformat()
    with get_db() as db:
        total_posts = db.execute(f"SELECT COUNT(*) FROM posts {where}", params).fetchone()[0]
        total_pages = max(1, math.ceil(total_posts / PAGE_SIZE))
        current_page = min(max(page, 1), total_pages)
        offset = (current_page - 1) * PAGE_SIZE
        posts = db.execute(
            f"""
            SELECT * FROM posts
            {where}
            ORDER BY date(published_at) {order_direction}, id {order_direction}
            LIMIT ? OFFSET ?
            """,
            [*params, PAGE_SIZE, offset],
        ).fetchall()
        filters = {
            "categories": CATEGORY_LABELS,
            "sources": distinct_values(db, "source_name"),
            "teams": distinct_values(db, "team"),
            "drivers": distinct_values(db, "driver"),
            "brands": distinct_values(db, "model_brand"),
            "scales": distinct_values(db, "scale"),
        }
        stats = {
            "posts": db.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
            "sources": db.execute("SELECT COUNT(DISTINCT source_name) FROM posts").fetchone()[0],
            "teams": db.execute("SELECT COUNT(DISTINCT team) FROM posts WHERE team != ''").fetchone()[0],
            "today": db.execute("SELECT COUNT(*) FROM posts WHERE published_at = ?", (today_key,)).fetchone()[0],
            "week": db.execute("SELECT COUNT(*) FROM posts WHERE published_at >= ?", (week_start_key,)).fetchone()[0],
            "last_updated": db.execute("SELECT MAX(published_at) FROM posts").fetchone()[0] or "",
            "pending": db.execute("SELECT COUNT(*) FROM discovered_items WHERE status = 'new'").fetchone()[0],
        }
        brief = {
            "highlights": db.execute(
                """
                SELECT id, title_cn, source_name, category, model_brand, team, driver, scale
                FROM posts
                ORDER BY date(published_at) DESC, id DESC
                LIMIT 3
                """
            ).fetchall(),
            "source_counts": db.execute(
                """
                SELECT source_name, COUNT(*) AS total, MAX(published_at) AS last_seen
                FROM posts
                GROUP BY source_name
                ORDER BY total DESC, last_seen DESC, source_name
                LIMIT 6
                """
            ).fetchall(),
        }
    query = {
        "q": q,
        "source_name": source_name,
        "category": category,
        "team": team,
        "driver": driver,
        "model_brand": model_brand,
        "scale": scale,
        "date_from": date_from,
        "date_to": date_to,
        "sort": sort,
        "page": current_page,
    }
    pagination = {
        "page": current_page,
        "per_page": PAGE_SIZE,
        "total": total_posts,
        "total_pages": total_pages,
        "start": 0 if total_posts == 0 else offset + 1,
        "end": min(offset + len(posts), total_posts),
        "prev_url": build_page_url(query, current_page - 1) if current_page > 1 else "",
        "next_url": build_page_url(query, current_page + 1) if current_page < total_pages else "",
    }
    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "request": request,
            "posts": posts,
            "grouped_posts": group_posts_by_date(posts),
            "filters": filters,
            "stats": stats,
            "brief": brief,
            "query": query,
            "pagination": pagination,
            "meta": {
                "title": "F1 车模情报站｜海外新品、预售、补货集中看",
                "description": "整理 Stone Model Car、GPworld、品牌官方和海外店铺动态，国内 F1 车模玩家不用到处翻。",
                "url": str(request.url),
                "type": "website",
            },
        },
    )


@app.get("/posts/{post_id}", response_class=HTMLResponse)
def post_detail(request: Request, post_id: int) -> HTMLResponse:
    init_db()
    with get_db() as db:
        post = db.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return templates.TemplateResponse(
        request=request,
        name="post.html",
        context={
            "request": request,
            "post": post,
            "meta": {
                "title": f"{post['title_cn']}｜F1 车模情报站",
                "description": post["summary_cn"],
                "url": str(request.url),
                "type": "article",
                "image": absolute_url(request, post["image_url"]),
            },
        },
    )


def rss_date(value: str) -> str:
    try:
        if len(value or "") == 10:
            parsed = datetime.combine(date.fromisoformat(value), datetime.min.time(), tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        parsed = datetime.now(timezone.utc)
    return format_datetime(parsed, usegmt=True)


@app.get("/rss.xml")
def rss_feed(request: Request) -> Response:
    init_db()
    base_url = public_base_url(request)
    with get_db() as db:
        posts = db.execute(
            """
            SELECT * FROM posts
            ORDER BY date(published_at) DESC, id DESC
            LIMIT 50
            """
        ).fetchall()
    items = []
    for post in posts:
        link = f"{base_url}/posts/{post['id']}"
        items.append(
            "\n".join(
                [
                    "<item>",
                    f"<title>{escape(post['title_cn'])}</title>",
                    f"<link>{escape(link)}</link>",
                    f"<guid>{escape(link)}</guid>",
                    f"<pubDate>{rss_date(post['published_at'])}</pubDate>",
                    f"<description>{escape(post['summary_cn'])}</description>",
                    "</item>",
                ]
            )
        )
    content = "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<rss version="2.0">',
            "<channel>",
            "<title>F1 车模情报站</title>",
            f"<link>{escape(base_url)}</link>",
            "<description>海外 F1 车模新品、预售、补货消息中文整理。</description>",
            *items,
            "</channel>",
            "</rss>",
        ]
    )
    return Response(content, media_type="application/rss+xml; charset=utf-8")


@app.get("/robots.txt")
def robots(request: Request) -> Response:
    base_url = public_base_url(request)
    content = f"User-agent: *\nAllow: /\nSitemap: {base_url}/sitemap.xml\n"
    return Response(content, media_type="text/plain; charset=utf-8")


@app.get("/healthz")
def healthz() -> dict[str, int | str]:
    init_db()
    with get_db() as db:
        posts = db.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM discovered_items WHERE status = 'new'").fetchone()[0]
        last_run = db.execute("SELECT MAX(finished_at) FROM crawl_runs").fetchone()[0] or ""
    return {"ok": 1, "posts": posts, "pending": pending, "last_crawl": last_run, "storage": str(storage_status()["engine"])}


@app.get("/sitemap.xml")
def sitemap(request: Request) -> Response:
    init_db()
    base_url = public_base_url(request)
    with get_db() as db:
        posts = db.execute("SELECT id, published_at FROM posts ORDER BY id DESC LIMIT 1000").fetchall()
    urls = [
        f"<url><loc>{escape(base_url)}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>",
        f"<url><loc>{escape(base_url)}/submit</loc><changefreq>monthly</changefreq><priority>0.4</priority></url>",
    ]
    for post in posts:
        urls.append(
            f"<url><loc>{escape(base_url)}/posts/{post['id']}</loc><lastmod>{escape(post['published_at'])}</lastmod><priority>0.8</priority></url>"
        )
    content = "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
            *urls,
            "</urlset>",
        ]
    )
    return Response(content, media_type="application/xml; charset=utf-8")


@app.get("/submit", response_class=HTMLResponse)
def submit_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request=request, name="submit.html", context={"request": request})


@app.post("/submit")
def submit_source(
    source_url: str = Form(...),
    note: str = Form(""),
    contact: str = Form(""),
):
    init_db()
    with get_db() as db:
        db.execute(
            "INSERT INTO submissions (source_url, note, contact, created_at) VALUES (?, ?, ?, ?)",
            (source_url.strip(), note.strip(), contact.strip(), datetime.utcnow().isoformat(timespec="seconds")),
        )
    return RedirectResponse("/submit?sent=1", status_code=303)


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="admin_login.html",
        context={"request": request, "admin_configured": bool(ADMIN_TOKEN)},
    )


@app.post("/admin/login")
def admin_login(request: Request, token: str = Form(...)):
    if not valid_admin_token(token):
        raise HTTPException(status_code=401, detail="Invalid admin token")
    response = RedirectResponse("/admin", status_code=303)
    set_admin_cookie(response, token)
    return response


@app.post("/admin/logout")
def admin_logout():
    response = RedirectResponse("/admin/login", status_code=303)
    response.delete_cookie(ADMIN_COOKIE, secure=ADMIN_COOKIE_SECURE, samesite="strict")
    return response


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request, source: str = "") -> HTMLResponse:
    if not valid_admin_token(request.cookies.get(ADMIN_COOKIE)):
        return RedirectResponse("/admin/login", status_code=303)
    init_db()
    discovered_params: list[str] = []
    source_clause = ""
    if source:
        source_clause = "AND source_name = ?"
        discovered_params.append(source)
    with get_db() as db:
        posts = db.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 20").fetchall()
        submissions = db.execute(
            "SELECT * FROM submissions ORDER BY reviewed ASC, id DESC LIMIT 20"
        ).fetchall()
        discovered = db.execute(
            f"""
            SELECT * FROM discovered_items
            WHERE status = 'new'
            {source_clause}
            ORDER BY first_seen_at DESC, id DESC
            LIMIT 60
            """,
            discovered_params,
        ).fetchall()
        source_stats = db.execute(
            """
            SELECT source_name,
                   SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS pending,
                   COUNT(*) AS seen_total,
                   SUM(CASE WHEN image_url != '' THEN 1 ELSE 0 END) AS with_images,
                   MAX(last_seen_at) AS last_seen_at
            FROM discovered_items
            GROUP BY source_name
            ORDER BY pending DESC, last_seen_at DESC, source_name
            """
        ).fetchall()
        crawl_runs = db.execute(
            """
            SELECT * FROM crawl_runs
            ORDER BY id DESC
            LIMIT 8
            """
        ).fetchall()
    response = templates.TemplateResponse(
        request=request,
        name="admin.html",
        context={
            "request": request,
            "posts": posts,
            "submissions": submissions,
            "discovered": discovered,
            "source_stats": source_stats,
            "crawl_runs": crawl_runs,
            "selected_source": source,
            "sources": load_sources(SOURCES_PATH),
            "categories": CATEGORY_LABELS,
            "statuses": STATUS_LABELS,
            "today": date.today().isoformat(),
            "auto_fetch_minutes": AUTO_FETCH_INTERVAL_MINUTES,
            "storage": storage_status(),
        },
    )
    return response


@app.post("/admin/posts")
def create_post(
    request: Request,
    title_cn: str = Form(...),
    summary_cn: str = Form(...),
    source_name: str = Form(...),
    source_url: str = Form(""),
    image_url: str = Form(""),
    category: str = Form("new"),
    model_brand: str = Form(""),
    team: str = Form(""),
    driver: str = Form(""),
    scale: str = Form(""),
    release_status: str = Form("normal"),
    published_at: str = Form(""),
    tags: str = Form(""),
    image_file: UploadFile | None = File(None),
):
    require_admin(request)
    init_db()
    uploaded_image_url = save_uploaded_image(image_file)
    item = {
        "title_cn": title_cn.strip(),
        "summary_cn": summary_cn.strip(),
        "source_name": source_name.strip(),
        "source_url": source_url.strip(),
        "image_url": uploaded_image_url or image_url.strip(),
        "category": category,
        "model_brand": model_brand.strip(),
        "team": team.strip(),
        "driver": driver.strip(),
        "scale": scale.strip(),
        "release_status": release_status,
        "published_at": published_at or date.today().isoformat(),
        "tags": tags.strip(),
    }
    xhs_title, xhs_body = make_xhs_copy(item)
    source_key = stable_key("manual", item["source_name"], item["title_cn"], item["source_url"])
    with get_db() as db:
        duplicate = db.execute(
            """
            SELECT 1 FROM posts
            WHERE source_key = ?
               OR (source_url != '' AND source_url = ? AND source_name = ?)
               OR (source_name = ? AND title_cn = ?)
            LIMIT 1
            """,
            (
                source_key,
                item["source_url"],
                item["source_name"],
                item["source_name"],
                item["title_cn"],
            ),
        ).fetchone()
        if duplicate:
            return RedirectResponse("/admin?duplicate=1", status_code=303)
        db.execute(
            """
            INSERT INTO posts (
                title_cn, summary_cn, source_name, source_url, image_url, category,
                model_brand, team, driver, scale, release_status, published_at,
                tags, xhs_title, xhs_body, source_key, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["title_cn"],
                item["summary_cn"],
                item["source_name"],
                item["source_url"],
                item["image_url"],
                item["category"],
                item["model_brand"],
                item["team"],
                item["driver"],
                item["scale"],
                item["release_status"],
                item["published_at"],
                item["tags"],
                xhs_title,
                xhs_body,
                source_key,
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/submissions/{submission_id}/reviewed")
def mark_submission_reviewed(request: Request, submission_id: int):
    require_admin(request)
    init_db()
    with get_db() as db:
        db.execute("UPDATE submissions SET reviewed = 1 WHERE id = ?", (submission_id,))
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/posts/{post_id}/delete")
def delete_post(request: Request, post_id: int):
    require_admin(request)
    init_db()
    with get_db() as db:
        db.execute("DELETE FROM posts WHERE id = ?", (post_id,))
    return RedirectResponse("/admin", status_code=303)


def save_discovered_items(items: list[dict]) -> tuple[int, int]:
    now = datetime.utcnow().isoformat(timespec="seconds")
    inserted = 0
    seen = 0
    with get_db() as db:
        for item in items:
            cur = db.execute(
                """
                INSERT OR IGNORE INTO discovered_items (
                    source_name, source_url, source_key, title, summary, image_url,
                    category, model_brand, team, driver, scale, release_status,
                    published_at, tags, fetched_from, raw_text, status,
                    first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
                """,
                (
                    item.get("source_name", ""),
                    item.get("source_url", ""),
                    item.get("source_key", ""),
                    item.get("title", ""),
                    item.get("summary", ""),
                    item.get("image_url", ""),
                    item.get("category", "new"),
                    item.get("model_brand", ""),
                    item.get("team", ""),
                    item.get("driver", ""),
                    item.get("scale", ""),
                    item.get("release_status", "normal"),
                    item.get("published_at") or date.today().isoformat(),
                    item.get("tags", ""),
                    item.get("fetched_from", ""),
                    item.get("raw_text", ""),
                    now,
                    now,
                ),
            )
            if cur.rowcount:
                inserted += 1
            else:
                seen += 1
                db.execute(
                    """
                    UPDATE discovered_items
                    SET last_seen_at = ?,
                        image_url = CASE
                            WHEN (image_url IS NULL OR image_url = '') AND ? != '' THEN ?
                            ELSE image_url
                        END,
                        source_url = CASE
                            WHEN (source_url IS NULL OR source_url = '') AND ? != '' THEN ?
                            ELSE source_url
                        END
                    WHERE source_key = ?
                    """,
                    (
                        now,
                        item.get("image_url", ""),
                        item.get("image_url", ""),
                        item.get("source_url", ""),
                        item.get("source_url", ""),
                        item.get("source_key", ""),
                    ),
                )
    return inserted, seen


@app.post("/admin/fetch-sources")
def fetch_sources(request: Request):
    require_admin(request)
    result = run_fetch_sources("manual")
    if result.get("busy"):
        return RedirectResponse("/admin?busy=1", status_code=303)
    return RedirectResponse(
        f"/admin?fetched={result['inserted']}&seen={result['seen']}&errors={result['errors']}",
        status_code=303,
    )


def discovered_row_to_post_data(item: sqlite3.Row) -> dict[str, str]:
    return polished_discovered_data(item)


def post_exists_for_discovered(db: sqlite3.Connection, item: sqlite3.Row) -> bool:
    return bool(
        db.execute(
            """
            SELECT 1 FROM posts
            WHERE source_key = ?
               OR (source_url != '' AND source_url = ? AND source_name = ?)
               OR (source_name = ? AND title_cn = ?)
            LIMIT 1
            """,
            (
                item["source_key"],
                item["source_url"],
                item["source_name"],
                item["source_name"],
                item["title"],
            ),
        ).fetchone()
    )


def insert_post_from_discovered(db: sqlite3.Connection, item: sqlite3.Row) -> bool:
    if post_exists_for_discovered(db, item):
        return False
    data = discovered_row_to_post_data(item)
    xhs_title, xhs_body = make_xhs_copy(data)
    db.execute(
        """
        INSERT INTO posts (
            title_cn, summary_cn, source_name, source_url, image_url, category,
            model_brand, team, driver, scale, release_status, published_at,
            tags, xhs_title, xhs_body, source_key, discovered_item_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["title_cn"],
            data["summary_cn"],
            data["source_name"],
            data["source_url"],
            data["image_url"],
            data["category"],
            data["model_brand"],
            data["team"],
            data["driver"],
            data["scale"],
            data["release_status"],
            data["published_at"],
            data["tags"],
            xhs_title,
            xhs_body,
            data["source_key"],
            item["id"],
            datetime.utcnow().isoformat(timespec="seconds"),
        ),
    )
    return True


@app.post("/admin/discovered/{item_id}/publish")
def publish_discovered(request: Request, item_id: int):
    require_admin(request)
    init_db()
    with get_db() as db:
        item = db.execute("SELECT * FROM discovered_items WHERE id = ?", (item_id,)).fetchone()
        if not item:
            raise HTTPException(status_code=404, detail="Discovered item not found")
        inserted = insert_post_from_discovered(db, item)
        db.execute("UPDATE discovered_items SET status = 'published' WHERE id = ?", (item_id,))
    suffix = "" if inserted else "?duplicate=1"
    return RedirectResponse(f"/admin{suffix}", status_code=303)


@app.post("/admin/discovered/publish-source")
def publish_discovered_source(
    request: Request,
    source_name: str = Form(...),
    limit: int = Form(20),
):
    require_admin(request)
    init_db()
    publish_limit = max(1, min(limit, 100))
    inserted_count = 0
    skipped_count = 0
    with get_db() as db:
        items = db.execute(
            """
            SELECT * FROM discovered_items
            WHERE status = 'new' AND source_name = ?
            ORDER BY first_seen_at DESC, id DESC
            LIMIT ?
            """,
            (source_name, publish_limit),
        ).fetchall()
        for item in items:
            if insert_post_from_discovered(db, item):
                inserted_count += 1
            else:
                skipped_count += 1
        ids = [str(item["id"]) for item in items]
        if ids:
            db.execute(
                f"UPDATE discovered_items SET status = 'published' WHERE id IN ({','.join(['?'] * len(ids))})",
                ids,
            )
    return RedirectResponse(
        f"/admin?published={inserted_count}&skipped={skipped_count}&source={source_name}",
        status_code=303,
    )


@app.post("/admin/discovered/{item_id}/ignore")
def ignore_discovered(request: Request, item_id: int):
    require_admin(request)
    init_db()
    with get_db() as db:
        db.execute("UPDATE discovered_items SET status = 'ignored' WHERE id = ?", (item_id,))
    return RedirectResponse("/admin", status_code=303)
