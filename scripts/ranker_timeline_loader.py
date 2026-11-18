#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import hashlib
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

import pymysql
from pymysql.cursors import DictCursor

class NeopleSystemInspect(Exception):
    pass

# -----------------------------
# Runtime Config (env-driven)
# -----------------------------
TOP_N = int(os.getenv("TOP_N", "0"))              # 0이면 전체
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100"))  # 100개씩

DEFAULT_START_DATE = "2025-12-11 00:00"
DEFAULT_END_DATE   = "2025-12-18 00:00"
START_DATE = os.getenv("START_DATE", DEFAULT_START_DATE)
END_DATE   = os.getenv("END_DATE", DEFAULT_END_DATE)

MAX_DATE_PARSE_WARN = 10

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

def load_cfg():
    """
    로컬 실행(.env) + Airflow 실행(env 주입) 모두 지원.
    """
    load_dotenv()
    cfg = {
        "NEOPLE_API_KEY": os.getenv("NEOPLE_API_KEY", "").strip(),

        "DB_HOST": os.getenv("DB_HOST", "localhost"),
        "DB_PORT": int(os.getenv("DB_PORT", "3306")),
        "DB_USER": os.getenv("DB_USER", "root"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
        "DB_NAME": os.getenv("DB_NAME", "bronze"),

        "TABLE_NAME": os.getenv("TABLE_NAME", "timeline_ranker"),
        "RANKER_TABLE": os.getenv("RANKER_TABLE", "ranker_info"),
    }
    if not cfg["NEOPLE_API_KEY"]:
        raise RuntimeError("NEOPLE_API_KEY is empty. Set it via env or .env")
    return cfg

# -----------------------------
# Target Table DDL (as provided)
# -----------------------------
DDL = r"""
CREATE TABLE IF NOT EXISTS `timeline_ranker` (
  `event_uid` char(64) NOT NULL,
  `server_id` varchar(32) NOT NULL,
  `character_id` varchar(64) NOT NULL,
  `character_name` varchar(64) DEFAULT NULL,
  `level` int DEFAULT NULL,
  `job_id` varchar(64) DEFAULT NULL,
  `job_grow_id` varchar(64) DEFAULT NULL,
  `job_name` varchar(64) DEFAULT NULL,
  `job_grow_name` varchar(64) DEFAULT NULL,
  `fame` int DEFAULT NULL,
  `adventure_name` varchar(64) DEFAULT NULL,
  `guild_id` varchar(64) DEFAULT NULL,
  `guild_name` varchar(64) DEFAULT NULL,
  `event_date` datetime NOT NULL,
  `event_code` int NOT NULL,
  `event_name` varchar(255) NOT NULL,
  `item_id` varchar(64) DEFAULT NULL,
  `item_name` varchar(255) DEFAULT NULL,
  `item_rarity` varchar(32) DEFAULT NULL,
  `channel_name` varchar(64) DEFAULT NULL,
  `channel_no` int DEFAULT NULL,
  `dungeon_name` varchar(128) DEFAULT NULL,
  `mist_gear` tinyint(1) DEFAULT NULL,
  `raw_json` json DEFAULT NULL,
  `inserted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_uid`),
  KEY `idx_char_date` (`character_id`,`event_date`),
  KEY `idx_server_char` (`server_id`,`character_id`),
  KEY `idx_code_date` (`event_code`,`event_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

def get_conn(cfg):
    return pymysql.connect(
        host=cfg["DB_HOST"],
        port=cfg["DB_PORT"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASSWORD"],
        database=cfg["DB_NAME"],
        cursorclass=DictCursor,
        autocommit=True,
        charset="utf8mb4"
    )

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)

def upsert_rows(conn, table, rows):
    if not rows:
        return 0

    cols = [
        "event_uid","server_id","character_id",
        "character_name","level","job_id","job_grow_id","job_name","job_grow_name",
        "fame","adventure_name","guild_id","guild_name",
        "event_date","event_code","event_name",
        "item_id","item_name","item_rarity",
        "channel_name","channel_no","dungeon_name","mist_gear",
        "raw_json",
    ]

    placeholders = ",".join(["%s"] * len(cols))
    sql = f"""
    INSERT INTO `{table}` ({",".join(f"`{c}`" for c in cols)})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE
      character_name=VALUES(character_name),
      level=VALUES(level),
      job_id=VALUES(job_id),
      job_grow_id=VALUES(job_grow_id),
      job_name=VALUES(job_name),
      job_grow_name=VALUES(job_grow_name),
      fame=VALUES(fame),
      adventure_name=VALUES(adventure_name),
      guild_id=VALUES(guild_id),
      guild_name=VALUES(guild_name),
      item_id=VALUES(item_id),
      item_name=VALUES(item_name),
      item_rarity=VALUES(item_rarity),
      channel_name=VALUES(channel_name),
      channel_no=VALUES(channel_no),
      dungeon_name=VALUES(dungeon_name),
      mist_gear=VALUES(mist_gear),
      raw_json=VALUES(raw_json)
    """

    vals = [tuple(r.get(c) for c in cols) for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, vals)
    return len(rows)

# -----------------------------
# Source Rankers
# -----------------------------
def get_ranker_total_count(conn, ranker_table):
    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT DISTINCT serverId, characterId
        FROM `{ranker_table}`
        WHERE serverId IS NOT NULL AND characterId IS NOT NULL
    ) t
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()["cnt"])

def collect_rankers_chunk(conn, ranker_table, limit, offset):
    sql = f"""
    SELECT DISTINCT serverId, characterId, characterName
    FROM `{ranker_table}`
    WHERE serverId IS NOT NULL AND characterId IS NOT NULL
    ORDER BY serverId, characterId
    LIMIT %s OFFSET %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()
    return [{
        "serverId": r["serverId"],
        "characterId": r["characterId"],
        "characterName": r.get("characterName"),
    } for r in rows]

# -----------------------------
# Neople API
# -----------------------------
def _raise_if_system_inspect(resp, server_id, character_id):
    if resp.status_code != 503:
        return
    try:
        j = resp.json()
    except Exception:
        j = {}
    if isinstance(j, dict):
        err = j.get("error") or {}
        if err.get("code") == "DNF980" and err.get("message") == "SYSTEM_INSPECT":
            raise NeopleSystemInspect(f"SYSTEM_INSPECT: {server_id}/{character_id}")

def neople_get_character_basic(api_key, server_id, character_id):
    url = f"https://api.neople.co.kr/df/servers/{server_id}/characters/{character_id}"
    r = requests.get(url, params={"apikey": api_key}, timeout=15)
    _raise_if_system_inspect(r, server_id, character_id)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    data = r.json()
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return {}
    return data if isinstance(data, dict) else {}

def neople_get_timeline(api_key, server_id, character_id, start_dt, end_dt, limit=100, next_token=None):
    url = f"https://api.neople.co.kr/df/servers/{server_id}/characters/{character_id}/timeline"
    params = {"apikey": api_key, "startDate": start_dt, "endDate": end_dt, "limit": limit}
    if next_token:
        params["next"] = next_token
    r = requests.get(url, params=params, timeout=20)
    _raise_if_system_inspect(r, server_id, character_id)
    if r.status_code == 404:
        return {"timeline": [], "next": None}
    r.raise_for_status()
    data = r.json()
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return {"timeline": [], "next": None}
    return data if isinstance(data, dict) else {"timeline": [], "next": None}

# -----------------------------
# Timeline parse
# -----------------------------
def parse_event_datetime(date_str: str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def hash_event_uid(character_id, date_str, code, name):
    base = f"{character_id}|{date_str}|{code}|{name}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def extract_items_from_payload(payload):
    if not isinstance(payload, dict):
        return []
    timeline_obj = payload.get("timeline")
    if isinstance(timeline_obj, list):
        return timeline_obj
    if isinstance(timeline_obj, dict):
        rows = timeline_obj.get("rows") or []
        return rows if isinstance(rows, list) else []
    rows2 = payload.get("rows") or payload.get("records") or []
    return rows2 if isinstance(rows2, list) else []

def extract_next_token(payload):
    if not isinstance(payload, dict):
        return None
    nxt = payload.get("next")
    if isinstance(nxt, dict):
        return nxt.get("token") or nxt.get("next")
    return nxt

def pick_event_code(ev: dict) -> int:
    raw = ev.get("code") or ev.get("eventCode") or 0
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        s = "".join(ch for ch in raw if ch.isdigit())
        return int(s) if s else 0
    return 0

def extract_common_item_fields(ev: dict):
    def get_any(d, keys):
        for k in keys:
            v = d.get(k)
            if v is not None:
                return v
        return None

    item_id = get_any(ev, ["itemId", "item_id"])
    item_name = get_any(ev, ["itemName", "item_name"])
    item_rarity = get_any(ev, ["itemRarity", "rarity", "item_rarity"])

    channel_name = get_any(ev, ["channelName", "channel_name"])
    channel_no = get_any(ev, ["channelNo", "channel_no"])

    dungeon_name = get_any(ev, ["dungeonName", "dungeon_name"])

    mist_gear = get_any(ev, ["mistGear", "mist_gear"])
    if isinstance(mist_gear, bool):
        mist_gear = 1 if mist_gear else 0

    if channel_no is not None and not isinstance(channel_no, int):
        try:
            channel_no = int(str(channel_no))
        except Exception:
            channel_no = None

    return {
        "item_id": str(item_id) if item_id is not None else None,
        "item_name": str(item_name) if item_name is not None else None,
        "item_rarity": str(item_rarity) if item_rarity is not None else None,
        "channel_name": str(channel_name) if channel_name is not None else None,
        "channel_no": channel_no,
        "dungeon_name": str(dungeon_name) if dungeon_name is not None else None,
        "mist_gear": mist_gear if mist_gear is None or isinstance(mist_gear, int) else None,
    }

def collect_full_timeline(api_key, server_id, character_id, start_dt, end_dt):
    rows = []
    next_token = None
    date_warn_count = 0

    while True:
        payload = neople_get_timeline(api_key, server_id, character_id, start_dt, end_dt, limit=100, next_token=next_token)
        items = extract_items_from_payload(payload)

        for ev in items:
            if not isinstance(ev, dict):
                continue

            date_str = (ev.get("date") or ev.get("time") or ev.get("eventDate")
                        or ev.get("occurDate") or ev.get("createdAt"))
            dt = parse_event_datetime(date_str)
            if dt is None:
                if date_warn_count < MAX_DATE_PARSE_WARN:
                    log.warning(f"[PARSE] Unparsed date: {date_str} (server={server_id}, char={character_id})")
                    date_warn_count += 1
                continue

            event_code = pick_event_code(ev)
            event_name = str(ev.get("name") or ev.get("eventName") or "")

            rows.append({
                "event_uid": hash_event_uid(character_id, date_str or "", event_code, event_name),
                "server_id": server_id,
                "character_id": character_id,

                "character_name": None,
                "level": None,
                "job_id": None,
                "job_grow_id": None,
                "job_name": None,
                "job_grow_name": None,
                "fame": None,
                "adventure_name": None,
                "guild_id": None,
                "guild_name": None,

                "event_date": dt,
                "event_code": event_code,
                "event_name": event_name,

                **extract_common_item_fields(ev),

                "raw_json": json.dumps(ev, ensure_ascii=False),
            })

        next_token = extract_next_token(payload)
        if not next_token:
            break
        time.sleep(0.2)

    return rows

def main():
    cfg = load_cfg()
    conn = get_conn(cfg)

    ensure_table(conn)

    total = get_ranker_total_count(conn, cfg["RANKER_TABLE"])
    if total <= 0:
        log.error("ranker_info에서 랭커 목록을 얻지 못했습니다.")
        sys.exit(2)

    target_total = min(total, TOP_N) if TOP_N and TOP_N > 0 else total
    log.info(f"[DB] Total distinct rankers: {total} / Target: {target_total} / Chunk size: {CHUNK_SIZE}")
    log.info(f"[CFG] Timeline range: {START_DATE} ~ {END_DATE}")

    inserted_total = 0
    processed = 0
    offset = 0

    while processed < target_total:
        remaining = target_total - processed
        limit = CHUNK_SIZE if remaining > CHUNK_SIZE else remaining

        chunk = collect_rankers_chunk(conn, cfg["RANKER_TABLE"], limit=limit, offset=offset)
        if not chunk:
            break

        log.info(f"[DB] Processing chunk: offset={offset}, size={len(chunk)} (processed {processed}/{target_total})")

        for r in chunk:
            processed += 1
            server_id = r["serverId"]
            character_id = r["characterId"]

            # 1) 기본정보
            try:
                basic = neople_get_character_basic(cfg["NEOPLE_API_KEY"], server_id, character_id)

                character_name = basic.get("characterName") or r.get("characterName")
                level = basic.get("level")
                job_id = basic.get("jobId")
                job_grow_id = basic.get("jobGrowId")
                job_name = basic.get("jobName")
                job_grow_name = basic.get("jobGrowName")
                fame = basic.get("fame")
                adventure_name = basic.get("adventureName")
                guild_id = basic.get("guildId")
                guild_name = basic.get("guildName")

                if level is not None and not isinstance(level, int):
                    try:
                        level = int(str(level))
                    except Exception:
                        level = None
                if fame is not None and not isinstance(fame, int):
                    try:
                        fame = int(str(fame))
                    except Exception:
                        fame = None

            except NeopleSystemInspect as e:
                # 점검이면 Airflow 재시도에 맡기기 위해 실패 종료
                log.error(str(e))
                raise  # Airflow가 PythonOperator로 잡아 retry 가능

            except Exception as e:
                log.warning(f"[{processed}/{target_total}] Basic info failed for {server_id}/{character_id}: {e}")
                character_name = r.get("characterName")
                level = job_id = job_grow_id = job_name = job_grow_name = fame = adventure_name = guild_id = guild_name = None

            # 2) 타임라인
            try:
                ev_rows = collect_full_timeline(cfg["NEOPLE_API_KEY"], server_id, character_id, START_DATE, END_DATE)

                for ev in ev_rows:
                    ev["character_name"] = character_name
                    ev["level"] = level
                    ev["job_id"] = str(job_id) if job_id is not None else None
                    ev["job_grow_id"] = str(job_grow_id) if job_grow_id is not None else None
                    ev["job_name"] = job_name
                    ev["job_grow_name"] = job_grow_name
                    ev["fame"] = fame
                    ev["adventure_name"] = adventure_name
                    ev["guild_id"] = str(guild_id) if guild_id is not None else None
                    ev["guild_name"] = guild_name

                n = upsert_rows(conn, cfg["TABLE_NAME"], ev_rows)
                inserted_total += n
                log.info(f"[{processed}/{target_total}] {character_name or character_id}: inserted/upserted {n} events")

                time.sleep(0.2)

            except NeopleSystemInspect as e:
                log.error(str(e))
                raise  # Airflow retry

            except Exception as e:
                log.error(f"[{processed}/{target_total}] Timeline failed for {server_id}/{character_id}: {e}")
                continue

        offset += len(chunk)

    log.info(f"Done. Processed rankers: {processed}/{target_total}, Total upserted rows: {inserted_total}")

if __name__ == "__main__":
    main()
