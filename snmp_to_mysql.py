"""
Скрипт для Туризм РФ / сбор счетчиков и уровней тонеров по SNMP
Инструкция, последовательно выполнить в терминале:

sudo apt update
sudo apt install -y python3 python3-venv python3-pip wget
sudo mkdir -p /opt/snmp_to_mysql
sudo chown $USER:$USER /opt/snmp_to_mysql
cd /opt/snmp_to_mysql

python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install "pysnmp==7.1.21" "mysql-connector-python==9.4.0" "pandas==2.3.3" "sqlalchemy==2.0.43" "pymysql==1.1.2"

wget -qO snmp_to_mysql.py https://raw.githubusercontent.com/ViktorShch/turism/refs/heads/main/snmp_to_mysql.py
chmod +x snmp_to_mysql.py

CRON_CMD="/opt/snmp_to_mysql/venv/bin/python /opt/snmp_to_mysql/snmp_to_mysql.py"
( crontab -l 2>/dev/null | grep -Fv "$CRON_CMD" ; echo "20,50 * * * * $CRON_CMD" ) | crontab -
"""


import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import sqlalchemy
from pysnmp.hlapi.asyncio import (CommunityData, ContextData, ObjectIdentity,
                                  ObjectType, SnmpEngine, UdpTransportTarget,
                                  get_cmd)

# ---------- Конфиг и логирование ----------
LOG = logging.getLogger(__name__)
root = logging.getLogger()
if not root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HOST = "95.165.168.239"
DB_NAME = "copier_contracts"
USERNAME = "turizm_user"
PASSWORD = "Strong*Turizm*Password"

ENGINE = sqlalchemy.create_engine(
    f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}",
    pool_pre_ping=True,
    pool_recycle=600,
    pool_size=5,
    max_overflow=10,
)


def append_sql(df: pd.DataFrame, name: str) -> None:
    """Записать DataFrame в таблицу (append)."""
    if df.empty:
        LOG.info("No rows to append to %s", name)
        return
    with ENGINE.begin() as conn:
        df.to_sql(name=name, con=conn, if_exists="append", index=False)


def read_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    """Прочитать SQL-запрос в DataFrame."""
    with ENGINE.connect() as conn:
        return pd.read_sql_query(query, conn, params=params)


def _to_int(v: Any) -> Optional[int]:
    """Безопасно привести значение к int, вернуть None при ошибке/пустом."""
    try:
        if v is None:
            return None
        return int(float(v))
    except Exception:
        return None


def _safe_percent(current: Any, maximum: Any) -> Optional[int]:
    """Вернуть процент заполнения (0..100) или None."""
    cur = _to_int(current)
    mx = _to_int(maximum)
    if cur is None or mx in (None, 0):
        return None
    return int(round(100.0 * cur / mx))


def _safe_sum(simplex: Any, duplex: Any, duplex_weight: int = 2) -> Optional[int]:
    """Простая сумма: simplex + duplex_weight * duplex."""
    s = _to_int(simplex)
    d = _to_int(duplex)
    if s is None and d is None:
        return None
    s = s or 0
    d = d or 0
    return s + duplex_weight * d


async def snmp_get_bulk_async(
    ip: str,
    oids: List[str],
    community: str = "public",
    port: int = 161,
    snmp_version: int = 1,
    timeout: int = 3,
    retries: int = 1,
) -> Dict[str, Optional[str]]:
    """
    Выполнить пакетный SNMP GET для списка OID.
    Возвращает словарь {oid_str: value_str | None}.
    """
    results: Dict[str, Optional[str]] = {}
    snmp_engine = SnmpEngine()
    varbinds = [ObjectType(ObjectIdentity(oid)) for oid in oids]

    try:
        transport_target = await UdpTransportTarget.create(
            (ip, port), timeout=timeout, retries=retries
        )
        iterator = get_cmd(
            snmp_engine,
            CommunityData(community, mpModel=snmp_version),
            transport_target,
            ContextData(),
            *varbinds,
        )

        errorIndication, errorStatus, errorIndex, varBinds = await iterator

        if errorIndication:
            LOG.warning("SNMP %s error: %s", ip, errorIndication)
            return results
        if errorStatus:
            LOG.warning("SNMP %s errorStatus: %s", ip, errorStatus.prettyPrint())
            return results
        LOG.warning("SNMP %s done!", ip)

        for oid_obj, val in varBinds:
            results[str(oid_obj)] = val.prettyPrint()
    except Exception:
        LOG.exception("SNMP exception while polling %s", ip)
    finally:
        try:
            snmp_engine.close_dispatcher()
        except Exception:
            pass

    return results


RICOH_A3_OIDS = {
    "model": "1.3.6.1.2.1.43.5.1.1.16.1",
    "serial": "1.3.6.1.2.1.43.5.1.1.17.1",
    "a4_mono_simplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.292",
    "a3_mono_simplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.293",
    "a4_color_simplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.294",
    "a3_color_simplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.295",
    "a4_mono_duplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.296",
    "a3_mono_duplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.297",
    "a4_color_duplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.298",
    "a3_color_duplex": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.299",
    "scan": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.286",
    "cur_k": "1.3.6.1.2.1.43.11.1.1.9.1.1",
    "cur_c": "1.3.6.1.2.1.43.11.1.1.9.1.3",
    "cur_m": "1.3.6.1.2.1.43.11.1.1.9.1.4",
    "cur_y": "1.3.6.1.2.1.43.11.1.1.9.1.5",
    "max_k": "1.3.6.1.2.1.43.11.1.1.8.1.1",
    "max_c": "1.3.6.1.2.1.43.11.1.1.8.1.3",
    "max_m": "1.3.6.1.2.1.43.11.1.1.8.1.4",
    "max_y": "1.3.6.1.2.1.43.11.1.1.8.1.5",
}


def _proc_ricoh_a3(data: Dict[str, Any]) -> Dict[str, Any]:
    """Постобработка для Ricoh A3."""
    # helper to get by friendly name -> oid value
    get = lambda key: data.get(RICOH_A3_OIDS[key])
    return {
        "date": datetime.now(),
        "serial": get("serial"),
        "model": get("model"),
        "c": _safe_percent(get("cur_c"), get("max_c")),
        "m": _safe_percent(get("cur_m"), get("max_m")),
        "y": _safe_percent(get("cur_y"), get("max_y")),
        "k": _safe_percent(get("cur_k"), get("max_k")),
        "a4_mono": _safe_sum(get("a4_mono_simplex"), get("a4_mono_duplex")),
        "a3_mono": _safe_sum(get("a3_mono_simplex"), get("a3_mono_duplex")),
        "a4_color": _safe_sum(get("a4_color_simplex"), get("a4_color_duplex")),
        "a3_color": _safe_sum(get("a3_color_simplex"), get("a3_color_duplex")),
        "scan_total": _to_int(get("scan"))
    }


KYO_A3_OIDS = {
    "model": "1.3.6.1.2.1.43.5.1.1.16.1",
    "serial": "1.3.6.1.2.1.43.5.1.1.17.1",
    "scan_total": "1.3.6.1.4.1.1347.46.10.1.1.5.3",
    "a3_color": "1.3.6.1.4.1.1347.42.2.1.1.1.8.1.1",
    "a3_one": "1.3.6.1.4.1.1347.42.2.1.1.1.9.1.1",
    "a3_mono": "1.3.6.1.4.1.1347.42.2.1.1.1.7.1.1",
    "a4_color": "1.3.6.1.4.1.1347.42.2.1.1.1.8.1.3",
    "a4_one": "1.3.6.1.4.1.1347.42.2.1.1.1.9.1.3",
    "a4_mono": "1.3.6.1.4.1.1347.42.2.1.1.1.7.1.3",
    "cur_k": "1.3.6.1.2.1.43.11.1.1.9.1.4",
    "cur_c": "1.3.6.1.2.1.43.11.1.1.9.1.1",
    "cur_m": "1.3.6.1.2.1.43.11.1.1.9.1.2",
    "cur_y": "1.3.6.1.2.1.43.11.1.1.9.1.3",
    "max_k": "1.3.6.1.2.1.43.11.1.1.8.1.4",
    "max_c": "1.3.6.1.2.1.43.11.1.1.8.1.1",
    "max_m": "1.3.6.1.2.1.43.11.1.1.8.1.2",
    "max_y": "1.3.6.1.2.1.43.11.1.1.8.1.3",
}


def _proc_kyo_a3(data: Dict[str, Any]) -> Dict[str, Any]:
    g = lambda k: data.get(KYO_A3_OIDS[k])
    a4_mono = _to_int(g("a4_mono"))
    # a3_mono = (
    #     (_to_int(g("a3_mono")) // 2) if _to_int(g("a3_mono")) is not None else None
    # )
    a3_mono = _to_int(g("a3_mono")) if _to_int(g("a3_mono")) is not None else None
    a4_color = (_to_int(g("a4_color")) or 0) + (_to_int(g("a4_one")) or 0)
    # a3_color = (
    #     ((_to_int(g("a3_color")) or 0) + (_to_int(g("a3_one")) or 0)) // 2
    #     if _to_int(g("a3_color")) is not None
    #     else None
    # )
    a3_color = (
        (_to_int(g("a3_color")) or 0) + (_to_int(g("a3_one")) or 0)
        if _to_int(g("a3_color")) is not None
        else None
    )

    return {
        "date": datetime.now(),
        "serial": g("serial"),
        "model": g("model"),
        "c": _safe_percent(g("cur_c"), g("max_c")),
        "m": _safe_percent(g("cur_m"), g("max_m")),
        "y": _safe_percent(g("cur_y"), g("max_y")),
        "k": _safe_percent(g("cur_k"), g("max_k")),
        "a4_color": a4_color,
        "a4_mono": a4_mono,
        "a3_color": a3_color,
        "a3_mono": a3_mono,
        "scan_total": _to_int(g("scan_total")),
    }


KYO_A4_OIDS = {
    "model": "1.3.6.1.2.1.43.5.1.1.16.1",
    "serial": "1.3.6.1.2.1.43.5.1.1.17.1",
    "scan_total": "1.3.6.1.4.1.1347.46.10.1.1.5.3",
    "a4_color": "1.3.6.1.4.1.1347.42.2.1.1.1.8.1.1",
    "a4_one": "1.3.6.1.4.1.1347.42.2.1.1.1.9.1.1",
    "a4_mono": "1.3.6.1.4.1.1347.42.2.1.1.1.7.1.1",
    "cur_k": "1.3.6.1.2.1.43.11.1.1.9.1.4",
    "cur_c": "1.3.6.1.2.1.43.11.1.1.9.1.1",
    "cur_m": "1.3.6.1.2.1.43.11.1.1.9.1.2",
    "cur_y": "1.3.6.1.2.1.43.11.1.1.9.1.3",
    "max_k": "1.3.6.1.2.1.43.11.1.1.8.1.4",
    "max_c": "1.3.6.1.2.1.43.11.1.1.8.1.1",
    "max_m": "1.3.6.1.2.1.43.11.1.1.8.1.2",
    "max_y": "1.3.6.1.2.1.43.11.1.1.8.1.3",
}


def _proc_kyo_a4(data: Dict[str, Any]) -> Dict[str, Any]:
    g = lambda k: data.get(KYO_A4_OIDS[k])
    a4_mono = _to_int(g("a4_mono"))
    a4_color = (_to_int(g("a4_color")) or 0) + (_to_int(g("a4_one")) or 0)
    return {
        "date": datetime.now(),
        "serial": g("serial"),
        "model": g("model"),
        "c": _safe_percent(g("cur_c"), g("max_c")),
        "m": _safe_percent(g("cur_m"), g("max_m")),
        "y": _safe_percent(g("cur_y"), g("max_y")),
        "k": _safe_percent(g("cur_k"), g("max_k")),
        "a4_color": a4_color,
        "a4_mono": a4_mono,
        "a3_color": None,
        "a3_mono": None,
        "scan_total": _to_int(g("scan_total")),
    }


RICOH_A4_OIDS = {
    "model": "1.3.6.1.2.1.43.5.1.1.16.1",
    "serial": "1.3.6.1.2.1.43.5.1.1.17.1",
    "a4_color": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.21",
    "a4_mono": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.22",
    "scan_color": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.71",
    "scan_mono": "1.3.6.1.4.1.367.3.2.1.2.19.5.1.9.72",
    "cur_k": "1.3.6.1.2.1.43.11.1.1.9.1.1",
    "cur_c": "1.3.6.1.2.1.43.11.1.1.9.1.3",
    "cur_m": "1.3.6.1.2.1.43.11.1.1.9.1.4",
    "cur_y": "1.3.6.1.2.1.43.11.1.1.9.1.5",
    "max_k": "1.3.6.1.2.1.43.11.1.1.8.1.1",
    "max_c": "1.3.6.1.2.1.43.11.1.1.8.1.3",
    "max_m": "1.3.6.1.2.1.43.11.1.1.8.1.4",
    "max_y": "1.3.6.1.2.1.43.11.1.1.8.1.5",
}


def _proc_ricoh_a4(data: Dict[str, Any]) -> Dict[str, Any]:
    g = lambda k: data.get(RICOH_A4_OIDS[k])
    scan_total = (_to_int(g("scan_color")) or 0) + (_to_int(g("scan_mono")) or 0)
    return {
        "date": datetime.now(),
        "serial": g("serial"),
        "model": g("model"),
        "c": _safe_percent(g("cur_c"), g("max_c")),
        "m": _safe_percent(g("cur_m"), g("max_m")),
        "y": _safe_percent(g("cur_y"), g("max_y")),
        "k": _safe_percent(g("cur_k"), g("max_k")),
        "a4_mono": _to_int(g("a4_mono")),
        "a3_mono": None,
        "a4_color": _to_int(g("a4_color")),
        "a3_color": None,
        "scan_total": scan_total,
    }


DEVICE_PROFILES = {
    "TASKalfa 7054ci": {"oids": KYO_A3_OIDS, "processor": _proc_kyo_a3},
    "RICOH IM C4500": {"oids": RICOH_A3_OIDS, "processor": _proc_ricoh_a3},
    "ECOSYS MA3500cix": {"oids": KYO_A4_OIDS, "processor": _proc_kyo_a4},
    "RICOH SP C360SFNw": {"oids": RICOH_A4_OIDS, "processor": _proc_ricoh_a4},
    "RICOH SP C360SNw": {"oids": RICOH_A4_OIDS, "processor": _proc_ricoh_a4},
}


async def poll_device_async(ip: str, profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Опрашивает устройство по профилю и возвращает результат в нужном формате или None."""
    oids_list = list(profile["oids"].values())
    raw = await snmp_get_bulk_async(ip, oids_list)
    result = profile["processor"](raw)
    result["ip"] = ip

    if not (result.get("serial") or result.get("model")):
        return None

    return result


async def main_async(concurrency: int = 30):
    """Асинхронная главная функция — читает устройства, опрашивает параллельно и сохраняет в БД."""
    devices = read_sql("SELECT ip, model FROM tur_ip")
    if devices.empty:
        LOG.info("No devices found in DB")
        return

    sem = asyncio.Semaphore(concurrency)
    tasks = []

    async def _worker(ip: str, model: str):
        profile = DEVICE_PROFILES.get(model)
        if not profile:
            LOG.warning("No profile for model %s (ip=%s)", model, ip)
            return None
        async with sem:
            try:
                return await poll_device_async(ip, profile)
            except Exception:
                LOG.exception("Failed polling %s (%s)", ip, model)
                return None

    for row in devices.itertuples(index=False):
        tasks.append(_worker(row.ip, row.model))

    results = [r for r in await asyncio.gather(*tasks) if r]
    if not results:
        LOG.info("No successful SNMP results")
        return

    df = pd.DataFrame(results)
    if "ip" in df.columns:
        df = df.drop_duplicates(subset=["ip"], keep="last").reset_index(drop=True)

    append_sql(df, name="tur_data")
    LOG.info("Appended %d rows to tur_data", len(df))


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
