import sys
import traceback
from datetime import datetime, time, timezone, timedelta
import os
import csv

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Text, TIMESTAMP,
    select, func, case, literal_column, text,
    ForeignKey, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
import boto3
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL in your .env")

engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"}, echo=False)
S3 = boto3.client("s3")

# Reflect OLAP dims & fact once
meta_olap = MetaData(schema="OLAP")
dim_time = Table("dim_time",            meta_olap, autoload_with=engine)
dim_loc  = Table("dim_location",        meta_olap, autoload_with=engine)
dim_user = Table("dim_user",            meta_olap, autoload_with=engine)
dim_svc  = Table("dim_service",         meta_olap, autoload_with=engine)
fact     = Table("fact_log_transactions", meta_olap, autoload_with=engine)

# Define & create mart tables once (Step 2)
meta_mart = MetaData(schema="OLAP")
def _common_columns():
    return [
        Column("mart_id", Integer, primary_key=True),
        Column("log_id", Integer, nullable=False),
        Column("created_at", TIMESTAMP, nullable=False),
        Column("hour", Integer, nullable=False),
        Column("day", Integer, nullable=False),
        Column("month", Integer, nullable=False),
        Column("year", Integer, nullable=False),
        Column("country", String(100), nullable=False),
        Column("region", String(100), nullable=False),
        Column("city", String(100), nullable=False),
        Column("zip_code", String(20), nullable=False),
        Column("latitude", Float, nullable=False),
        Column("longitude", Float, nullable=False),
        Column("role", String(100), nullable=False),
        Column("origin", String(100), nullable=False),
        Column("destination", String(100), nullable=False),
        Column("api_version", String(50), nullable=False),
        Column("service_type", String(50), nullable=False),
        Column("request_method", String(20)),
        Column("request_url", Text),
        Column("request_body", Text),
        Column("response_status_code", Integer),
        Column("response_body", Text),
        Column("execution_time_ms", Integer),
        Column("error_message", Text)
    ]

mart_teleo     = Table("mart_teleo",     meta_mart, *(_common_columns()))
mart_pillars   = Table("mart_pillars",   meta_mart, *(_common_columns()))
mart_campus    = Table("mart_campus",    meta_mart, *(_common_columns()))
mart_evntgarde = Table("mart_evntgarde", meta_mart, *(_common_columns()))
meta_mart.create_all(engine)

MART_TABLES = {
    'teleo': mart_teleo,
    'pillars': mart_pillars,
    'campus': mart_campus,
    'evntgarde': mart_evntgarde
}

# STEP 1 - ETL TO OLAP:
def define_tables():
    """
    Define OLTP source and OLAP target tables with SQLAlchemy Core.
    """
    # OLTP source
    meta_src = MetaData(schema="OLTP")
    src = Table("api_transactions", meta_src, autoload_with=engine)

    # OLAP target
    meta_olap = MetaData(schema="OLAP")

    dim_time = Table(
        "dim_time", meta_olap,
        Column("time_id",   Integer, primary_key=True),
        Column("hour",      Integer,   nullable=False),
        Column("day",       Integer,   nullable=False),
        Column("month",     Integer,   nullable=False),
        Column("year",      Integer,   nullable=False),
        UniqueConstraint("hour","day","month","year", name="uq_dim_time")
    )

    dim_loc = Table(
        "dim_location", meta_olap,
        Column("location_id", Integer, primary_key=True),
        Column("country",     String(100), nullable=False),
        Column("region",      String(100), nullable=False),
        Column("city",        String(100), nullable=False),
        Column("zip_code",    String(20),  nullable=False),
        Column("latitude",    Float,       nullable=False),
        Column("longitude",   Float,       nullable=False),
        UniqueConstraint(
            "country","region","city","zip_code","latitude","longitude",
            name="uq_dim_location"
        )
    )

    dim_user = Table(
        "dim_user", meta_olap,
        Column("user_id", Integer, primary_key=True),
        Column("role",    String(100), nullable=False),
        Column("origin",  String(100), nullable=False),
        UniqueConstraint("role","origin", name="uq_dim_user")
    )

    dim_svc = Table(
        "dim_service", meta_olap,
        Column("service_id",   Integer, primary_key=True),
        Column("destination",  String(100), nullable=False),
        Column("api_version",  String(50),  nullable=False),
        Column("service_type", String(50),  nullable=False),
        UniqueConstraint(
            "destination","api_version","service_type",
            name="uq_dim_service"
        )
    )

    fact = Table(
        "fact_log_transactions", meta_olap,
        Column("log_id",               Integer, primary_key=True),
        Column("time_id",              Integer, ForeignKey("OLAP.dim_time.time_id"), nullable=False),
        Column("location_id",          Integer, ForeignKey("OLAP.dim_location.location_id"), nullable=False),
        Column("user_id",              Integer, ForeignKey("OLAP.dim_user.user_id"), nullable=False),
        Column("service_id",           Integer, ForeignKey("OLAP.dim_service.service_id"), nullable=False),
        Column("created_at",           TIMESTAMP, nullable=False),
        Column("request_method",       String(20)),
        Column("request_url",          Text),
        Column("request_headers",      Text),
        Column("request_body",         Text),
        Column("response_status_code", Integer),
        Column("response_body",        Text),
        Column("execution_time_ms",    Integer),
        Column("error_message",        Text),
    )

    # Create all OLAP tables (if they don't already exist)
    meta_olap.create_all(engine)
    return src, dim_time, dim_loc, dim_user, dim_svc, fact

def etl(date_str):
    """
    Perform the ETL for a single date:
     1) Upsert dims (time, location, user, service)
     2) Insert fact rows, skipping duplicates
    Only rows with created_at between [date_str 00:00, date_str+1 00:00) are processed.
    """
    # parse the input date and build our window
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date format: '{date_str}', expected YYYY-MM-DD")

    start_dt = datetime.combine(dt, time.min)
    end_dt   = start_dt + timedelta(days=1)
    print(f"[etl] Filtering transactions from {start_dt} to {end_dt} (exclusive)")

    src, dim_time, dim_loc, dim_user, dim_svc, fact = define_tables()

    date_filter = (src.c.created_at >= start_dt) & (src.c.created_at < end_dt)

    # 1) Upsert dim_time
    time_stmt = pg_insert(dim_time).from_select(
        ["hour","day","month","year"],
        select(
            func.date_part("hour",  src.c.created_at).cast(Integer).label("hour"),
            func.date_part("day",   src.c.created_at).cast(Integer).label("day"),
            func.date_part("month", src.c.created_at).cast(Integer).label("month"),
            func.date_part("year",  src.c.created_at).cast(Integer).label("year"),
        )
        .where(date_filter)
        .distinct()
    ).on_conflict_do_nothing(constraint="uq_dim_time")

    # 2) Upsert dim_location
    loc_stmt = pg_insert(dim_loc).from_select(
        ["country","region","city","zip_code","latitude","longitude"],
        select(
            src.c.country,
            src.c.region,
            src.c.city,
            src.c.zip_code,
            src.c.latitude,
            src.c.longitude
        )
        .where(date_filter)
        .distinct()
    ).on_conflict_do_nothing(constraint="uq_dim_location")

    # 3) Normalize origin + validate role
    origin_norm = case(
        (src.c.origin.is_(None), literal_column("'Unknown'")),
        else_=func.initcap(src.c.origin)
    ).label("origin")
    
    role_valid = case(
        ((src.c.origin.ilike("teleo"))     & src.c.role.in_(["Normal_User","Guest","Church_Admin","Pastor"]), src.c.role),
        ((src.c.origin.ilike("campus"))    & src.c.role.in_(["Student","Professor","Admin"]),                 src.c.role),
        ((src.c.origin.ilike("evntgarde")) & src.c.role.in_(["Customer","Organizer","Vendor"]),             src.c.role),
        ((src.c.origin.ilike("pillars"))   & src.c.role.in_(["Employer","Dean","Professor","Student"]),     src.c.role),
        else_=literal_column("'Unknown'")
    ).label("role")

    # 4) Upsert dim_user
    user_stmt = pg_insert(dim_user).from_select(
        ["role","origin"],
        select(role_valid, origin_norm)
        .where(date_filter)
        .distinct()
    ).on_conflict_do_nothing(constraint="uq_dim_user")

    # 5) Upsert dim_service
    svc_type = case(
        (src.c.destination.in_(["Pillars","Evntgarde","Teleo","Campus"]), literal_column("'System-to-System'")),
        else_=literal_column("'3rd-Party'")
    ).label("service_type")

    svc_stmt = pg_insert(dim_svc).from_select(
        ["destination","api_version","service_type"],
        select(src.c.destination, src.c.api_version, svc_type)
        .where(date_filter)
        .distinct()
    ).on_conflict_do_nothing(constraint="uq_dim_service")

    # 6) Build and execute fact INSERT…SELECT
    hour_part  = func.date_part("hour",  src.c.created_at).cast(Integer)
    day_part   = func.date_part("day",   src.c.created_at).cast(Integer)
    month_part = func.date_part("month", src.c.created_at).cast(Integer)
    year_part  = func.date_part("year",  src.c.created_at).cast(Integer)

    fact_select = (
        select(
            src.c.log_id,
            dim_time.c.time_id,
            dim_loc.c.location_id,
            dim_user.c.user_id,
            dim_svc.c.service_id,
            src.c.created_at,
            src.c.request_method,
            src.c.request_url,
            src.c.request_headers,
            src.c.request_body,
            src.c.response_status_code,
            src.c.response_body,
            src.c.execution_time_ms,
            src.c.error_message
        )
        .select_from(
            src
            .join(
                dim_time,
                (hour_part  == dim_time.c.hour)  &
                (day_part   == dim_time.c.day)   &
                (month_part == dim_time.c.month) &
                (year_part  == dim_time.c.year)
            )
            .join(
                dim_loc,
                (src.c.country   == dim_loc.c.country)  &
                (src.c.region    == dim_loc.c.region)   &
                (src.c.city      == dim_loc.c.city)     &
                (src.c.zip_code  == dim_loc.c.zip_code) &
                (src.c.latitude  == dim_loc.c.latitude) &
                (src.c.longitude == dim_loc.c.longitude)
            )
            .join(
                dim_user,
                (role_valid  == dim_user.c.role) &
                (origin_norm == dim_user.c.origin)
            )
            .join(
                dim_svc,
                (src.c.destination == dim_svc.c.destination) &
                (src.c.api_version  == dim_svc.c.api_version) &
                (svc_type           == dim_svc.c.service_type)
            )
        )
        .where(date_filter)
    )

    fact_stmt = pg_insert(fact).from_select(
        [
            "log_id", "time_id", "location_id", "user_id", "service_id", "created_at",
            "request_method", "request_url", "request_headers", "request_body",
            "response_status_code", "response_body", "execution_time_ms", "error_message"
        ],
        fact_select
    ).on_conflict_do_nothing(index_elements=["log_id"])


    with engine.begin() as conn:
        conn.execute(time_stmt)
        conn.execute(loc_stmt)
        conn.execute(user_stmt)
        conn.execute(svc_stmt)
        inserted = conn.execute(fact_stmt).rowcount
        print(f"[etl] inserted {inserted} fact rows (duplicates skipped)")

#STEP 2 - CREATE DATA MARTS:
def _load_one_mart(date_str: str, tenant: str) -> int:
    tenant_key = tenant.lower()
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    start_dt = datetime.combine(dt, time.min)
    end_dt = start_dt + timedelta(days=1)

    sql = text(f"""
    WITH deleted AS (
      DELETE FROM "OLAP"."mart_{tenant_key}"
       WHERE created_at >= :start_dt
         AND created_at <  :end_dt
    )
    INSERT INTO "OLAP"."mart_{tenant_key}" (
      log_id, created_at, hour, day, month, year,
      country, region, city, zip_code, latitude, longitude,
      role, origin, destination, api_version, service_type,
      request_method, request_url, request_body,
      response_status_code, response_body, execution_time_ms, error_message
    )
    SELECT
      f.log_id, f.created_at,
      t.hour, t.day, t.month, t.year,
      l.country, l.region, l.city, l.zip_code, l.latitude, l.longitude,
      u.role, u.origin, s.destination, s.api_version, s.service_type,
      f.request_method, f.request_url, f.request_body,
      f.response_status_code, f.response_body, f.execution_time_ms, f.error_message
    FROM "OLAP"."fact_log_transactions" f
      JOIN "OLAP"."dim_time"     t ON f.time_id     = t.time_id
      JOIN "OLAP"."dim_location" l ON f.location_id = l.location_id
      JOIN "OLAP"."dim_user"     u ON f.user_id     = u.user_id
      JOIN "OLAP"."dim_service"  s ON f.service_id  = s.service_id
    WHERE f.created_at >= :start_dt
      AND f.created_at <  :end_dt
      AND (LOWER(u.origin) = :tenant_key OR LOWER(s.destination) = :tenant_key)
    """).bindparams(start_dt=start_dt, end_dt=end_dt, tenant_key=tenant_key)

    with engine.begin() as conn:
        return conn.execute(sql).rowcount

def create_data_marts(date_str: str):
    def _task(t):
        cnt = _load_one_mart(date_str, t)
        print(f"[mart] {t}: inserted {cnt}")
    with ThreadPoolExecutor(max_workers=4) as ex:
        ex.map(_task, MART_TABLES)

#MAIN ENTRY
def main(date_override: str = None):
    date_str = date_override or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[MAIN] pipeline for {date_str}")
    etl(date_str)
    create_data_marts(date_str)
    print("[MAIN] done")

def ETL_Handler(event, context):
    """
    AWS Lambda handler for Integreat ETL pipeline.
    Accepts optional 'date_str' in the event to override the date.
    """
    date_str = event.get('date_str') if event else None
    main(date_str)

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        main(arg)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
