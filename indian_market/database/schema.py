from __future__ import annotations

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)

metadata = MetaData()

security_master = Table(
    "security_master",
    metadata,
    Column("security_id", String(36), primary_key=True),
    Column("exchange", String(12), nullable=False),
    Column("isin", String(12)),
    Column("symbol", String(64), nullable=False),
    Column("series", String(24), nullable=False),
    Column("name", String(256)),
    Column("effective_from", Date, nullable=False),
    Column("effective_to", Date),
    Column("active", Boolean, nullable=False, server_default=text("1")),
    Column("source", String(64), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "effective_to IS NULL OR effective_to >= effective_from",
        name="ck_security_mapping_effective_dates",
    ),
    UniqueConstraint(
        "exchange", "symbol", "series", "effective_from",
        name="uq_security_mapping_effective",
    ),
)
Index("ix_security_master_isin", security_master.c.isin)
Index(
    "ix_security_master_lookup",
    security_master.c.exchange,
    security_master.c.symbol,
    security_master.c.series,
    security_master.c.effective_from,
    security_master.c.effective_to,
)

daily_prices = Table(
    "daily_prices",
    metadata,
    Column("price_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "security_id",
        String(36),
        ForeignKey("security_master.security_id"),
        nullable=False,
    ),
    Column("trade_date", Date, nullable=False),
    Column("open", Numeric(18, 6), nullable=False),
    Column("high", Numeric(18, 6), nullable=False),
    Column("low", Numeric(18, 6), nullable=False),
    Column("close", Numeric(18, 6), nullable=False),
    Column("volume", Numeric(24, 4)),
    Column("traded_value", Numeric(24, 4)),
    Column("source", String(64), nullable=False),
    Column("source_file", Text, nullable=False),
    Column("source_as_of", DateTime(timezone=True), nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("security_id", "trade_date", name="uq_daily_price_security_date"),
    CheckConstraint("open > 0 AND high > 0 AND low > 0 AND close > 0", name="ck_daily_price_positive"),
    CheckConstraint("low <= high AND open >= low AND open <= high AND close >= low AND close <= high", name="ck_daily_price_ohlc"),
    CheckConstraint("volume IS NULL OR volume >= 0", name="ck_daily_price_volume"),
    CheckConstraint("traded_value IS NULL OR traded_value >= 0", name="ck_daily_price_traded_value"),
)
Index("ix_daily_prices_date_security", daily_prices.c.trade_date, daily_prices.c.security_id)

index_master = Table(
    "index_master",
    metadata,
    Column("index_id", String(64), primary_key=True),
    Column("exchange", String(12), nullable=False),
    Column("name", String(128), nullable=False),
    Column("source_code", String(128)),
    Column("source", String(64), nullable=False),
)

index_prices = Table(
    "index_prices",
    metadata,
    Column("index_price_id", Integer, primary_key=True, autoincrement=True),
    Column("index_id", String(64), ForeignKey("index_master.index_id"), nullable=False),
    Column("trade_date", Date, nullable=False),
    Column("open", Numeric(18, 6)),
    Column("high", Numeric(18, 6)),
    Column("low", Numeric(18, 6)),
    Column("close", Numeric(18, 6), nullable=False),
    Column("source", String(64), nullable=False),
    Column("source_file", Text, nullable=False),
    Column("source_as_of", DateTime(timezone=True), nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("index_id", "trade_date", name="uq_index_price_index_date"),
    CheckConstraint("close > 0", name="ck_index_price_close_positive"),
    CheckConstraint(
        "open IS NULL OR open > 0", name="ck_index_price_open_positive"
    ),
    CheckConstraint(
        "high IS NULL OR high > 0", name="ck_index_price_high_positive"
    ),
    CheckConstraint("low IS NULL OR low > 0", name="ck_index_price_low_positive"),
)
Index("ix_index_prices_date_index", index_prices.c.trade_date, index_prices.c.index_id)

mutual_fund_schemes = Table(
    "mutual_fund_schemes",
    metadata,
    Column("scheme_code", Integer, primary_key=True),
    Column("scheme_name", Text, nullable=False),
    Column("fund_house", String(256)),
    Column("scheme_type", String(128)),
    Column("scheme_category", String(256)),
    Column("isin_growth", String(32)),
    Column("isin_div_reinvestment", String(32)),
    Column("source", String(64), nullable=False),
    Column("retrieved_at", DateTime(timezone=True), nullable=False),
)

mutual_fund_nav = Table(
    "mutual_fund_nav",
    metadata,
    Column("nav_id", Integer, primary_key=True, autoincrement=True),
    Column(
        "scheme_code",
        Integer,
        ForeignKey("mutual_fund_schemes.scheme_code"),
        nullable=False,
    ),
    Column("nav_date", Date, nullable=False),
    Column("nav", Numeric(18, 6), nullable=False),
    Column("source", String(64), nullable=False),
    Column("source_file", Text, nullable=False),
    Column("source_as_of", DateTime(timezone=True), nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("scheme_code", "nav_date", name="uq_mf_nav_scheme_date"),
    CheckConstraint("nav > 0", name="ck_mf_nav_positive"),
)
Index("ix_mutual_fund_nav_date_scheme", mutual_fund_nav.c.nav_date, mutual_fund_nav.c.scheme_code)

data_ingestion_log = Table(
    "data_ingestion_log",
    metadata,
    Column("run_id", String(36), primary_key=True),
    Column("source", String(64), nullable=False),
    Column("dataset", String(64), nullable=False),
    Column("requested_as_of", String(64)),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(16), nullable=False),
    Column("counts", JSON, nullable=False),
    Column("raw_file", Text),
    Column("checksum_sha256", String(64)),
    Column("error_summary", Text),
    CheckConstraint(
        "status IN ('started', 'succeeded', 'partial', 'failed')",
        name="ck_ingestion_log_status",
    ),
)
Index("ix_ingestion_log_source_started", data_ingestion_log.c.source, data_ingestion_log.c.started_at)

ingestion_issues = Table(
    "ingestion_issues",
    metadata,
    Column("issue_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), ForeignKey("data_ingestion_log.run_id"), nullable=False),
    Column("row_number", Integer),
    Column("issue_type", String(64), nullable=False),
    Column("record_key", String(256)),
    Column("details", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_ingestion_issues_run", ingestion_issues.c.run_id)
