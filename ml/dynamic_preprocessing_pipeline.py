"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       DYNAMIC PREPROCESSING PIPELINE v2.9 — Production-Grade SaaS Engine  ║
║       Industry-agnostic, B2B multi-tenant hardened, integrity-first        ║
╚══════════════════════════════════════════════════════════════════════════════╝

HOW TO USE:
    from dynamic_preprocessing_pipeline import DynamicPreprocessingPipeline

    # Without train/test split (backward-compatible):
    pipeline = DynamicPreprocessingPipeline()
    cleaned_df, report = pipeline.run(
        source  = "your_dataset.csv",
        db_url  = "postgresql://user:password@localhost:5432/optimaai_db",
        user_id = 1
    )

    # With train/test split (recommended for ML):
    cleaned_df, report = pipeline.run(
        source     = "your_dataset.csv",
        fit_on     = 0.8,           # first 80% of rows used for statistics (C-01)
        target_col = "next_sales",  # excluded from imputation + discovery (C-02)
    )

    # IMPORTANT — Thread Safety (H-01):
    # DynamicPreprocessingPipeline is NOT safe to share across concurrent requests.
    # Always instantiate one pipeline per request in multi-tenant deployments.
    #
    # FastAPI example:
    #   @app.post("/process")
    #   async def process(file: UploadFile, db: Session = Depends(get_db)):
    #       pipeline = DynamicPreprocessingPipeline()   # fresh per request
    #       return pipeline.run(source=await file.read(), ...)

────────────────────────────────────────────────────────────────────────────────
 CHANGELOG
────────────────────────────────────────────────────────────────────────────────
 Sessions 1-8  — V2.2   (Fixes 1-31, see archived version)
 Session 9     — V2.3   (Fixes 32-46, Audit Bugs 1-15)
 Session 11    — V2.5   (Fixes 47-53)
 Session 12    — V2.6   Full-audit patch (Fixes 54-64)
 Session 13    — V2.7   Pipeline-catalog patch (column_catalog support)
 Session 14    — V2.8   Edge-case & data-quality patch (Fixes 65-72)
 Session 15    — V2.9   Bug-fix patch (B-01 to B-07)

   Session 15  — V2.9   BUG-FIX PATCH

   Fix B-01: MONETARY IMPUTATION TRACKING WAS DISHONEST
            Step 4's monetary branch (Fix 72) correctly preserved NaN values
            on financial columns, but then fell through to the shared tracking
            block at the bottom of the loop.  As a result:
              • _track_imputation() marked rows as "imputed" though no fillna
                ran, inflating the "Total Imputed Cell-Rows Tracked" KPI.
              • report["missing_filled"][col] was populated with a strategy
                of "NaN preserved (monetary)" — conflating "filled" with
                "logged".
              • The SUCCESS log read "filled with NaN preserved" — a literal
                contradiction.
            Fix: monetary nulls now write to a new report["missing_preserved"]
            bucket and `continue` past the tracking block.  The
            "imputed cell-rows" KPI now reflects only true imputations.

   Fix B-02: STEP 3.9.1 (TOTAL SALES) MATCHED EXACT NAMES ONLY
            The detector required column names equal to "sales", "sale",
            "revenue", or "مبيعات" after lower+underscore normalisation.
            Realistic Odoo-derived columns like Total_Sales, Sales_Amount,
            Net_Revenue, Revenue_EGP all silently failed and the step was a
            no-op on real data.
            Fix: anchored substring matching ("sales" matches both "Sales"
            and "Total_Sales"), with explicit ID-keyword exclusion so a
            "sales_id" column is never picked.  The two source columns are
            also guaranteed distinct from each other and from the "Total
            Sales" target column.

   Fix B-03: TEMPORAL LOGIC (FIX 68) USED UNANCHORED SUBSTRING MATCH
            START_KW={"start","begin","from","open","issue",...} and
            END_KW={"end","close","to","expire","finish",...} were tested
            with `kw in name`, so "to" matched tomorrow_dt, "from" matched
            frame_dt, "end" matched weekend_dt.  False positives could NaT
            unrelated date pairs whose values happened to satisfy A > B.
            Fix: anchored matching helper (kw == name | name.endswith(_kw)
            | name.startswith(kw_) | _kw_ inside name) shared by both
            START_KW and END_KW so only genuinely-named start/end pairs
            participate in the inversion check.

   Fix B-04: TOTAL SALES COMPUTED TOO EARLY IN THE PIPELINE
            _step_total_sales ran at Step 3.9.1, BEFORE Step 4 imputation
            and Step 4.8 constraint sync.  Quantity nulls (filled by median
            in Step 4) and any relationally-recoverable nulls (Step 4.8) had
            no chance to contribute, so Total Sales rows whose Quantity was
            NaN at load time stayed NaN forever even after the source nulls
            were resolved.
            Fix: step renumbered to 4.9 and moved AFTER 4.8 in run() so
            it benefits from every prior null-recovery pass.  Sales is
            still preserved as monetary NaN (Fix 72), so any Total Sales
            row whose Sales is NaN remains NaN — that is intentional, the
            caller must inspect manually rather than have a fabricated
            value injected.

   Fix B-05: SCATTERED print() CALLS BYPASSED THE LOGGER
            Session 14 migrated log() onto stdlib logging so handlers,
            levels, and formatters could be controlled by the host
            application (FastAPI / CloudWatch / Datadog).  Dozens of bare
            print() calls across the step methods were missed and continued
            writing directly to stdout, ignoring the configured log level
            and breaking structured logging in containerised deployments.
            Fix: every step-internal print() replaced with _logger.info(...)
            so log output is uniform.  CLI banner colour print()s in the
            __main__ block remain untouched — those run only in interactive
            CLI mode, never in library use.

   Fix B-06: REDUNDANT TEXT NORMALISATION IN STEP 3.2
            _step_categorical_standardization re-applied .astype(str)
            .str.strip().str.lower() on every value, even though Step 3
            had already normalised non-case-sensitive text columns. On a
            500K-row dataset that is ~3M wasted Python operations.
            Fix: replaced with a typed `_norm` helper that maps strings
            via the synonym table and passes NaN/None through unchanged.
            No more "nan"-string round-trips, no double work.

   Fix B-07: COLUMN_CATALOG TABLE MISSING ON UPGRADED DEPLOYMENTS
            Deployments whose database was initialised under a pre-V2.7
            schema have `users` and `uploads` but lack `column_catalog`.
            The first upload after the pipeline upgrade hit
            psycopg2.errors.UndefinedTable mid-flight, AFTER the cleaned
            data table was already written — leaving an orphan upload row
            with no catalog entries and a 500 response on a successful
            cleaning operation.
            Fix: DatabaseManager.flush_column_catalog() now calls
            _create_catalog_table() defensively before the bulk upsert.
            CREATE TABLE IF NOT EXISTS is effectively a no-op on existing
            tables, so the cost in healthy deployments is negligible.
            The proper architectural fix (calling db.create_schema() once
            at FastAPI startup) remains the recommended approach; this
            guard only protects against the upgrade-without-migration case.

   Session 14  — V2.8   EDGE-CASE & DATA QUALITY PATCH

            Eight bugs identified in comprehensive data-quality testing.

   Fix 65 [F-01]: EXCEL FORMAT SUPPORT (.xlsx / .xls)
            pd.read_excel() was called without an engine argument, relying on
            pandas' default which varies by environment and panics on .xls.
            Fix: .xlsx → explicit openpyxl engine; .xls → explicit xlrd engine
            with a clear ImportError if xlrd is missing, rather than letting
            openpyxl's cryptic InvalidFileException surface to the caller.

   Fix 66 [F-02]: YYYYMMDD INTEGER DATE MISCLASSIFIED AS NUMERIC
            Columns like sls_order_dt with values 20101229 were registered as
            integers. Missing values were then imputed using median() — a
            mathematically meaningless operation on calendar offsets — and NaT
            was never set, preventing correct time-based analysis.
            Fix: _looks_like_yyyymmdd() helper detects 8-digit integers in the
            range [19000101, 21001231] that parse cleanly via strptime %Y%m%d.
            Qualifying columns are converted to datetime64 before any numeric
            branch executes. The helper fires in Step 1 before the object→
            numeric coercion path, ensuring NaT-based imputation applies.

   Fix 67 [F-03]: CASE-SENSITIVE IDENTIFIER COLUMNS LOWERCASED
            CASE_SENSITIVE_KEYWORDS ("id", "hash", "token" …) protected only
            columns whose names contained those exact tokens. Columns flagged
            as ID columns by ID_KEYWORDS ("num", "no", "ref" …) — like
            sls_ord_num — were not in _case_sensitive_cols and had their values
            silently force-lowercased in Step 3, corrupting identifiers.
            Fix: is_cs check in _step_text_normalization now also guards any
            col in self._id_cols. The two keyword sets serve different purposes
            and are now both consulted for case-preservation decisions.

   Fix 68 [F-04]: TEMPORAL LOGIC VIOLATIONS NOT DETECTED
            No code validated that start-date columns preceded their paired
            end-date columns. Rows where prd_start_dt > prd_end_dt passed
            through and produced negative duration deltas downstream.
            Fix: _step_business_logic_validation() now scans date-column pairs
            using START_KW / END_KW heuristics. Violated rows have both dates
            set to NaT and are recorded in report["business_logic_fixed"].

   Fix 69 [F-05]: EXTREME / IMPOSSIBLE DATE VALUES NOT BOUNDED
            Dates like 9999-01-01, 2980-06-15, or 2080-03-10 in a bdate column
            bypassed all checks. No date-bounds guard existed anywhere in the
            pipeline.
            Fix: After the temporal check, _step_business_logic_validation()
            applies an absolute floor (1900-01-01) and a context-aware ceiling:
            today for birthdate columns, today+5 years for all other dates.
            Out-of-range values are set to NaT and logged.

   Fix 70 [F-06]: PRIMARY KEY INTEGRITY NOT ENFORCED
            Null and duplicate values in ID columns passed through silently.
            report["pk_violations"] did not exist; the caller had no signal.
            Fix: _step_remove_duplicates() now iterates self._id_cols after
            row-level deduplication. Per-column null counts and duplicate counts
            are computed and written to report["pk_violations"] with sample
            duplicate values. The pipeline does NOT mutate PK cells — repair
            requires business-level knowledge — but the structured violation
            report makes the issue immediately actionable.

   Fix 71 [F-07]: INCONSISTENT CATEGORICAL DATA NOT STANDARDISED
            Low-cardinality columns (gender, country, marital status) retained
            mixed representations ("m"/"male"/"Male", "USA"/"U.S.A"/"United
            States") after text normalisation. Downstream encoding and grouping
            produced inflated cardinality and incorrect aggregations.
            Fix: New Step 3.2 _step_categorical_standardization() runs after
            text normalisation. Eligible columns (≤ 20 unique values, name
            matches a domain trigger keyword, at least one value appears in the
            synonym table) are remapped via comprehensive synonym dictionaries
            for gender, marital status, and countries. All remappings are logged
            for a full audit trail. ID columns are always skipped.

   Fix 72 [F-08]: FINANCIAL COLUMNS IMPUTED WITH STATISTICAL MEDIAN
            Monetary columns (prd_cost, price, revenue …) matched
            is_numeric_dtype() in Step 4 and received median imputation.
            Applying a dataset average to cost/price data is functionally
            incorrect — it silently inflates aggregated financial reports and
            introduces phantom values into billing and margin calculations.
            Fix: Step 4 numeric branch now calls _is_monetary_column() first.
            Monetary nulls are preserved as NaN with a clear log message so the
            caller can apply relational logic (unit_price × qty) or surface the
            gap for manual review. Statistical imputation is never applied.

   Fix 73 [F-09]: FASTAPI / IN-MEMORY BYTES UPLOAD NOT SUPPORTED
            _load() only accepted str (file path) or pd.DataFrame. FastAPI
            endpoints pass raw bytes from `await file.read()` which triggered
            TypeError("Invalid source...") immediately.
            Fix: _load() now accepts bytes/bytearray as a third source type.
            The bytes are wrapped in BytesIO and routed through the same
            engine-aware Excel/CSV dispatch as path-based loads. A new
            `filename=` keyword on run() carries the original UploadFile.filename
            so the extension is always available for format detection.
            A dedicated _read_csv_with_encoding_buffer() mirrors the encoding-
            fallback logic of _read_csv_with_encoding() for in-memory CSV
            streams. FastAPI usage:
                raw = await file.read()
                df  = pipeline.run(raw, filename=file.filename)

   Fix 74 [F-10]: DATE_UPPER_BOUND_YEARS HARD-CODED TO 5 — REJECTS VALID DATA
            The 5-year ceiling on future dates invalidated credit card expiry
            dates and long-horizon contract dates that legitimately exceed 5
            years. The constant was buried as a local literal with no way to
            override it per deployment.
            Fix: DATE_UPPER_BOUND_YEARS is now a class-level constant
            (default 10) accessible on every instance. Override at class or
            instance level for domain-specific ceilings:
                pipeline.DATE_UPPER_BOUND_YEARS = 15  # mortgages
                pipeline.DATE_UPPER_BOUND_YEARS = 6   # standard credit cards


   Session 13  — V2.7   PIPELINE-CATALOG PATCH

            Adds column_catalog support to DatabaseManager and wires a
            _flush_column_catalog() call into the pipeline run() exit point.

            The column_catalog table is the contract between the cleaning
            pipeline and the analytics query engine — the frontend must never
            query cleaned_* tables directly.

            DatabaseManager additions:
              _create_catalog_table()    — DDL for column_catalog + partial index;
                                          called automatically from
                                          _create_core_tables() on every init.
              flush_column_catalog()     — bulk-upsert (ON CONFLICT … DO UPDATE)
                                          so re-runs against the same upload_id
                                          are safe.

            DynamicPreprocessingPipeline additions:
              _flush_column_catalog()    — builds the catalog payload from
                                          self.report + self._*_cols sets, then
                                          delegates the write to db.flush_column_catalog().
              _build_display_name()      — derives human-readable labels
                                          (e.g. "lag_1_total_revenue_egp" →
                                          "Total Revenue EGP (Lag 1)").

            run() exit-point change:
              _save_to_database() now returns (db, table_name) on success and
              (None, None) on failure; run() unpacks the tuple and calls
              _flush_column_catalog() only when both are non-None and
              self.report["upload_id"] is set.

   Fix 54 [C-01]: ML-WIDE DATA LEAKAGE — FIT/TRANSFORM ARCHITECTURE
            Steps 4 (median/mode fill), 5 (IQR bounds), and 3.7 (law discovery)
            previously computed statistics across the entire dataset, causing
            test-set values to contaminate training-set imputations.

            New API: run() accepts fit_on (None | float | bool array).
            • fit_on=None  → backward-compatible: all rows used (default).
            • fit_on=0.8   → first 80 % of rows in load order (float 0–1).
            • fit_on=mask  → caller-supplied boolean Series / ndarray.
            self._fit_mask (boolean Series) is built once in run() after loading.

            Affected steps:
            • Step 3.7: law discovery samples drawn from fit rows only.
            • Step 4:   median / mode computed on df[fit_mask][col].
            • Step 5:   Q1/Q3/IQR derived from df[fit_mask][col].dropna().
            The learned statistics are then APPLIED to all rows (train+test),
            which is the correct fit/transform contract.

   Fix 55 [C-02]: TARGET VARIABLE LEAKAGE
            The relational discovery engine (Step 3.7) had no mechanism to
            exclude the ML prediction target column. A discovered law involving
            the target caused Step 3.8 to impute null target values from
            features — pure label leakage before training.

            New API: run() accepts target_col (str | None).
            • Excluded from Step 3.7 eligible column list.
            • Excluded from Step 3.8 forward and inverse imputation.
            • Excluded from Step 4 statistical imputation.
            • Excluded from Step 5 IQR capping.
            Target column is never touched by the pipeline; the ML engineer
            retains full control over it.

   Fix 56 [C-03]: ROW ORDER DESTROYED IN STEP 6.5
            _step_advanced_lag_features sorted by date, concat'd NaT rows at
            the bottom, and returned the rearranged object — silently breaking
            positional alignment between the output CSV and the caller's input.

            Fix: an __orig_order__ tracking column (positional integer) is
            added before any sort, and a final sort_values('__orig_order__')
            + drop restores the original row order before returning. The
            function now returns a row-aligned object in all code paths.

   Fix 57 [C-04]: O(n×k) PYTHON LOOP IN STEP 4.5 (PRODUCTION TIMEOUT)
            The return-keyword classification used .apply(lambda v: any(kw in v
            for kw in RETURN_STATUS_KEYWORDS)) — ~1.5 M Python interpreter hops
            on a 500 K-row dataset with 3 text columns (30–90 s wall-clock).
            Replaced with a single vectorized str.contains(pattern, na=False)
            call compiled to a C-speed regex, reducing the same check to < 1 s.

   Fix 58 [H-01]: THREAD SAFETY — PER-REQUEST INSTANTIATION
            All intermediate state lives on self. Sharing one instance across
            concurrent requests causes User A's rules to corrupt User B's
            discovery mid-pipeline (intermittent, silent).
            Fix: thorough full-state reset at the TOP of run() guarantees a
            clean slate if the same instance is reused sequentially. For true
            concurrency safety, instantiate one pipeline per request (see HOW
            TO USE above). A RuntimeWarning is emitted when a used instance is
            re-entered while a run appears active.

   Fix 59 [H-02]: BOOLEAN COLUMNS IMPUTED WITH NUMERIC MEDIAN
            pd.api.types.is_numeric_dtype() returns True for bool dtype. A
            boolean column with 50% nulls received median()=0.5, causing
            fillna(0.5) to silently corrupt or mis-cast the column.
            Fix: boolean columns are skipped in the numeric imputation branch
            of Step 4 and instead receive mode fill (same as text columns).

   Fix 60 [H-03]: LAG fillna(0) COLLIDES WITH GENUINE ZERO VALUES
            lag_1_* and lag_3_* used fillna(0) for first-in-group rows,
            making "no prior period" indistinguishable from "prior value was 0".
            Fix: unified -1 sentinel across ALL Step 6.5 features (inter-order
            gap, lag_1, lag_3) — consistent with Fix 42's NaT sentinel.
            The report now documents -1 = "no prior period available".

   Fix 61 [H-04]: TABLE NAME RACE CONDITION — DATA LOSS UNDER CONCURRENCY
            datetime.now().strftime('%Y%m%d_%H%M%S') has 1-second granularity.
            Two concurrent uploads of identically-named files within the same
            second produced identical table names; if_exists="fail" raised an
            unhandled exception, losing the second user's processed data.
            Fix: 8-character UUID hex suffix appended to every table name.

   Fix 62 [M-01]: CUSTOM RULE SILENT FAILURE — NO ACTIONABLE REPORT
            A rule referencing a misspelled column caught a KeyError, logged a
            WARNING to the console, and continued silently. The JSON report
            showed custom_rule_imputation:{} with no error flag; downstream
            ML features were all-NaN with no indication.
            Fix: pre-validation pass before rule execution checks that every
            column referenced in the rule's signature exists in df at entry
            time. Validation failures are written to report["custom_rule_errors"]
            as structured dicts (rule, missing_cols, action).

   Fix 63 [M-02]: QUALITY SCORE FLOOR — FALSE CONFIDENCE
            after_score = max(before_score, ...) hardcoded a floor ensuring
            the "after" score never dropped below "before". A run that added
            40 high-correlation lag features or over-imputed a skewed column
            still showed improvement. The floor is removed; after_score is now
            an independent measurement that CAN be lower than before_score
            when the pipeline introduces new data quality concerns.

   Fix 64 [M-03]: INCONSISTENT NaT SENTINELS ACROSS STEP 6.5 FEATURES
            inter_order_col used fillna(0) for the first row in group but -1
            for NaT rows. lag_1_* / lag_3_* used fillna(0) for first-in-group;
            NaT rows got -1 only in the re-merge block. Two different "missing"
            sentinels (0 and -1) existed across features created in the same
            step, making downstream null-handling ambiguous.
            Fix: all Step 6.5 features use -1 unconditionally for every form
            of "no prior data available" (NaT row, first-in-group, both).
            Covered by Fix 60 implementation.
"""

import math
import itertools
import uuid as _uuid_mod
import pandas  as pd
import numpy   as np
import warnings
import os
import sys
import json
import re
import threading
import logging
from datetime import datetime

# [Part-2 Fix 4]: Global warnings.filterwarnings("ignore") removed.
# It was masking legitimate Python warnings across the entire process.
# Scoped suppressions (e.g. per-step context managers) should be used
# where noisy-but-harmless warnings need silencing locally.


# ══════════════════════════════════════════════════════
#  COLORS  (CLI banner use only — not used in log())
# ══════════════════════════════════════════════════════
class Colors:
    HEADER    = "\033[95m"
    BLUE      = "\033[94m"
    CYAN      = "\033[96m"
    GREEN     = "\033[92m"
    YELLOW    = "\033[93m"
    RED       = "\033[91m"
    BOLD      = "\033[1m"
    UNDERLINE = "\033[4m"
    END       = "\033[0m"


# ── Module-level logger ────────────────────────────────────────────────────────
# Consumers configure handlers/level from their own logging config.
# A NullHandler is registered here so the library never emits output when
# the caller has not configured logging (PEP 396 / logging best-practice).
_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())

# Level mapping: pipeline levels → stdlib logging levels
_LOG_LEVEL_MAP = {
    "INFO":    logging.INFO,
    "SUCCESS": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR":   logging.ERROR,
    "STEP":    logging.INFO,
}
# Prefix icons kept for readability in log records (no ANSI codes)
_LOG_ICONS = {
    "INFO":    "•",
    "SUCCESS": "✓",
    "WARNING": "⚠",
    "ERROR":   "✗",
    "STEP":    "▶",
}

def log(msg: str, level: str = "INFO") -> None:
    """
    Emit a pipeline log message through Python's standard logging module.

    The custom `print`-based implementation is replaced here to:
      • Route all pipeline output through the caller's logging configuration.
      • Strip ANSI escape codes from production log records.
      • Map pipeline severity labels to stdlib logging levels so log aggregators
        (CloudWatch, Datadog, Loki) can filter by level correctly.

    The icon prefix is preserved for readability in plain-text log streams.
    ANSI colour codes are intentionally absent — colour belongs in the
    caller's logging formatter, not hardcoded in library code.
    """
    stdlib_level = _LOG_LEVEL_MAP.get(level, logging.INFO)
    icon         = _LOG_ICONS.get(level, "•")
    _logger.log(stdlib_level, "%s  %s", icon, msg)


# ══════════════════════════════════════════════════════
#  OP_DISPATCH — Serialisable formula registry
#
#  [Part-3 Fix 1] Triads previously stored lambda closures under the keys
#  "formula", "inv_a", and "inv_b".  Lambdas are not JSON-serialisable, which
#  caused save_json_report() to fall back to str(val) for the entire
#  discovered_laws list, blocking dashboard delivery.
#
#  The triad dict now stores only a plain string opkey (e.g. "addition").
#  _OP_DISPATCH maps each opkey → {"formula", "inv_a", "inv_b"} callables so
#  the arithmetic is looked up at runtime in _step_relational_imputation()
#  and _enforce_triadic_constraints() without persisting un-serialisable
#  objects on the instance.
# ══════════════════════════════════════════════════════
_OP_DISPATCH: dict = {
    "addition": {
        "formula": lambda a, b: a + b,
        "inv_a":   lambda d, b: d - b,
        "inv_b":   lambda d, a: d - a,
    },
    "subtraction": {
        "formula": lambda a, b: a - b,
        "inv_a":   lambda d, b: d + b,
        "inv_b":   lambda d, a: a - d,
    },
    "multiplication": {
        "formula": lambda a, b: a * b,
        "inv_a":   lambda d, b: d / b.replace(0, np.nan),
        "inv_b":   lambda d, a: d / a.replace(0, np.nan),
    },
    "division": {
        "formula": lambda a, b: a / b.replace(0, np.nan),
        "inv_a":   lambda d, b: d * b,
        "inv_b":   lambda d, a: a / d.replace(0, np.nan),
    },
}


# ══════════════════════════════════════════════════════
#  DATABASE MANAGER
# ══════════════════════════════════════════════════════
class DatabaseManager:
    def __init__(self, engine):
        """
        Accept an already-initialised SQLAlchemy Engine.

        The engine must be created *once* at application startup (e.g. inside
        FastAPI's lifespan handler) and shared across requests via a dependency.
        Creating a new engine per request exhausts the connection pool and causes
        DDL lock contention.

        Schema initialisation (CREATE TABLE IF NOT EXISTS …) is intentionally
        NOT performed here.  Call ``create_schema()`` exactly once at startup:

            engine = create_engine(db_url, pool_size=10, max_overflow=5)
            db_manager = DatabaseManager(engine)
            db_manager.create_schema()   # ← run DDL once, not per-request

        Raises
        ------
        ImportError
            If sqlalchemy is not installed in the current environment.
        """
        try:
            from sqlalchemy.engine import Engine  # noqa: F401 — availability check
        except ImportError:
            raise ImportError(
                "sqlalchemy is not installed — run: pip install sqlalchemy psycopg2-binary"
            )
        self.engine = engine

    def create_schema(self):
        """
        Execute all DDL statements (CREATE TABLE IF NOT EXISTS, ALTER TABLE,
        CREATE INDEX IF NOT EXISTS) exactly **once** at application startup.

        Call this from your FastAPI lifespan or startup event — never inside a
        request handler.  Running DDL per-request acquires exclusive locks and
        exhausts the connection pool under concurrent load.

        Example (FastAPI lifespan)::

            from contextlib import asynccontextmanager
            from sqlalchemy import create_engine

            engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=5)

            @asynccontextmanager
            async def lifespan(app: FastAPI):
                db = DatabaseManager(engine)
                db.create_schema()          # DDL runs once here
                app.state.engine = engine   # share the engine, not the manager
                yield

            app = FastAPI(lifespan=lifespan)
        """
        self._create_core_tables()

    def _create_core_tables(self):
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    SERIAL PRIMARY KEY,
                    username   VARCHAR(100) NOT NULL UNIQUE,
                    email      VARCHAR(255) NOT NULL UNIQUE,
                    password   VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS uploads (
                    upload_id          SERIAL PRIMARY KEY,
                    user_id            INTEGER NOT NULL
                                       REFERENCES users(user_id) ON DELETE CASCADE,
                    original_file_name VARCHAR(255),
                    table_name         VARCHAR(255) NOT NULL,
                    rows_count         INTEGER,
                    columns_count      INTEGER,
                    quality_before     FLOAT,
                    quality_after      FLOAT,
                    uploaded_at        TIMESTAMP DEFAULT NOW(),
                    status             VARCHAR(50) DEFAULT 'completed',
                    cache_status       VARCHAR(50) DEFAULT 'pending'
                );
            """))
            # V2.7 migration: add cache_status to pre-existing uploads tables
            # that were created before this column was introduced.
            # ADD COLUMN IF NOT EXISTS is a no-op when the column already exists,
            # so this guard is always safe to run on both fresh and live databases.
            conn.execute(text("""
                ALTER TABLE uploads
                    ADD COLUMN IF NOT EXISTS cache_status VARCHAR(50) DEFAULT 'pending';
            """))
            conn.commit()
        log("Core tables verified (users + uploads)", "SUCCESS")
        self._create_catalog_table()    # Pipeline-catalog patch

    # ── Pipeline-catalog patch: Section 1 ────────────────────────────────

    def _create_catalog_table(self):
        """
        Creates column_catalog if it does not already exist.

        Schema design notes:
        ─────────────────────────────────────────────────────────────────────
        column_type     : mirrors pipeline's columns_detected keys:
                          'numeric' | 'text' | 'date' | 'boolean'

        is_monetary     : float64 was preserved by the pipeline (Fix 23/47).
                          The analytics layer must never cast these to float32
                          or apply rounding before aggregation.

        is_target       : column was excluded from imputation/discovery [C-02].
                          The UI should offer it only in "predicted value" slots,
                          never as a grouping or filter dimension.

        is_lag          : pipeline-generated lag/inter-order feature.
                          Hidden from the UI's "group by" and "filter" pickers
                          by default; only shown in "advanced / raw features"
                          mode so analysts can still reach them.

        is_engineered   : any pipeline-generated column (lag_*, date parts,
                          delta_days_*). Filtered out of the primary schema
                          view to keep the UI clean for business users.

        display_name    : human-readable label derived at flush time
                          (e.g. "Order Date — Month" instead of
                          "order_date_month"). Overridable by the user later
                          via a PATCH /catalog endpoint.

        agg_default     : the aggregation the UI should pre-select for this
                          column when it is dragged onto a chart axis.
                          Monetary columns default to 'sum'; counts to 'count';
                          date parts to 'none' (used only as group dimensions).
        ─────────────────────────────────────────────────────────────────────
        """
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS column_catalog (
                    catalog_id      SERIAL PRIMARY KEY,
                    upload_id       INTEGER NOT NULL
                                    REFERENCES uploads(upload_id) ON DELETE CASCADE,
                    table_name      VARCHAR(255) NOT NULL,
                    column_name     VARCHAR(255) NOT NULL,
                    column_type     VARCHAR(50)  NOT NULL,
                    is_monetary     BOOLEAN      DEFAULT FALSE,
                    is_target       BOOLEAN      DEFAULT FALSE,
                    is_lag          BOOLEAN      DEFAULT FALSE,
                    is_engineered   BOOLEAN      DEFAULT FALSE,
                    display_name    VARCHAR(255),
                    agg_default     VARCHAR(50)  DEFAULT 'sum',
                    created_at      TIMESTAMP    DEFAULT NOW(),
                    UNIQUE (upload_id, column_name)
                );
            """))
            # Partial index: fast lookup of "original" columns for the
            # schema discovery endpoint — the most frequent query pattern.
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_catalog_upload_original
                ON column_catalog (upload_id)
                WHERE is_engineered = FALSE;
            """))
            conn.commit()
        log("column_catalog table verified", "SUCCESS")

    def flush_column_catalog(self, upload_id: int, table_name: str,
                             catalog_rows: list) -> int:
        """
        Bulk-upserts column metadata for one upload into column_catalog.

        Parameters
        ----------
        upload_id    : the upload_id returned by register_upload()
        table_name   : the cleaned_* table where the data lives
        catalog_rows : list of dicts, each containing the keys:
                       column_name, column_type, is_monetary, is_target,
                       is_lag, is_engineered, display_name, agg_default

        Returns
        -------
        Number of rows written.

        Design note — upsert not insert:
            ON CONFLICT (upload_id, column_name) DO UPDATE lets the pipeline
            re-run against the same upload_id (e.g. after a manual re-clean)
            without leaving orphan rows or raising a duplicate-key error.

        [v2.9.1 Fix B-07]: Self-healing DDL guard.
            Production deployments that were initialised with a pre-V2.7
            schema do not have the column_catalog table.  Without this
            guard the first upload after the pipeline upgrade fails with
            "relation column_catalog does not exist" — surfacing as a 500
            from the FastAPI route AFTER the cleaned data has already been
            written, leaving an orphan upload row with no catalog entries.
            The proper fix is to call create_schema() once at FastAPI
            startup, but as a defensive last-resort we ensure the table
            exists before the bulk upsert.  CREATE TABLE IF NOT EXISTS is
            effectively a no-op on existing tables, so the cost in the
            healthy path is negligible.
        """
        from sqlalchemy import text
        if not catalog_rows:
            log("flush_column_catalog: no rows to write — skipping.", "WARNING")
            return 0

        # [v2.9.1 B-07]: idempotent safety net — see docstring above.
        try:
            self._create_catalog_table()
        except Exception as ddl_exc:
            log(f"flush_column_catalog: DDL safety check failed: {ddl_exc}",
                "WARNING")

        sql = text("""
            INSERT INTO column_catalog
                (upload_id, table_name, column_name, column_type,
                 is_monetary, is_target, is_lag, is_engineered,
                 display_name, agg_default)
            VALUES
                (:upload_id, :table_name, :column_name, :column_type,
                 :is_monetary, :is_target, :is_lag, :is_engineered,
                 :display_name, :agg_default)
            ON CONFLICT (upload_id, column_name)
            DO UPDATE SET
                column_type   = EXCLUDED.column_type,
                is_monetary   = EXCLUDED.is_monetary,
                is_target     = EXCLUDED.is_target,
                is_lag        = EXCLUDED.is_lag,
                is_engineered = EXCLUDED.is_engineered,
                display_name  = EXCLUDED.display_name,
                agg_default   = EXCLUDED.agg_default;
        """)

        params = [
            {**row, "upload_id": upload_id, "table_name": table_name}
            for row in catalog_rows
        ]

        with self.engine.connect() as conn:
            conn.execute(sql, params)
            conn.commit()

        log(f"column_catalog flushed  →  {len(params):,} column(s) "
            f"for upload_id={upload_id}", "SUCCESS")
        return len(params)

    # ── End pipeline-catalog patch: Section 1 ────────────────────────────

    def save_cleaned_data(self, df, upload_id: int):
        """
        Persist the cleaned DataFrame as a new PostgreSQL table and return
        the table name.

        Table-name generation
        ─────────────────────
        The name is derived **solely from system-controlled variables**:

            cleaned_upload_{upload_id}_{uuid8}

        ``upload_id`` is the SERIAL primary key assigned by PostgreSQL in
        ``register_upload()`` — an integer that can never contain SQL metacharacters.
        ``uuid8`` (8 random hex chars) preserves the Fix-61 uniqueness guarantee
        so that re-runs against the same upload_id still produce distinct tables.

        The previous implementation built the name from ``file_name``, a
        value that originates from the HTTP multipart upload.  Even after
        ``re.sub(r'[^a-z0-9_]', '_', …)`` sanitisation, a crafted filename
        could still influence DDL identifiers, which is categorically unsafe.

        Parameters
        ----------
        df        : cleaned DataFrame to persist.
        upload_id : system-assigned integer from ``register_upload()``.
                    Must be a positive int; never derived from user input.
        """
        uid_suffix = _uuid_mod.uuid4().hex[:8]
        # Only system variables: a DB-assigned integer + random hex — zero user input.
        table_name = f"cleaned_upload_{upload_id}_{uid_suffix}"

        df.to_sql(name=table_name, con=self.engine, if_exists="fail",
                  index=False, chunksize=10_000, method="multi")
        log(f"Cleaned data saved  →  table: '{table_name}'  ({len(df):,} rows)", "SUCCESS")
        return table_name

    def update_upload_table_name(self, upload_id: int, table_name: str):
        """
        Set the ``table_name`` column on an existing uploads row.

        Called after ``save_cleaned_data()`` to write the finalised table name
        back to the upload record that was created with the provisional value
        ``'pending'`` by ``register_upload()``.
        """
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(
                text("UPDATE uploads SET table_name = :tn WHERE upload_id = :uid"),
                {"tn": table_name, "uid": upload_id},
            )
            conn.commit()

    def register_upload(self, user_id, file_name, table_name,
                        rows, cols, q_before, q_after):
        from sqlalchemy import text
        sql = text("""
            INSERT INTO uploads
                (user_id, original_file_name, table_name,
                 rows_count, columns_count, quality_before, quality_after)
            VALUES (:user_id,:file_name,:table_name,:rows,:cols,:q_before,:q_after)
            RETURNING upload_id;
        """)
        with self.engine.connect() as conn:
            upload_id = conn.execute(sql, {
                "user_id": user_id, "file_name": os.path.basename(file_name),
                "table_name": table_name, "rows": rows, "cols": cols,
                "q_before": q_before, "q_after": q_after,
            }).fetchone()[0]
            conn.commit()
        log(f"Upload registered  →  upload_id: {upload_id}  |  user_id: {user_id}", "SUCCESS")
        return upload_id

    def create_test_user(self, username="test_user", email="test@optimaai.com"):
        from sqlalchemy import text
        sql = text("""
            INSERT INTO users (username,email) VALUES (:username,:email)
            ON CONFLICT (email) DO UPDATE SET username=EXCLUDED.username
            RETURNING user_id;
        """)
        with self.engine.connect() as conn:
            uid = conn.execute(sql, {"username": username, "email": email}).fetchone()[0]
            conn.commit()
        log(f"Test user ready  →  user_id: {uid}  ({email})", "INFO")
        return uid


# ══════════════════════════════════════════════════════
#  MAIN PIPELINE  — V2.6
# ══════════════════════════════════════════════════════
class DynamicPreprocessingPipeline:
    """
    V2.7: Pipeline-catalog patch merged (column_catalog support).

    V2.6: Full-audit patch applied (Fixes 54-64).

    Critical fixes:
      C-01  fit/transform architecture — no train/test stat leakage.
      C-02  target_col exclusion — no label leakage via relational imputation.
      C-03  row order preserved in Step 6.5.
      C-04  vectorized return-keyword detection (Step 4.5).

    High fixes:
      H-01  thread safety — thorough per-run state reset; per-request pattern.
      H-02  boolean columns skip numeric median branch (Step 4).
      H-03  unified -1 sentinel for all "no prior period" lag values.
      H-04  UUID suffix on DB table names prevents race condition data loss.

    Medium fixes:
      M-01  custom rule pre-validation surfaces failures in JSON report.
      M-02  quality score floor removed — after score is a real measurement.
      M-03  consistent NaT sentinels across all Step 6.5 features (see H-03).

    Thread-safety contract:
      One DynamicPreprocessingPipeline instance per request.
      Do NOT share instances across concurrent threads.
      See module docstring for the correct FastAPI pattern.
    """

    # ── Class-level constants (immutable — safe to share) ──────────────────

    MONETARY_KEYWORDS: frozenset = frozenset({
        "price", "revenue", "balance", "cost", "amount", "salary", "wage",
        "pay", "benefit", "fee", "payment", "income", "profit", "total",
        "gross", "net", "earnings", "spend", "sales", "invoice", "rate",
        "charge", "value", "discount", "tax", "taxes", "penalty", "fine",
        "installment", "emi", "budget", "commission", "expense", "expenses",
        "capital", "loan", "debt", "bonus", "incentive", "deposit",
        "downpayment", "allowance", "fare", "funding",
        "السعر", "المبلغ", "الراتب", "الإجمالي", "خصم", "ضريبة", "ضرائب",
        "غرامة", "غرامات", "قسط", "أقساط", "ميزانية", "موازنة", "عمولة",
        "مصاريف", "نفقات", "مكافأة", "حافز", "عربون", "مقدم", "إيداع",
        "قرض", "ديون", "رسوم", "تكلفة", "إيرادات", "مبيعات", "ارباح", "ربح", "رصيد"
    })

    NON_NEGATIVE_KEYWORDS: frozenset = frozenset({
        "quantity", "qty", "count", "units", "volume", "stock",
        "age", "weight", "height",
        "الكمية",
    })

    CASE_SENSITIVE_KEYWORDS: frozenset = frozenset({
        "id", "hash", "code", "token", "url", "key", "ref",
        "uuid", "guid", "sku", "barcode", "serial",
    })

    ENTITY_KEYWORDS: frozenset = frozenset({
        "customer", "client", "user", "member", "account",
        "buyer", "seller", "vendor", "supplier", "employee",
        "driver", "agent", "person", "contact",
    })

    PRIMARY_DATE_KEYWORDS: frozenset = frozenset({
        "order", "purchase", "created", "timestamp", "placed",
        "start", "open", "issued", "booked", "registered",
    })

    SPARSITY_THRESHOLD: float = 0.30
    ENCODING_FALLBACKS: tuple = (
        "utf-8", "utf-8-sig", "latin1", "cp1252", "windows-1256", "iso-8859-1"
    )
    FILE_SIZE_WARN_MB: int = 500

    # [Part-2 Fix 1]: Hard file-size ceiling to prevent OOM crashes.
    # _load() raises ValueError before calling any pd.read_* if the input
    # (bytes payload or file path) exceeds this limit.
    # Raise at the class or instance level for large-file deployments:
    #     pipeline.MAX_FILE_SIZE_MB = 500
    MAX_FILE_SIZE_MB: int = 100

    # Fix 74 [F-10]: Configurable future-date ceiling for non-birthdate columns.
    # Credit card expiry dates and long-horizon contracts can exceed 5 years, so
    # the previous hard-coded 5 was rejecting valid data.  Raise or lower this
    # at the class or instance level to match your domain:
    #     pipeline = DynamicPreprocessingPipeline()
    #     pipeline.DATE_UPPER_BOUND_YEARS = 15   # e.g. for long-term mortgages
    DATE_UPPER_BOUND_YEARS: int = 10

    # [Part-2 Fix 2]: Wall-clock timeout for the O(N³) relational discovery loop.
    # When the deadline fires the loop breaks immediately; partial candidates are
    # discarded, non_triadic_bypass is set to True, and the pipeline continues
    # gracefully.  signal.alarm is NOT used so this is safe in non-main threads.
    # Increase for large column counts on fast hardware:
    #     pipeline.DISCOVERY_TIMEOUT_SECONDS = 60
    DISCOVERY_TIMEOUT_SECONDS: int = 30

    # ── Fix 58 [H-01]: reuse guard ─────────────────────────────────────────
    _ACTIVE_LOCK = threading.Lock()

    def __init__(self):
        # Fix 58 [H-01]: All mutable run-time state is defined here AND fully
        # reset at the top of run().  This guarantees sequential reuse safety.
        # For concurrent use, instantiate one pipeline per request (see docs).
        self._reset_run_state()

    def _reset_run_state(self):
        """
        Fix 58 [H-01]: Reinitialise every piece of mutable run-time state.
        Called both from __init__ and from the top of run() so that
        sequential reuse of the same instance is always safe.
        """
        self.report = {
            "original_shape":        None,
            "final_shape":           None,
            "duplicates_removed":    0,
            "missing_filled":        {},
            # [v2.9 B-01]: monetary cells whose NaN was deliberately preserved
            # are tracked here, NOT in missing_filled (nothing was filled).
            "missing_preserved":     {},
            "skipped_sparse_cols":   [],
            "type_conversions":      {},
            "outliers_handled":      {},
            "date_features_added":   [],
            "columns_detected":      {"numeric": [], "text": [], "date": [], "boolean": []},
            "encoding_report":       {},
            "high_correlations":     [],
            "quality_score":         {"before": 0, "after": 0},
            "lag_features_added":    [],
            "memory_optimization":   {},
            "database_table":        None,
            "upload_id":             None,
            "business_logic_fixed":  {},
            "relational_imputation": {},
            "custom_rule_imputation": {},
            "custom_rule_errors":    [],   # Fix 62 [M-01]: structured error list
            "constraint_sync":       {},
            "discovered_laws":       [],
            "discovery_conflicts":   [],
            "file_encoding":         None,
            "monetary_protected":    [],
            "non_triadic_bypass":    False,
            # Fix 54 [C-01]: record fit-mask coverage for transparency
            "fit_mask_rows":         None,
            "total_rows_at_fit":     None,
            # Fix 55 [C-02]: record excluded target column
            "target_col_excluded":   None,
            # Fix 70 [F-06]: primary key integrity results
            "pk_violations":         {},
        }
        self._integer_intent_cols: set  = set()
        self._id_cols:             set  = set()
        self._case_sensitive_cols: set  = set()
        self._discovered_triads         = None
        self._primary_date_col: str     = None
        self._custom_rules:     dict    = {}

        # Fix 54 [C-01]: boolean Series marking training rows used for stats
        self._fit_mask: pd.Series       = None

        # Fix 55 [C-02]: target column name to exclude from imputation/discovery
        self._target_col: str           = None

        # Row-tracking for reporting (retained from V2.3/V2.5)
        self._imputed_rows: dict        = {}
        self._capped_rows:  dict        = {}

    # ──────────────────────────────────────────────────
    #  ENTRY POINT
    # ──────────────────────────────────────────────────
    def run(self, source, save_output=True, output_path=None,
            db_url=None, user_id=None, custom_rules: dict = None,
            fit_on=None, target_col: str = None, filename: str = None):
        """
        Parameters
        ----------
        source       : str (file path), pd.DataFrame, or bytes/bytearray
                       (in-memory upload from FastAPI / UploadFile.read())
        save_output  : bool — write cleaned CSV + JSON report to disk
        output_path  : str — override default timestamped filename
        db_url       : str — SQLAlchemy connection string (optional)
        user_id      : int — registered user ID for upload registry (optional)
        custom_rules : dict, optional — N-ary business equation injection.
            Keys   = derived column name (str).
            Values = callable(df) → pd.Series
        filename     : str, optional — original filename hint for bytes uploads.
                       Required when source is bytes so the extension can be
                       detected.  Example: filename=file.filename in FastAPI.
                       [Fix 73]
        fit_on       : None | float | bool-array, optional  [Fix 54, C-01]
            Controls which rows are used to COMPUTE statistics (median, IQR,
            discovered laws).  Learned statistics are applied to ALL rows.
            • None        → all rows (default, backward-compatible).
            • float 0–1   → positional fraction, e.g. 0.8 = first 80 % of
                            rows in load order.  Use this only when the dataset
                            is already sorted chronologically by the caller.
            • bool Series / ndarray / list of bool — explicit training mask
                            aligned to the loaded DataFrame's positional index.
            NOTE: For time-series data, the caller should sort the dataset by
            date BEFORE passing it in, then supply fit_on=0.8 (or a date-based
            mask). The pipeline does NOT sort before applying the mask.
        target_col   : str | None, optional  [Fix 55, C-02]
            Name of the ML prediction target column.  When supplied:
            • Excluded from Step 3.7 relational discovery (no label leakage
              via discovered formulas).
            • Excluded from Step 3.8 forward and inverse imputation.
            • Excluded from Step 4 median/mode imputation.
            • Excluded from Step 5 IQR capping.
            The column is passed through to the output completely untouched.
        """
        # ── Fix [H-01 Part-3]: Enforce the class-level lock on every run() entry.
        # _ACTIVE_LOCK prevents two concurrent calls from mutating the same
        # instance simultaneously (e.g. shared instance across async tasks).
        # acquire(blocking=True) waits for any in-flight run to finish first.
        # The finally clause guarantees the lock is released even if an
        # unhandled exception escapes any pipeline step.
        # NOTE: For true multi-request concurrency, still prefer one instance
        # per request (see module docstring). The lock is a last-resort guard.
        self._ACTIVE_LOCK.acquire(blocking=True)
        try:
            return self._run_impl(
                source=source, save_output=save_output,
                output_path=output_path, db_url=db_url,
                user_id=user_id, custom_rules=custom_rules,
                fit_on=fit_on, target_col=target_col, filename=filename,
            )
        finally:
            self._ACTIVE_LOCK.release()

    def _run_impl(self, source, save_output=True, output_path=None,
                  db_url=None, user_id=None, custom_rules: dict = None,
                  fit_on=None, target_col: str = None, filename: str = None):
        """Internal implementation — called exclusively from run() under lock."""
        _logger.info("=" * 60)
        _logger.info("  DYNAMIC PREPROCESSING PIPELINE v2.9 — START")
        _logger.info("=" * 60)

        # Fix 58 [H-01]: Full state reset on every run() entry.
        # Fix 73 [F-09]: stash the caller-supplied filename so _load() can
        # derive the file extension when source is a raw bytes object.
        self._upload_filename = filename or (
            source if isinstance(source, str) else None
        )

        # This makes sequential reuse of one instance safe.
        # For CONCURRENT use, instantiate one pipeline per request.
        self._reset_run_state()

        # ── Fix 50: Validate and store custom rules ────────────────────────
        if custom_rules:
            for col, rule in custom_rules.items():
                if not callable(rule):
                    log(f"  [custom_rules] '{col}': rule is not callable — skipped.",
                        "WARNING")
                    continue
                self._custom_rules[col] = rule
            if self._custom_rules:
                log(f"  Custom rules registered: "
                    f"{list(self._custom_rules.keys())}  [Fix 50]", "INFO")

        # ── Load data ──────────────────────────────────────────────────────
        df = self._load(source)

        # [Part-2 Fix 3]: Removed df_original = df.copy() — that full copy was
        # kept only to supply "before" metrics to _calculate_quality_score at
        # the end of the run, wasting up to 2× peak RAM for large datasets.
        # Instead, capture the three lightweight scalars/sets we actually need
        # right here, before any step mutates df.  All three are O(cols) or
        # O(rows) in the narrow sense (null bitmap), not a full data copy.
        #
        #   null_total  — total missing cells in the raw input
        #   n_rows      — row count before Step 2 deduplication
        #   cols        — set of column names (used for original_cols intersection
        #                 in the after-score calculation)
        #   obj_cols    — object-dtype column names (for n_untyped_obj, mirrors
        #                 the df_before[col].dtype==object test in the old code)
        _before_metrics: dict = {
            "null_total": int(df.isnull().sum().sum()),
            "size":       df.size,
            "n_rows":     len(df),
            "cols":       set(df.columns),
            "obj_cols":   {col for col in df.columns if df[col].dtype == object},
        }

        self.report["original_shape"] = df.shape
        log(f"Dataset loaded  →  {df.shape[0]:,} rows  ×  {df.shape[1]} columns",
            "SUCCESS")

        # ── Fix 54 [C-01]: Build fit mask AFTER loading ────────────────────
        # All statistical computations (median, IQR, law discovery) will use
        # only the rows where _fit_mask is True.  The learned statistics are
        # then applied to ALL rows, preserving the fit/transform contract.
        n_total = len(df)
        if fit_on is None:
            self._fit_mask = pd.Series(True, index=df.index)
            log(f"  Fit mask: all {n_total:,} rows (no split)  [C-01]", "INFO")
        elif isinstance(fit_on, float):
            if not (0.0 < fit_on <= 1.0):
                raise ValueError(
                    f"fit_on float must be in (0, 1], got {fit_on}"
                )
            n_fit  = max(1, int(n_total * fit_on))
            mask   = pd.Series(False, index=df.index)
            mask.iloc[:n_fit] = True
            self._fit_mask = mask
            log(f"  Fit mask: first {n_fit:,} of {n_total:,} rows "
                f"({fit_on:.0%})  [C-01]", "INFO")
        elif isinstance(fit_on, (pd.Series, np.ndarray, list)):
            self._fit_mask = pd.Series(
                np.asarray(fit_on, dtype=bool), index=df.index
            )
            n_fit = int(self._fit_mask.sum())
            log(f"  Fit mask: {n_fit:,} of {n_total:,} rows "
                f"(caller-supplied)  [C-01]", "INFO")
        else:
            raise TypeError(
                f"fit_on must be None, a float, or a boolean array, "
                f"got {type(fit_on).__name__}"
            )
        n_fit_rows = int(self._fit_mask.sum())
        self.report["fit_mask_rows"]     = n_fit_rows
        self.report["total_rows_at_fit"] = n_total

        # ── Fix 55 [C-02]: Store target column ────────────────────────────
        self._target_col = target_col
        if target_col:
            if target_col not in df.columns:
                log(f"  WARNING: target_col='{target_col}' not found in dataset — "
                    f"exclusion has no effect.  [C-02]", "WARNING")
            else:
                log(f"  Target column '{target_col}' excluded from "
                    f"discovery/imputation  [C-02]", "INFO")
            self.report["target_col_excluded"] = target_col

        # ── Pipeline execution (Fix 33: 4.5 before 4.8) ───────────────────
        df = self._step_detect_and_cast_types(df)               # Step 1
        df = self._step_remove_duplicates(df)                   # Step 2
        df = self._step_text_normalization(df)                  # Step 3
        df = self._step_categorical_standardization(df)         # Step 3.2 [Fix 71]
        df = self._step_redetect_dates_after_normalization(df)  # Step 3.5
        df = self._step_relational_discovery(df)                # Step 3.7
        df = self._step_relational_imputation(df)               # Step 3.8
        df = self._step_custom_rule_imputation(df)              # Step 3.9
        df = self._step_clean_missing_values(df)                # Step 4
        df = self._step_business_logic_validation(df)           # Step 4.5
        df = self._step_post_imputation_constraint_sync(df)     # Step 4.8
        # [v2.9 Fix B-04]: total_sales moved here from Step 3.9.1 so that
        # any Quantity nulls filled in Step 4 (median imputation) and any
        # values recovered in Step 4.8 (constraint sync) contribute to the
        # derivation.  Sales is monetary (Fix 72) and may still be NaN —
        # rows whose Sales is NaN correctly produce NaN Total Sales.
        df = self._step_total_sales(df)                         # Step 4.9 [v2.9 B-04]
        df = self._step_outlier_detection(df)                   # Step 5
        df = self._step_post_outlier_constraint_sync(df)        # Step 5.5
        df = self._step_date_feature_engineering(df)            # Step 6
        df = self._step_advanced_lag_features(df)               # Step 6.5
        df = self._step_memory_optimization(df)                 # Step 7
        df = self._step_encoding_report(df)                     # Step 8
        df = self._step_correlation_detection(df)               # Step 9
        self._calculate_quality_score(_before_metrics, df)  # [Part-2 Fix 3]

        self.report["final_shape"] = df.shape
        self._print_report(df)

        if save_output:
            if output_path is None:
                # H-04 (unified): mirrors DatabaseManager.save_cleaned_data()
                # exactly — same sanitisation, same timestamp, same 8-char UUID
                # suffix.  Resulting filenames are collision-safe even when two
                # uploads of the same file complete within the same second.
                #
                # Format:  cleaned_{base}_{YYYYMMDD_HHMMSS}_{uuid8}.csv
                # Example: cleaned_sales_data_20260427_143201_a3f9c12e.csv
                #
                # Fix 75 [F-11]: self._upload_filename is populated for ALL
                # source types (str path, DataFrame, and bytes upload) at the
                # top of run(), so it is the single reliable name source.
                # The old isinstance(source, str) branch silently fell through
                # to "dataset" for every FastAPI bytes upload, producing
                # identically-named output files for every user.
                _fname     = getattr(self, "_upload_filename", None) or ""
                _raw_base  = os.path.splitext(os.path.basename(_fname))[0] if _fname else ""
                _base_name = (
                    re.sub(r'[^a-z0-9_]', '_', _raw_base.lower()).strip('_')
                    or "dataset"
                )
                output_path = (
                    f"cleaned_{_base_name}"
                    f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    f"_{_uuid_mod.uuid4().hex[:8]}.csv"
                )
            df.to_csv(output_path, index=False)
            log(f"Cleaned CSV saved  →  {output_path}", "SUCCESS")
            self.save_json_report(output_path.replace(".csv", "_report.json"))

        if db_url:
            db, table_name = self._save_to_database(
                df=df, source=source, db_url=db_url, user_id=user_id
            )
            # Catalog flush requires a valid upload_id, which register_upload()
            # sets on self.report["upload_id"] inside _save_to_database().
            if db is not None and self.report.get("upload_id") and table_name:
                self._flush_column_catalog(
                    df=df,
                    db=db,
                    upload_id=self.report["upload_id"],
                    table_name=table_name,
                )

        return df, self.report

    # ──────────────────────────────────────────────────
    #  PIPELINE-CATALOG PATCH — Section 2
    #  _flush_column_catalog() + _build_display_name()
    # ──────────────────────────────────────────────────
    def _flush_column_catalog(self, df, db,
                              upload_id: int, table_name: str) -> None:
        """
        Builds the column_catalog payload from the pipeline's own report
        and self._*_cols sets, then delegates the DB write to
        db.flush_column_catalog().

        Called at the very end of run() after _save_to_database(), so
        upload_id is guaranteed to exist in the uploads table before we
        write the FK-constrained catalog rows.

        Column classification logic
        ───────────────────────────
        The pipeline already computed everything we need:

          self.report["columns_detected"]     → type buckets
          self.report["lag_features_added"]   → lag/engineered cols
          self.report["date_features_added"]  → engineered date parts
          self._target_col                    → target exclusion flag
          MONETARY_KEYWORDS                   → monetary flag
          self._integer_intent_cols           → influences agg_default

        We derive is_engineered as the union of lag and date-part features,
        which means the analytics endpoint can easily filter them out of the
        default schema view without needing to pattern-match column names.
        """
        log("Flushing column catalog to DB …", "INFO")

        # Sets for fast O(1) membership testing
        lag_cols      = set(self.report.get("lag_features_added", []))
        date_eng_cols = set(self.report.get("date_features_added", []))
        engineered    = lag_cols | date_eng_cols

        # Build type map: column_name → 'numeric' | 'text' | 'date' | 'boolean'
        type_map: dict = {}
        for bucket in ("numeric", "text", "date", "boolean"):
            for col in self.report["columns_detected"].get(bucket, []):
                type_map[col] = bucket

        # Monetary set for flag + agg_default override
        monetary_set = {
            col for col in df.columns
            if self._is_monetary_column(col)
        }

        catalog_rows: list = []

        for col in df.columns:
            col_type    = type_map.get(col, "text")
            is_lag      = col in lag_cols
            is_eng      = col in engineered
            is_monetary = col in monetary_set
            is_target   = (self._target_col is not None
                           and col == self._target_col)

            # ── agg_default ─────────────────────────────────────────────────
            # The UI uses this to pre-select the aggregation when a column
            # is dropped onto a chart axis, reducing configuration clicks.
            #
            #   monetary / numeric → 'sum'  (revenue, cost, quantity)
            #   date parts         → 'none' (used as dimensions, not measures)
            #   lag features       → 'avg'  (average of lagged values)
            #   text / boolean     → 'count_distinct'
            #   target column      → 'sum'  (regressed value) or
            #                        'count_distinct' (classification target)
            if col in date_eng_cols:
                agg_default = "none"
            elif is_lag:
                agg_default = "avg"
            elif col_type == "numeric":
                agg_default = "sum"
            else:
                agg_default = "count_distinct"

            display_name = self._build_display_name(col, col_type, is_lag,
                                                     is_eng, is_target)

            catalog_rows.append({
                "column_name":   col,
                "column_type":   col_type,
                "is_monetary":   is_monetary,
                "is_target":     is_target,
                "is_lag":        is_lag,
                "is_engineered": is_eng,
                "display_name":  display_name,
                "agg_default":   agg_default,
            })

        db.flush_column_catalog(upload_id, table_name, catalog_rows)
        self.report["catalog_flushed"] = len(catalog_rows)

    @staticmethod
    def _build_display_name(col: str, col_type: str,
                             is_lag: bool, is_engineered: bool,
                             is_target: bool) -> str:
        """
        Derives a human-readable label from a raw column name.

        Transformation rules (applied in order):
          1. Replace underscores with spaces.
          2. Title-case each word.
          3. Suffix tag for pipeline-generated features so business users
             can immediately distinguish engineered columns from raw fields.

        Examples
        ─────────────────────────────────────────────────────────────────────
        'Total_Revenue'                    → 'Total Revenue'
        'lag_1_total_revenue_egp'          → 'Total Revenue Egp (Lag 1)'
        'order_date_month'                 → 'Order Date Month (Engineered)'
        'days_since_last_order_date'       → 'Days Since Last Order Date (Engineered)'
        'next_month_sales'  [target]       → 'Next Month Sales ★ Target'
        ─────────────────────────────────────────────────────────────────────
        """
        name = col

        # Strip known lag prefixes and capture the period number
        lag_match = re.match(r'^lag_(\d+)_(.+)$', name, re.IGNORECASE)
        if lag_match:
            period, rest = lag_match.group(1), lag_match.group(2)
            name = f"{rest} (Lag {period})"
        else:
            name = name.replace("_", " ").strip()

        # Title-case (preserves known acronyms like EGP, ID, SKU)
        name = " ".join(
            word.upper() if len(word) <= 3 and word.upper() in {
                "EGP", "USD", "ID", "SKU", "SLA", "QTY", "VAT"
            } else word.capitalize()
            for word in name.split()
        )

        # Append semantic tag
        if is_target:
            name = f"{name} ★ Target"
        elif is_lag:
            pass  # period already appended above
        elif is_engineered:
            name = f"{name} (Engineered)"

        return name

    # ── End pipeline-catalog patch: Section 2 ────────────────────────────

    # ──────────────────────────────────────────────────
    #  DATABASE SAVE
    # ──────────────────────────────────────────────────
    def _save_to_database(self, df, source, db_url, user_id):
        _logger.info("")
        log("DATABASE — Saving cleaned data", "STEP")
        _logger.info("  " + "─" * 52)
        try:
            from sqlalchemy import create_engine
            # Fix 1 [Priority 1]: Create the SQLAlchemy engine here (one per
            # request) and inject it into DatabaseManager rather than letting
            # DatabaseManager construct its own engine.  The engine itself is
            # lightweight to create; the cost is borne once per request rather
            # than leaking an unbounded number of pool handles.
            #
            # In a production FastAPI deployment the engine should be created
            # once at startup (see DatabaseManager.create_schema() docstring)
            # and injected via Depends(); the per-request create_engine() call
            # below is preserved for the standalone / CLI execution path only.
            engine     = create_engine(db_url)
            db         = DatabaseManager(engine)
            # Fix 75 [F-11]: For in-memory (bytes) uploads the source is not a
            # string path, so the old isinstance(source, str) guard always fell
            # through to "dataset", registering every FastAPI upload under the
            # same name in the PostgreSQL uploads table and breaking the SaaS
            # dashboard.  self._upload_filename is set at the top of run() for
            # ALL source types (str path, DataFrame, and bytes), so it is the
            # single reliable source of truth for the original filename.
            file_name  = getattr(self, "_upload_filename", None) or "dataset"
            if user_id is not None:
                qs = self.report["quality_score"]
                # Fix 3 [Priority 2]: Register the upload FIRST to obtain a
                # system-assigned upload_id (PostgreSQL SERIAL), then pass that
                # integer to save_cleaned_data() so the table name is derived
                # solely from system variables — never from user-supplied input.
                # A provisional table_name of 'pending' is written initially and
                # updated to the real name once the table has been created.
                upload_id = db.register_upload(
                    user_id=user_id, file_name=file_name,
                    table_name="pending",           # updated below after table creation
                    rows=int(df.shape[0]), cols=int(df.shape[1]),
                    q_before=float(qs["before"]), q_after=float(qs["after"]),
                )
                self.report["upload_id"] = upload_id
                # Fix 3 [Priority 2]: Table name is now derived only from the
                # DB-assigned upload_id integer + a random UUID suffix (Fix 61).
                # No user-controlled string can reach the DDL identifier.
                table_name = db.save_cleaned_data(df, upload_id)
                db.update_upload_table_name(upload_id, table_name)
            else:
                log("No user_id — upload not registered in uploads table.", "WARNING")
                # Generate a safe anonymous table name when no user context exists
                # (e.g. CLI runs without a user_id).  Still no user input touches
                # the table name; uuid4 provides the sole differentiator.
                anon_id   = _uuid_mod.uuid4().int & 0x7FFFFFFF  # safe positive int
                table_name = db.save_cleaned_data(df, anon_id)
            self.report["database_table"] = table_name
            return db, table_name   # Pipeline-catalog patch: Section 4
        except Exception as e:
            log(f"Database save failed: {e}", "ERROR")
            return None, None       # Pipeline-catalog patch: Section 4

    # ──────────────────────────────────────────────────
    #  STEP 0 : LOAD DATA
    # ──────────────────────────────────────────────────
    def _load(self, source):
        if isinstance(source, pd.DataFrame):
            log("Source is a DataFrame — using directly.", "INFO")
            return source.copy()
        # Fix 73 [F-09]: FastAPI / in-memory upload support.
        # When a file arrives via `await file.read()` in a FastAPI endpoint it
        # is a plain `bytes` object — not a path string.  Wrap it in BytesIO so
        # pandas/openpyxl/xlrd can seek over it exactly as they would a real
        # file handle.  A `filename` keyword argument is accepted so the caller
        # can pass the original UploadFile.filename and preserve extension-based
        # routing without touching the rest of the load logic.
        #
        # FastAPI usage example:
        #   @app.post("/upload")
        #   async def upload(file: UploadFile):
        #       raw = await file.read()
        #       pipeline = DynamicPreprocessingPipeline()
        #       df = pipeline.run(raw, filename=file.filename)
        if isinstance(source, (bytes, bytearray)):
            # [Part-2 Fix 1]: Hard size check — reject before any pd.read_* call.
            _size_mb = len(source) / (1024 ** 2)
            if _size_mb > self.MAX_FILE_SIZE_MB:
                raise ValueError(
                    f"Upload rejected: in-memory payload is {_size_mb:.1f} MB, "
                    f"which exceeds the hard limit of {self.MAX_FILE_SIZE_MB} MB. "
                    f"Stream the file in chunks or raise MAX_FILE_SIZE_MB on the "
                    f"pipeline instance before calling run()."
                )
            from io import BytesIO
            buffer = BytesIO(source)
            # filename must be supplied so we can derive the extension;
            # fall back to the instance-level hint set by run() if available.
            fname = getattr(self, "_upload_filename", None) or "upload.csv"
            ext   = os.path.splitext(fname)[-1].lower()
            log(f"  In-memory bytes upload detected (filename='{fname}', "
                f"ext='{ext}')  [Fix 73]", "INFO")
            if ext == ".csv":
                return self._read_csv_with_encoding_buffer(buffer)
            elif ext == ".xlsx":
                log("  Loading .xlsx from buffer with openpyxl engine  [Fix 73]", "INFO")
                return pd.read_excel(buffer, engine="openpyxl")
            elif ext == ".xls":
                try:
                    import xlrd  # noqa: F401
                except ImportError:
                    raise ImportError(
                        "Reading .xls files requires xlrd ≥ 2.0. "
                        "Install with:  pip install xlrd"
                    )
                log("  Loading .xls from buffer with xlrd engine  [Fix 73]", "INFO")
                return pd.read_excel(buffer, engine="xlrd")
            else:
                raise ValueError(
                    f"Unsupported file type '{ext}' in bytes upload. "
                    "Pass filename=<original_filename> to run()."
                )

        if isinstance(source, str):
            if not os.path.exists(source):
                raise FileNotFoundError(f"File not found: {source}")
            # [Part-2 Fix 1]: Hard size check — reject before any pd.read_* call.
            _size_mb = os.path.getsize(source) / (1024 ** 2)
            if _size_mb > self.MAX_FILE_SIZE_MB:
                raise ValueError(
                    f"Upload rejected: '{os.path.basename(source)}' is "
                    f"{_size_mb:.1f} MB, which exceeds the hard limit of "
                    f"{self.MAX_FILE_SIZE_MB} MB. Stream the file in chunks "
                    f"or raise MAX_FILE_SIZE_MB on the pipeline instance before "
                    f"calling run()."
                )
            ext = os.path.splitext(source)[-1].lower()
            if ext == ".csv":
                return self._read_csv_with_encoding(source)
            elif ext == ".xlsx":
                # Fix 65 [F-01]: Explicit openpyxl engine for .xlsx.
                log("  Loading .xlsx with openpyxl engine  [Fix 65]", "INFO")
                return pd.read_excel(source, engine="openpyxl")
            elif ext == ".xls":
                # Fix 65 [F-01]: xlrd required for legacy .xls.
                try:
                    import xlrd  # noqa: F401
                except ImportError:
                    raise ImportError(
                        "Reading .xls files requires xlrd ≥ 2.0. "
                        "Install with:  pip install xlrd"
                    )
                log("  Loading .xls with xlrd engine  [Fix 65]", "INFO")
                return pd.read_excel(source, engine="xlrd")
            else:
                raise ValueError(f"Unsupported file type '{ext}'.")
        raise TypeError(
            "Invalid source. Pass a file path (str), a DataFrame, "
            "or raw bytes with filename= keyword.  [Fix 73]"
        )

    def _read_csv_with_encoding_buffer(self, buffer) -> pd.DataFrame:
        """
        Fix 73 [F-09]: Encoding-resilient CSV reader that accepts a BytesIO
        buffer (from an in-memory FastAPI upload) instead of a file path.
        Mirrors _read_csv_with_encoding() exactly — tries each encoding in
        ENCODING_FALLBACKS in order so Arabic / UTF-8 / Latin-1 datasets all
        work without any change in the calling code.
        """
        import io
        raw_bytes = buffer.read()
        last_err  = None
        for enc in list(self.ENCODING_FALLBACKS):
            try:
                text_io = io.StringIO(raw_bytes.decode(enc))
                df      = pd.read_csv(text_io, low_memory=False)
                self.report["file_encoding"] = enc
                log(f"  In-memory CSV decoded with encoding '{enc}'  [Fix 73]",
                    "SUCCESS")
                return df
            except (UnicodeDecodeError, Exception) as e:
                last_err = e
        raise ValueError(
            f"Could not decode in-memory CSV with any of "
            f"{list(self.ENCODING_FALLBACKS)}.  Last error: {last_err}"
        )

    def _read_csv_with_encoding(self, path: str) -> pd.DataFrame:
        file_size_mb = os.path.getsize(path) / (1024 ** 2)
        if file_size_mb > self.FILE_SIZE_WARN_MB:
            log(f"  Large file detected ({file_size_mb:.1f} MB). "
                f"Consider Polars or chunked loading for production.", "WARNING")

        encodings_to_try = list(self.ENCODING_FALLBACKS)
        try:
            import chardet
            sample_size = min(100_000, os.path.getsize(path))
            with open(path, "rb") as fh:
                raw_bytes = fh.read(sample_size)
            detected     = chardet.detect(raw_bytes)
            detected_enc = detected.get("encoding")
            confidence   = detected.get("confidence", 0)
            if detected_enc:
                encodings_to_try = [detected_enc] + [
                    e for e in encodings_to_try
                    if e.lower() != detected_enc.lower()
                ]
                log(f"  chardet detected encoding: {detected_enc} "
                    f"(confidence: {confidence:.0%})", "INFO")
        except ImportError:
            log("  chardet not installed — using fallback encoding chain.", "WARNING")

        last_error = None
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(path, encoding=enc)
                self.report["file_encoding"] = enc
                log(f"  File loaded with encoding: {enc}", "SUCCESS")
                return df
            except (UnicodeDecodeError, LookupError) as exc:
                last_error = exc

        raise ValueError(
            f"Could not decode '{os.path.basename(path)}' with any known encoding. "
            f"Last error: {last_error}"
        )

    # ──────────────────────────────────────────────────
    #  STEP 1 : AUTO TYPE DETECTION & CASTING
    # ──────────────────────────────────────────────────
    def _step_detect_and_cast_types(self, df):
        _logger.info("")
        log("STEP 1 — Automated Type Detection & Casting", "STEP")
        _logger.info("  " + "─" * 52)

        ID_KEYWORDS = {"id", "code", "no", "num", "number", "key", "ref"}

        for col in df.columns:
            original_dtype = str(df[col].dtype)
            col_lower      = col.lower().replace(" ", "_")

            is_id_col = any(
                kw == col_lower
                or col_lower.endswith(f"_{kw}")
                or col_lower.startswith(f"{kw}_")
                for kw in ID_KEYWORDS
            )

            if is_id_col:
                df[col] = df[col].astype(str).str.strip().where(
                    df[col].notna(), other=np.nan
                )
                self._id_cols.add(col)
                if any(kw in col_lower for kw in self.CASE_SENSITIVE_KEYWORDS):
                    self._case_sensitive_cols.add(col)
                if col not in self.report["columns_detected"]["text"]:
                    self.report["columns_detected"]["text"].append(col)
                self.report["type_conversions"][col] = (
                    f"{original_dtype} → string  (ID preserved as immutable string)"
                )
                log(f"  [{col}]  identified as ID column → preserved as string", "INFO")
                continue

            if self._looks_like_date(df[col]):
                try:
                    cleaned = df[col].astype(str).str.strip()
                    cleaned[cleaned.str.lower().isin({
                        "not_a_date", "n/a", "na", "null", "none", "nan",
                        "missing", "unknown", "invalid", "error", "-", "--",
                        "لا يوجد", "غير معروف", "فارغ", "مفقود", "خطأ", "غير صالح"
                    })] = np.nan
                    df[col] = self._to_datetime_robust(cleaned)
                    self.report["columns_detected"]["date"].append(col)
                    self.report["type_conversions"][col] = (
                        f"{original_dtype} → datetime64[UTC→naive]"
                    )
                    log(f"  [{col}]  {original_dtype} → datetime64 (UTC-normalized)",
                        "SUCCESS")
                    continue
                except (ValueError, TypeError):
                    pass  # Fall through to object→numeric coercion branch

            if df[col].dtype == object:
                if self._detect_european_decimal(df[col]):
                    log(f"  [{col}]  European decimal format detected  [Fix 22]",
                        "WARNING")
                    stripped = (
                        df[col].astype(str)
                        .str.replace(r'[$€£¥₹\s]', '', regex=True)
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        .str.strip()
                        .map(self._word_to_number_map)
                    )
                else:
                    stripped = (
                        df[col].astype(str)
                        .str.replace(",",  "", regex=False)
                        .str.replace("%",  "", regex=False)
                        .str.replace(r'[$€£¥₹]', '', regex=True)
                        .str.strip()
                        .map(self._word_to_number_map)
                    )

                converted    = pd.to_numeric(stripped, errors="coerce")
                non_null     = df[col].notna().sum()
                converted_ok = converted.notna().sum()

                if non_null > 0 and (converted_ok / non_null) >= 0.80:
                    df[col] = converted
                    self.report["columns_detected"]["numeric"].append(col)
                    self.report["type_conversions"][col] = f"{original_dtype} → numeric"
                    clean_vals = converted.dropna()
                    if len(clean_vals) > 0 and (clean_vals % 1 == 0).all():
                        self._integer_intent_cols.add(col)
                    log(f"  [{col}]  {original_dtype} → numeric  (auto-converted)",
                        "SUCCESS")
                    continue

            if pd.api.types.is_bool_dtype(df[col]):
                if col not in self.report["columns_detected"]["boolean"]:
                    self.report["columns_detected"]["boolean"].append(col)
            elif pd.api.types.is_numeric_dtype(df[col]):
                # Fix 66 [F-02]: Detect compact 8-digit YYYYMMDD integer dates
                # (e.g. 20101229) BEFORE classifying as numeric.  Without this
                # check the column is registered as integer and missing values
                # are later imputed with a mathematical average — a nonsensical
                # operation on calendar offsets.
                if self._looks_like_yyyymmdd(df[col]):
                    try:
                        df[col] = pd.to_datetime(
                            df[col].dropna().astype(int).astype(str),
                            format="%Y%m%d", errors="coerce"
                        ).reindex(df.index)
                        self.report["columns_detected"]["date"].append(col)
                        self.report["type_conversions"][col] = (
                            f"{original_dtype} → datetime64  (YYYYMMDD integer)  [Fix 66]"
                        )
                        log(f"  [{col}]  {original_dtype} → datetime64  "
                            f"(YYYYMMDD compact integer)  [Fix 66]", "SUCCESS")
                        continue
                    except (ValueError, TypeError):
                        pass  # Fall through to normal numeric registration
                if col not in self.report["columns_detected"]["numeric"]:
                    self.report["columns_detected"]["numeric"].append(col)
                if pd.api.types.is_integer_dtype(df[col]):
                    self._integer_intent_cols.add(col)
                elif len(df[col].dropna()) > 0 and (df[col].dropna() % 1 == 0).all():
                    self._integer_intent_cols.add(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                if col not in self.report["columns_detected"]["date"]:
                    self.report["columns_detected"]["date"].append(col)
            else:
                if self._looks_like_boolean(df[col]):
                    df[col] = df[col].map(lambda x: True  if str(x).strip().lower()
                              in {"true","yes","1","صح","نعم"} else
                              (False if str(x).strip().lower()
                              in {"false","no","0","غلط","لا"} else x))
                    if col not in self.report["columns_detected"]["boolean"]:
                        self.report["columns_detected"]["boolean"].append(col)
                    self.report["type_conversions"][col] = f"{original_dtype} → boolean"
                    log(f"  [{col}]  {original_dtype} → boolean  (auto-detected)",
                        "SUCCESS")
                else:
                    if any(kw in col_lower for kw in self.CASE_SENSITIVE_KEYWORDS):
                        self._case_sensitive_cols.add(col)
                    self.report["columns_detected"]["text"].append(col)

        _logger.info("")
        log(f"  Numeric columns  : {len(self.report['columns_detected']['numeric'])}", "INFO")
        log(f"  Text columns     : {len(self.report['columns_detected']['text'])}", "INFO")
        log(f"  Date columns     : {len(self.report['columns_detected']['date'])}", "INFO")
        log(f"  Boolean columns  : {len(self.report['columns_detected']['boolean'])}", "INFO")
        if self._id_cols:
            log(f"  ID columns (protected): {sorted(self._id_cols)}", "INFO")
        if self._case_sensitive_cols:
            log(f"  Case-sensitive cols   : {sorted(self._case_sensitive_cols)}", "INFO")
        if self._integer_intent_cols:
            log(f"  Integer-intent        : {sorted(self._integer_intent_cols)}", "INFO")
        return df

    # ── Utility: UTC datetime parser ──────────────────────────────────────
    @staticmethod
    def _to_datetime_robust(series):
        for kwargs in [
            {"format": "mixed", "dayfirst": False, "errors": "coerce", "utc": True},
            {"errors": "coerce", "utc": True},
        ]:
            try:
                result = pd.to_datetime(series, **kwargs)
                if result.dt.tz is not None:
                    result = result.dt.tz_convert(None)
                return result
            except (TypeError, ValueError):
                continue
        return pd.to_datetime(series, errors="coerce")

    @staticmethod
    def _detect_european_decimal(series) -> bool:
        sample = (
            series.dropna().astype(str).str.strip()
            .str.replace(r'[$€£¥₹\s]', '', regex=True)
            .head(500)
        )
        eu_count = sample.str.match(r'^-?(\d{1,3}\.)*\d{1,3},\d+$').sum()
        us_count = sample.str.match(r'^-?(\d{1,3},)*\d{1,3}\.\d+$').sum()
        return int(eu_count) > int(us_count) and int(eu_count) > 0

    def _looks_like_date(self, series):
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        if series.dtype != object:
            return False
        GARBAGE = {
            "not_a_date","n/a","na","null","none","nan","missing","unknown",
            "invalid","error","-","--","n.a","nd",
            "لا يوجد","غير معروف","فارغ","مفقود","خطأ","غير صالح"
        }
        clean = series.dropna().astype(str).str.strip()
        clean = clean[~clean.str.lower().isin(GARBAGE)]
        if len(clean) == 0:
            return False
        return (self._to_datetime_robust(clean).notna().sum() / len(clean)) >= 0.40

    def _looks_like_boolean(self, series):
        if series.dtype == object:
            BOOL = {"true","false","yes","no","1","0","صح","غلط","نعم","لا"}
            sample = series.dropna().astype(str).str.strip().str.lower().head(50)
            return bool(len(sample)) and all(v in BOOL for v in sample.unique())
        return False

    @staticmethod
    def _looks_like_yyyymmdd(series) -> bool:
        """
        Fix 66 [F-02]: Detect 8-digit integer columns that encode dates in the
        compact YYYYMMDD format (e.g. 20101229).  Returns True only when ≥ 80 %
        of non-null values are integers in the valid YYYYMMDD range and actually
        parse cleanly, so pure numeric IDs with 8 digits are not misclassified.
        """
        if not pd.api.types.is_numeric_dtype(series):
            return False
        sample = series.dropna()
        if len(sample) == 0:
            return False
        # Must be 8-digit integers: 19000101 ≤ v ≤ 21001231
        in_range = sample[(sample >= 19_000_101) & (sample <= 21_001_231)]
        if len(in_range) / len(sample) < 0.80:
            return False
        # Verify they actually parse as YYYYMMDD
        try:
            parsed = pd.to_datetime(
                in_range.astype(int).astype(str), format="%Y%m%d", errors="coerce"
            )
            return (parsed.notna().sum() / len(in_range)) >= 0.80
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _word_to_number_map(val):
        WORDS = {
            "zero":"0","one":"1","two":"2","three":"3","four":"4","five":"5",
            "six":"6","seven":"7","eight":"8","nine":"9","ten":"10",
            "eleven":"11","twelve":"12","thirteen":"13","fourteen":"14",
            "fifteen":"15","sixteen":"16","seventeen":"17","eighteen":"18",
            "nineteen":"19","twenty":"20","thirty":"30","forty":"40",
            "fifty":"50","hundred":"100",
        }
        return WORDS.get(str(val).strip().lower(), val)

    @staticmethod
    def _find_column_by_keywords(df, numeric_cols, keywords, exclude=None):
        ID_KW = {"id","code","index","num","no","number","key","ref"}
        if exclude is None:
            exclude = set()
        best_col, best_score = None, 0
        for col in numeric_cols:
            if col not in df.columns or col in exclude:
                continue
            cl = col.lower().replace(" ","_")
            if any(
                kw == cl or cl.endswith(f"_{kw}") or cl.startswith(f"{kw}_")
                for kw in ID_KW
            ):
                continue
            score = sum(3 if kw == cl else (1 if kw in cl else 0) for kw in keywords)
            if score > best_score:
                best_score, best_col = score, col
        return best_col if best_score > 0 else None

    def _is_id_column(self, col: str) -> bool:
        return col in self._id_cols

    def _is_monetary_column(self, col: str) -> bool:
        cl = col.lower().replace(" ", "_")
        return any(kw in cl for kw in self.MONETARY_KEYWORDS)

    def _track_imputation(self, df, col: str, was_null_mask) -> None:
        filled_idx = set(df.index[was_null_mask].tolist())
        if filled_idx:
            self._imputed_rows[col] = self._imputed_rows.get(col, set()) | filled_idx

    @staticmethod
    def _unique_col_name(base: str, existing_cols) -> str:
        if base not in existing_cols:
            return base
        counter = 2
        while f"{base}_{counter}" in existing_cols:
            counter += 1
        return f"{base}_{counter}"

    # ──────────────────────────────────────────────────
    #  STEP 2 : REMOVE DUPLICATES
    # ──────────────────────────────────────────────────
    def _step_remove_duplicates(self, df):
        _logger.info("")
        log("STEP 2 — Duplicate Removal & Primary Key Integrity", "STEP")
        _logger.info("  " + "─" * 52)
        before  = len(df)
        df      = df.drop_duplicates()
        removed = before - len(df)
        self.report["duplicates_removed"] = removed
        if removed > 0:
            log(f"  Removed {removed:,} duplicate rows.", "WARNING")
        else:
            log("  No duplicate rows found.", "SUCCESS")

        # Fix 70 [F-06]: Primary Key integrity — validate every column that
        # Step 1 flagged as an ID column.  Two fatal violations are checked:
        #   1. NULL values  — a PK cell must never be empty.
        #   2. Duplicate values — a PK must be unique across all rows.
        # Violations are recorded in report["pk_violations"] for the caller
        # to act on (raise / filter / alert) without the pipeline silently
        # swallowing them.  The pipeline does NOT drop rows or impute PK cells
        # — PK repair requires business-level knowledge the pipeline lacks.
        pk_violations: dict = {}
        for col in list(self._id_cols):
            if col not in df.columns:
                continue
            n_null = int(df[col].isna().sum())
            n_dup  = int(df[col].duplicated(keep=False).sum())
            if n_null == 0 and n_dup == 0:
                log(f"  [{col}]  PK integrity OK — no nulls, no duplicates  "
                    f"[Fix 70]", "SUCCESS")
                continue
            entry: dict = {}
            if n_null:
                entry["null_count"] = n_null
                log(f"  [{col}]  ⚠ PK VIOLATION — {n_null:,} null value(s) "
                    f"detected  [Fix 70]", "WARNING")
            if n_dup:
                entry["duplicate_count"] = n_dup
                dup_vals = (
                    df.loc[df[col].duplicated(keep=False), col]
                    .dropna().unique().tolist()[:10]        # show first 10 examples
                )
                entry["sample_duplicates"] = dup_vals
                log(f"  [{col}]  ⚠ PK VIOLATION — {n_dup:,} duplicate value(s) "
                    f"detected  (sample: {dup_vals})  [Fix 70]", "WARNING")
            pk_violations[col] = entry
        self.report["pk_violations"] = pk_violations
        if pk_violations:
            log(f"  Total PK columns with violations: {len(pk_violations)}  "
                f"→ see report['pk_violations']  [Fix 70]", "WARNING")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 3 : TEXT NORMALIZATION
    # ──────────────────────────────────────────────────
    def _step_text_normalization(self, df):
        _logger.info("")
        log("STEP 3 — Text Normalization", "STEP")
        _logger.info("  " + "─" * 52)
        text_cols = self.report["columns_detected"]["text"]
        if not text_cols:
            log("  No text columns found — skipping.", "INFO")
            return df

        bool_cols = self.report["columns_detected"]["boolean"]
        SYMBOLIC_GARBAGE = {
            "??","?","n/a","na","null","none","nan","missing","unknown",
            "-","--","n.a","nd","invalid","error","not available",
            "not applicable","undefined","tbd",
            "لا يوجد","غير معروف","فارغ","مفقود","خطأ","غير صالح",
        }

        for col in text_cols:
            if col not in df.columns or col in bool_cols:
                continue

            mask      = df[col].notna()
            col_lower = col.lower().replace(" ", "_")
            # Fix 67 [F-03]: Any column in self._id_cols was explicitly
            # identified as an identifier in Step 1 and must NEVER be
            # lowercased — identifiers like sls_ord_num can carry
            # semantically meaningful case (e.g. "ORD-001" vs "ord-001").
            # Previously only CASE_SENSITIVE_KEYWORDS triggered this guard,
            # so order-number columns whose names contain "num" slipped
            # through and were silently force-lowercased.
            is_cs     = (col in self._case_sensitive_cols
                         or col in self._id_cols
                         or any(kw in col_lower for kw in self.CASE_SENSITIVE_KEYWORDS))

            if is_cs:
                df.loc[mask, col] = (
                    df.loc[mask, col].astype(str)
                    .str.strip()
                    .str.replace(r'\s+', ' ', regex=True)
                )
                if col not in self._case_sensitive_cols:
                    self._case_sensitive_cols.add(col)
                log(f"  [{col}]  whitespace trimmed (case preserved)  [Fix 25]", "INFO")
            else:
                df.loc[mask, col] = (
                    df.loc[mask, col].astype(str).str.strip().str.lower()
                    .str.replace(r'\s+', ' ', regex=True)
                )
                log(f"  [{col}]  normalized (NaN preserved)", "SUCCESS")

            check_vals   = df[col].str.lower()
            garbage_mask = df[col].notna() & check_vals.isin(SYMBOLIC_GARBAGE)
            n = int(garbage_mask.sum())
            if n > 0:
                df.loc[garbage_mask, col] = np.nan
                log(f"  [{col}]  {n:,} symbolic garbage value(s) → NaN", "WARNING")

        return df

    # ──────────────────────────────────────────────────
    #  STEP 3.2 : CATEGORICAL STANDARDISATION
    #  Fix 71 [F-07]: Consolidate mixed representations of known categorical
    #  domains (gender, country, marital status) into canonical labels so
    #  downstream grouping / encoding produces correct cardinality.
    #
    #  Design notes
    #  ─────────────────────────────────────────────────────────────────────
    #  • Only LOW-CARDINALITY columns (≤ 20 unique values after text
    #    normalisation) are eligible — high-cardinality free-text columns are
    #    skipped to avoid false positives on product descriptions, city names etc.
    #  • A column must match a DOMAIN_TRIGGERS keyword in its name AND have at
    #    least one value that appears in the domain's synonym map.  Both
    #    conditions must hold so that a column named "customer_gender_confirmed"
    #    with values ["yes","no"] is NOT remapped via the gender table.
    #  • ID columns are never touched.
    #  • The pipeline logs every remapping so the caller has a full audit trail.
    # ──────────────────────────────────────────────────
    def _step_categorical_standardization(self, df):
        _logger.info("")
        log("STEP 3.2 — Categorical Standardisation  [Fix 71]", "STEP")
        _logger.info("  " + "─" * 52)

        # ── Domain tables ──────────────────────────────────────────────────
        # Keys are already lowercase (Step 3 lowercased text columns).
        # Each tuple: (trigger_keywords_for_col_name, synonym_dict)
        #
        # [Part-3 Fix 5]:
        #   • Gender / Marital Status canonical values are now lowercase to
        #     enforce uniform downstream encoding and grouping.
        #   • Country synonyms are mapped to standardised numeric codes so the
        #     analytics layer can join on an integer foreign-key rather than
        #     fragile free-text strings.
        DOMAINS = [
            (
                {"gen", "gender", "sex"},
                {
                    # Canonical → lowercase
                    "m": "male",   "male": "male",   "man": "male",   "boy": "male",
                    "mr": "male",  "mr.": "male",
                    "f": "female", "female": "female", "woman": "female",
                    "girl": "female", "ms": "female", "ms.": "female",
                    "mrs": "female",  "mrs.": "female",
                    "nb": "non-binary", "non-binary": "non-binary",
                    "nonbinary": "non-binary", "non binary": "non-binary",
                    "other": "other", "unknown": "other",
                },
            ),
            (
                {"marital", "marital_status", "civil", "relationship"},
                {
                    # Canonical → lowercase
                    "s": "single",   "single": "single",   "unmarried": "single",
                    "m": "married",  "married": "married", "wed": "married",
                    "d": "divorced", "divorced": "divorced",
                    "w": "widowed",  "widowed": "widowed",  "widow": "widowed",
                    "widower": "widowed",
                    "sep": "separated", "separated": "separated",
                },
            ),
            (
                {"country", "nation", "nationality", "country_name", "ctry"},
                {
                    # United States → "1"
                    "usa": "1",   "us": "1",
                    "u.s.a": "1", "u.s.": "1",
                    "united states": "1",
                    "united states of america": "1",
                    "america": "1",
                    # United Kingdom → "3"
                    "uk": "3",   "u.k.": "3",
                    "united kingdom": "3",
                    "great britain": "3",  "britain": "3",
                    "england": "3",
                    # United Arab Emirates → "4"
                    "uae": "4",
                    "u.a.e": "4",
                    "united arab emirates": "4",
                    # Egypt → "5"
                    "eg": "5", "egy": "5", "egypt": "5",
                    "مصر": "5",
                    # Saudi Arabia → "6"
                    "ksa": "6", "sa": "6",
                    "saudi": "6", "saudi arabia": "6",
                    "المملكة العربية السعودية": "6",
                    # Germany → "2"
                    "de": "2", "deu": "2", "germany": "2",
                    "deutschland": "2",
                    # France → "7"
                    "fr": "7",  "fra": "7",  "france": "7",
                    # China → "8"
                    "cn": "8",   "chn": "8",   "china": "8",
                    "prc": "8",
                    # India → "9"
                    "in": "9",   "ind": "9",   "india": "9",
                },
            ),
        ]

        MAX_CARDINALITY = 20
        text_cols       = self.report["columns_detected"]["text"]
        any_remapped    = False

        for col in text_cols:
            if col not in df.columns or self._is_id_column(col):
                continue
            n_unique = df[col].dropna().nunique()
            if n_unique > MAX_CARDINALITY:
                continue

            col_l = col.lower().replace(" ", "_")

            for trigger_kws, synonym_map in DOMAINS:
                # Column name must hint at this domain
                if not any(kw in col_l for kw in trigger_kws):
                    continue
                # At least one value must appear in the synonym table
                present_values = set(df[col].dropna().astype(str).str.strip().str.lower())
                if not present_values.intersection(synonym_map.keys()):
                    continue

                before_counts = df[col].value_counts().to_dict()

                # [v2.9 Fix B-06]: Step 3 already lowered + stripped non-CS
                # text columns.  The previous version called .astype(str)
                # .str.strip().str.lower() again and then used the
                # `v != "nan"` round-trip to recover NaN — wasteful and
                # fragile.  Use a typed helper that lowercases the key for
                # lookup but passes NaN/None through unchanged.
                def _norm(v, _smap=synonym_map):
                    if isinstance(v, str):
                        key = v.strip().lower()
                        return _smap.get(key, v)
                    return v   # NaN, None, numpy.nan all pass through

                df[col] = df[col].map(_norm)
                after_counts = df[col].value_counts().to_dict()
                n_remapped   = sum(
                    cnt for val, cnt in before_counts.items()
                    if str(val).strip().lower() in synonym_map
                )
                if n_remapped:
                    log(f"  [{col}]  {n_remapped:,} value(s) standardised  "
                        f"→  {sorted(after_counts.keys())}  [Fix 71]", "SUCCESS")
                    any_remapped = True
                break   # A column matches at most one domain

        if not any_remapped:
            log("  No low-cardinality categorical columns required standardisation.",
                "INFO")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 3.5 : POST-NORMALIZATION DATE RE-DETECTION
    # ──────────────────────────────────────────────────
    def _step_redetect_dates_after_normalization(self, df):
        _logger.info("")
        log("STEP 3.5 — Post-Normalization Date Re-Detection", "STEP")
        _logger.info("  " + "─" * 52)
        GARBAGE = {
            "not_a_date","n/a","na","null","none","nan","missing","unknown",
            "invalid","error","-","--","n.a","nd",
            "لا يوجد","غير معروف","فارغ","مفقود","خطأ","غير صالح"
        }
        rescued = []
        for col in list(self.report["columns_detected"]["text"]):
            if col not in df.columns or not self._looks_like_date(df[col]):
                continue
            try:
                cleaned = df[col].astype(str).str.strip()
                cleaned[cleaned.str.lower().isin(GARBAGE)] = np.nan
                df[col] = self._to_datetime_robust(cleaned)
                self.report["columns_detected"]["text"].remove(col)
                self.report["columns_detected"]["date"].append(col)
                self.report["type_conversions"][col] = (
                    "text → datetime64[UTC→naive]  (rescued in Step 3.5)"
                )
                rescued.append(col)
                log(f"  [{col}]  text → datetime64 UTC-normalized  (rescued)", "SUCCESS")
            except (ValueError, TypeError, AttributeError) as exc:
                log(f"  [{col}]  re-detection failed: {exc}", "WARNING")
        if not rescued:
            log("  No additional date columns detected.", "INFO")
        return df

    # ══════════════════════════════════════════════════
    #  STEP 3.7 — AUTONOMOUS RELATIONAL DISCOVERY ENGINE
    #
    #  Fix 54 [C-01]: Law discovery restricted to fit-mask rows only.
    #    notna_arr / float_arr now built from df_fit = df[_fit_mask].
    #    This prevents test-period financial patterns from distorting the
    #    discovered formulas used to impute training-period nulls.
    #
    #  Fix 55 [C-02]: target_col excluded from the eligible column set.
    # ══════════════════════════════════════════════════
    def _step_relational_discovery(self, df):
        _logger.info("")
        log("STEP 3.7 — Autonomous Relational Discovery Engine", "STEP")
        _logger.info("  " + "─" * 52)

        CONFIDENCE_THRESHOLD = 0.99
        MIN_SUPPORT          = 50
        RELATIVE_EPSILON     = 1e-4
        EPSILON_FLOOR        = 1e-9
        MAX_COLS             = 15
        RNG_SEED             = 42

        n_rows               = len(df)
        MAX_SAMPLE_PER_TRIPLE = min(2_000, max(500, n_rows // 50))
        log(f"  Adaptive sample cap: {MAX_SAMPLE_PER_TRIPLE} rows/triple "
            f"(dataset: {n_rows:,} rows)  [Fix 45]", "INFO")

        # Fix 54 [C-01]: Restrict discovery to fit rows.
        # The sample universe is the training portion only.
        df_fit = df[self._fit_mask] if self._fit_mask is not None else df
        n_fit  = len(df_fit)
        if n_fit < MIN_SUPPORT:
            log(f"  Fit rows ({n_fit}) < MIN_SUPPORT ({MIN_SUPPORT}) — "
                f"skipping discovery.", "WARNING")
            self._discovered_triads = []
            self.report["non_triadic_bypass"] = True
            return df
        log(f"  Discovery scoped to {n_fit:,} fit-mask rows  [C-01]", "INFO")

        numeric_cols = self.report["columns_detected"]["numeric"]
        ID_KW        = {"id","code","index","num","no","number","key","ref"}

        eligible = []
        for col in numeric_cols:
            if col not in df_fit.columns or col in self._id_cols:
                continue
            # Fix 55 [C-02]: Never include the target column in law discovery.
            if self._target_col and col == self._target_col:
                log(f"  [{col}]  target column — excluded from discovery  [C-02]",
                    "INFO")
                continue
            cl = col.lower().replace(" ","_")
            if any(
                kw == cl or cl.endswith(f"_{kw}") or cl.startswith(f"{kw}_")
                for kw in ID_KW
            ):
                continue
            if df_fit[col].dropna().nunique() <= 1:
                log(f"  [{col}]  constant column — excluded  [Fix 44]", "WARNING")
                continue
            eligible.append(col)

        if len(eligible) < 3:
            log("  Fewer than 3 eligible numeric columns — skipping discovery.", "INFO")
            log("  Dataset will be treated as Non-Triadic.", "WARNING")
            self._discovered_triads = []
            self.report["non_triadic_bypass"] = True
            return df

        eligible.sort(key=lambda c: df_fit[c].notna().sum(), reverse=True)
        if len(eligible) > MAX_COLS:
            eligible = eligible[:MAX_COLS]
            log(f"  Columns capped at {MAX_COLS} for O(N³) bound.", "WARNING")
        else:
            log(f"  Eligible columns ({len(eligible)}): {eligible}", "INFO")

        n_cols        = len(eligible)
        n_triples     = n_cols * (n_cols - 1) * (n_cols - 2)
        total_probes  = n_triples * 4
        log(f"  {n_cols} cols → {n_triples:,} triples × 4 ops = "
            f"{total_probes:,} probes", "INFO")

        # Arrays built from fit rows only (C-01)
        notna_arr = {col: df_fit[col].notna().values       for col in eligible}
        float_arr = {col: df_fit[col].astype(float).values for col in eligible}
        rng       = np.random.default_rng(seed=RNG_SEED)

        all_candidates        = []
        seen_commutative: set = set()

        # [Part-2 Fix 2]: Wall-clock deadline — no signal, safe in worker threads.
        import time as _time
        _discovery_deadline = _time.time() + self.DISCOVERY_TIMEOUT_SECONDS
        _discovery_timed_out = False

        for x_col, y_col, z_col in itertools.permutations(eligible, 3):
            # [Part-2 Fix 2]: Check deadline at the top of every iteration.
            if _time.time() > _discovery_deadline:
                _discovery_timed_out = True
                break

            clean_mask = notna_arr[x_col] & notna_arr[y_col] & notna_arr[z_col]
            n_clean    = int(clean_mask.sum())
            if n_clean < MIN_SUPPORT:
                continue

            clean_indices = np.where(clean_mask)[0]
            if len(clean_indices) > MAX_SAMPLE_PER_TRIPLE:
                clean_indices = rng.choice(clean_indices,
                                           size=MAX_SAMPLE_PER_TRIPLE,
                                           replace=False)

            X = float_arr[x_col][clean_indices]
            Y = float_arr[y_col][clean_indices]
            Z = float_arr[z_col][clean_indices]
            Z_denom = np.maximum(np.abs(Z), EPSILON_FLOOR)

            def _confidence(result_arr, z_arr=Z, z_denom=Z_denom):
                residual = np.abs(z_arr - result_arr) / z_denom
                finite   = np.isfinite(residual)
                n_valid  = int(finite.sum())
                if n_valid == 0:
                    return 0.0, 0
                hits = int(((residual < RELATIVE_EPSILON) & finite).sum())
                return hits / n_valid, n_valid

            ops_to_test = []

            add_key = (frozenset({x_col, y_col}), z_col, "addition")
            if add_key not in seen_commutative:
                conf, sup = _confidence(X + Y)
                ops_to_test.append(("addition", conf, sup, add_key))

            conf, sup = _confidence(X - Y)
            ops_to_test.append(("subtraction", conf, sup, None))

            mul_key = (frozenset({x_col, y_col}), z_col, "multiplication")
            if mul_key not in seen_commutative:
                conf, sup = _confidence(X * Y)
                ops_to_test.append(("multiplication", conf, sup, mul_key))

            nonzero_y = np.abs(Y) > EPSILON_FLOOR
            n_nonzero = int(nonzero_y.sum())
            if n_nonzero >= MIN_SUPPORT:
                X_d = X[nonzero_y]; Y_d = Y[nonzero_y]; Z_d = Z[nonzero_y]
                Z_d_denom = np.maximum(np.abs(Z_d), EPSILON_FLOOR)
                conf, sup = _confidence(X_d / Y_d, z_arr=Z_d, z_denom=Z_d_denom)
                ops_to_test.append(("division", conf, sup, None))

            for op_name, conf, sup, dedup_key in ops_to_test:
                if conf < CONFIDENCE_THRESHOLD or sup < MIN_SUPPORT:
                    continue
                if dedup_key is not None:
                    seen_commutative.add(dedup_key)
                can_be_neg = bool((Z < 0).any())
                label = (
                    f"[{x_col}] "
                    f"{'+' if op_name=='addition' else '−' if op_name=='subtraction' else '×' if op_name=='multiplication' else '÷'} "
                    f"[{y_col}] = [{z_col}]"
                )
                all_candidates.append({
                    "x_col": x_col, "y_col": y_col, "z_col": z_col,
                    "op_name": op_name, "label": label,
                    "confidence": round(conf, 5), "support": sup,
                    "can_be_negative": can_be_neg,
                })

        # [Part-2 Fix 2]: If the wall-clock deadline fired, discard all partial
        # candidates and exit the step gracefully.  The pipeline continues with
        # non_triadic_bypass=True so Steps 3.8 and 4.8 are safely skipped.
        if _discovery_timed_out:
            log(
                f"  Discovery timeout ({self.DISCOVERY_TIMEOUT_SECONDS}s wall-clock) "
                f"reached — partial candidates discarded.  "
                f"Steps 3.8 and 4.8 will be bypassed.  [Part-2 Fix 2]",
                "WARNING",
            )
            self.report["non_triadic_bypass"] = True
            self._discovered_triads = []
            return df

        OP_PRIORITY = {"multiplication": 3, "addition": 2,
                       "subtraction": 1,    "division": 0}
        best_per_derived: dict = {}

        for cand in all_candidates:
            z        = cand["z_col"]
            existing = best_per_derived.get(z)
            if existing is None:
                best_per_derived[z] = cand
            else:
                cand_score     = (cand["confidence"],     OP_PRIORITY.get(cand["op_name"], 0))
                existing_score = (existing["confidence"], OP_PRIORITY.get(existing["op_name"], 0))
                if cand_score > existing_score:
                    self.report["discovery_conflicts"].append({
                        "derived":   z,
                        "winner":    {"law": cand["label"],     "confidence": cand["confidence"]},
                        "displaced": {"law": existing["label"], "confidence": existing["confidence"]},
                    })
                    best_per_derived[z] = cand
                else:
                    self.report["discovery_conflicts"].append({
                        "derived":   z,
                        "winner":    {"law": existing["label"], "confidence": existing["confidence"]},
                        "displaced": {"law": cand["label"],     "confidence": cand["confidence"]},
                    })

        OP_FAMILY    = {"addition": "additive",        "subtraction":    "additive",
                        "multiplication": "multiplicative", "division": "multiplicative"}
        PREFERRED_OP = {"additive": "addition",        "multiplicative": "multiplication"}

        seen_canonical: dict = {}
        deduplicated:   list = []

        for cand in best_per_derived.values():
            cols_key  = frozenset({cand["x_col"], cand["y_col"], cand["z_col"]})
            family    = OP_FAMILY.get(cand["op_name"], cand["op_name"])
            canon_key = (cols_key, family)

            if canon_key not in seen_canonical:
                seen_canonical[canon_key] = cand
                deduplicated.append(cand)
            else:
                existing     = seen_canonical[canon_key]
                preferred_op = PREFERRED_OP.get(family)
                if preferred_op and cand["op_name"] == preferred_op \
                        and existing["op_name"] != preferred_op:
                    deduplicated.remove(existing)
                    deduplicated.append(cand)
                    seen_canonical[canon_key] = cand
                    self.report["discovery_conflicts"].append({
                        "type":    "circular_deduplication",
                        "columns": sorted(cols_key),
                        "kept":    {"law": cand["label"],     "op": cand["op_name"]},
                        "dropped": {"law": existing["label"], "op": existing["op_name"]},
                        "reason":  "promoted to preferred forward direction",
                    })
                else:
                    self.report["discovery_conflicts"].append({
                        "type":    "circular_deduplication",
                        "columns": sorted(cols_key),
                        "kept":    {"law": existing["label"], "op": existing["op_name"]},
                        "dropped": {"law": cand["label"],     "op": cand["op_name"]},
                        "reason":  "algebraic rearrangement of already-registered law",
                    })

        n_dedup = len(best_per_derived) - len(deduplicated)
        if n_dedup > 0:
            log(f"  Circular dedup: {n_dedup} rearrangement(s) collapsed → "
                f"{len(deduplicated)} unique law(s).", "WARNING")

        discovered = []
        for cand in deduplicated:
            triad = self._make_discovered_triad(
                cand["x_col"], cand["y_col"], cand["z_col"],
                cand["op_name"], cand["label"],
                cand["confidence"], cand["support"],
                cand["can_be_negative"],
            )
            if triad:
                discovered.append(triad)
                self.report["discovered_laws"].append({
                    "law":            cand["label"],
                    "operation":      cand["op_name"],
                    "base_a":         cand["x_col"],
                    "base_b":         cand["y_col"],
                    "derived":        cand["z_col"],
                    "confidence":     cand["confidence"],
                    "support":        cand["support"],
                    "can_be_negative": cand["can_be_negative"],
                })
                log(f"  LAW REGISTERED  {cand['label']}  "
                    f"confidence={cand['confidence']:.4f}  "
                    f"support={cand['support']}", "SUCCESS")

        self._discovered_triads = discovered

        n_laws      = len(discovered)
        n_conflicts = len(self.report["discovery_conflicts"])
        _logger.info("")
        if n_laws == 0:
            log("  No mathematical laws discovered at ≥99% confidence.", "INFO")
            log("  Non-Triadic — Steps 3.8 and 4.8 bypassed.  [Fix 15]", "WARNING")
            self.report["non_triadic_bypass"] = True
        else:
            log(f"  {n_laws} law(s) discovered.", "SUCCESS")
            if n_conflicts:
                log(f"  {n_conflicts} conflict(s) resolved.", "WARNING")
        return df

    @staticmethod
    def _make_discovered_triad(x_col, y_col, z_col, op_name, label,
                                confidence, support, can_be_negative):
        """
        [Part-3 Fix 1] Build a triad descriptor that is fully JSON-serialisable.

        Formulas are referenced by a string opkey (e.g. "addition") instead of
        lambda closures.  The module-level _OP_DISPATCH dict maps each opkey to
        the actual callables, which are looked up at evaluation time by
        _step_relational_imputation() and _enforce_triadic_constraints().

        This removes the last un-serialisable field from the triad dict so
        save_json_report() can round-trip every discovered law without
        silently falling back to str(val).
        """
        if op_name not in _OP_DISPATCH:
            return None

        return {
            "label":           label,
            "base_a":          x_col,
            "base_b":          y_col,
            "derived":         z_col,
            "op":              op_name,   # serialisable string opkey
            "confidence":      confidence,
            "support":         support,
            "can_be_negative": can_be_negative,
            "source":          "discovered",
        }

    def _detect_triads(self, df):
        if self._discovered_triads is None:
            return []
        return self._discovered_triads

    # ──────────────────────────────────────────────────
    #  STEP 3.8 : RELATIONAL IMPUTATION
    #  Fix 55 [C-02]: Skip target column in forward + inverse imputation.
    # ──────────────────────────────────────────────────
    def _step_relational_imputation(self, df):
        _logger.info("")
        log("STEP 3.8 — Relational Imputation (Algebraic Column Dependencies)", "STEP")
        _logger.info("  " + "─" * 52)

        triads       = self._detect_triads(df)
        total_filled = 0

        if not triads:
            if self.report.get("non_triadic_bypass"):
                log("  Non-Triadic dataset — relational imputation bypassed.", "INFO")
            else:
                log("  No triads available — skipping.", "INFO")
            return df

        log(f"  Using {len(triads)} discovered triad(s).", "INFO")

        def _apply_safe(mask, col_to_fill, values, label, can_neg=False):
            nonlocal total_filled
            # Fix 55 [C-02]: Never impute the target column via inverse formula.
            if self._target_col and col_to_fill == self._target_col:
                return
            if not mask.any():
                return
            valid = mask & values.notna() & (True if can_neg else (values >= 0))
            n     = int(valid.sum())
            if n == 0:
                return
            deduced = values[valid]
            if col_to_fill in self._integer_intent_cols:
                deduced = deduced.round(0)
            was_null_before = df[col_to_fill].isna() & valid
            df.loc[valid, col_to_fill] = deduced
            self._track_imputation(df, col_to_fill, was_null_before)
            self.report["relational_imputation"][col_to_fill] = (
                self.report["relational_imputation"].get(col_to_fill, 0) + n
            )
            total_filled += n
            log(f"  [{col_to_fill}]  {n} value(s) recovered via {label}", "SUCCESS")

        for triad in triads:
            a_col   = triad["base_a"]
            b_col   = triad["base_b"]
            d_col   = triad["derived"]
            can_neg = triad.get("can_be_negative", False)

            if not all(c in df.columns for c in [a_col, b_col, d_col]):
                continue

            # [Part-3 Fix 1]: Dispatch formula and inverses from _OP_DISPATCH
            # using the serialisable opkey stored on the triad dict.
            op_fns  = _OP_DISPATCH.get(triad["op"])
            if op_fns is None:
                log(f"  Unknown op '{triad['op']}' in triad — skipped.", "WARNING")
                continue

            a = df[a_col]; b = df[b_col]; d = df[d_col]
            log(f"  {triad['label']}", "INFO")

            # Derived missing — both bases known
            # Fix 55 [C-02]: Skip if derived col is target
            if not (self._target_col and d_col == self._target_col):
                computed = op_fns["formula"](a, b)
                fwd_mask = d.isna() & a.notna() & b.notna()
                if fwd_mask.any():
                    valid = fwd_mask & computed.notna() & (True if can_neg else (computed >= 0))
                    n     = int(valid.sum())
                    if n > 0:
                        deduced = computed[valid]
                        if d_col in self._integer_intent_cols:
                            deduced = deduced.round(0)
                        was_null_before = df[d_col].isna() & valid
                        df.loc[valid, d_col] = deduced
                        self._track_imputation(df, d_col, was_null_before)
                        self.report["relational_imputation"][d_col] = (
                            self.report["relational_imputation"].get(d_col, 0) + n
                        )
                        total_filled += n
                        log(f"  [{d_col}]  {n} value(s) recovered via formula",
                            "SUCCESS")

            _apply_safe(a.isna() & d.notna() & b.notna(), a_col,
                         op_fns["inv_a"](d, b), f"inverse (÷ {b_col})", can_neg=False)
            _apply_safe(b.isna() & d.notna() & a.notna(), b_col,
                         op_fns["inv_b"](d, a), f"inverse (÷ {a_col})", can_neg=False)

        _logger.info("")
        if total_filled == 0:
            log("  Triads detected — no rows had exactly one missing member.", "INFO")
        else:
            log(f"  Total values recovered via relational algebra: {total_filled}",
                "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 3.9 : CUSTOM RULE IMPUTATION
    #  Fix 62 [M-01]: Pre-validation pass surfaces missing-column errors in
    #    the JSON report (report["custom_rule_errors"]) rather than silently
    #    producing an all-NaN feature with no indication in the output.
    # ──────────────────────────────────────────────────
    def _step_custom_rule_imputation(self, df):
        _logger.info("")
        log("STEP 3.9 — Custom Rule Imputation (User-Injected Business Equations)",
            "STEP")
        _logger.info("  " + "─" * 52)

        if not self._custom_rules:
            log("  No custom rules provided — skipping.", "INFO")
            return df

        log(f"  {len(self._custom_rules)} rule(s) registered  [Fix 50]", "INFO")

        # ── Fix 62 [M-01]: Pre-validation ─────────────────────────────────
        # Run each rule on a single-row probe DataFrame to detect missing
        # column references before touching the real data.  A KeyError here
        # means the rule references a column that doesn't exist yet — record
        # it in the structured error list and skip the rule entirely.
        valid_rules = {}
        for derived_col, rule_fn in self._custom_rules.items():
            probe = df.head(1)
            try:
                result = rule_fn(probe)
                _ = pd.Series(result, index=probe.index)    # coercibility check
                valid_rules[derived_col] = rule_fn
                log(f"  [{derived_col}]  pre-validation passed  [M-01]", "INFO")
            except KeyError as ke:
                missing_col = str(ke).strip("'\"")
                err = {
                    "rule":        derived_col,
                    "error_type":  "KeyError",
                    "missing_col": missing_col,
                    "action":      "rule skipped — column not found in dataset",
                }
                self.report["custom_rule_errors"].append(err)
                log(
                    f"  [{derived_col}]  PRE-VALIDATION FAILED — "
                    f"column '{missing_col}' not found in dataset.  "
                    f"Rule skipped.  Check for typos.  [M-01]",
                    "ERROR",
                )
            except Exception as exc:
                err = {
                    "rule":       derived_col,
                    "error_type": type(exc).__name__,
                    "detail":     str(exc),
                    "action":     "rule skipped — execution error on probe row",
                }
                self.report["custom_rule_errors"].append(err)
                log(
                    f"  [{derived_col}]  PRE-VALIDATION FAILED — "
                    f"{type(exc).__name__}: {exc}.  Rule skipped.  [M-01]",
                    "ERROR",
                )

        if not valid_rules:
            log("  All custom rules failed pre-validation — skipping.", "WARNING")
            return df

        total_filled = 0

        for derived_col, rule_fn in valid_rules.items():
            log(f"  Applying rule → [{derived_col}]", "INFO")

            try:
                computed = rule_fn(df)
            except Exception as exc:
                log(f"  [{derived_col}]  rule execution failed: {exc} — skipped.",
                    "WARNING")
                continue

            if not isinstance(computed, pd.Series):
                try:
                    computed = pd.Series(computed, index=df.index)
                except Exception as exc:
                    log(f"  [{derived_col}]  coercion failed: {exc} — skipped.",
                        "WARNING")
                    continue

            if derived_col not in df.columns:
                log(f"  [{derived_col}]  column not found — creating new column.",
                    "WARNING")
                df[derived_col] = np.nan
                if derived_col not in self.report["columns_detected"]["numeric"]:
                    self.report["columns_detected"]["numeric"].append(derived_col)

            null_mask = df[derived_col].isna()
            fill_mask = null_mask & computed.notna()
            n         = int(fill_mask.sum())

            if n == 0:
                log(f"  [{derived_col}]  no nulls to fill — already complete.",
                    "SUCCESS")
                continue

            df.loc[fill_mask, derived_col] = computed[fill_mask]
            self._track_imputation(df, derived_col, null_mask & fill_mask)
            self.report["custom_rule_imputation"][derived_col] = (
                self.report["custom_rule_imputation"].get(derived_col, 0) + n
            )
            total_filled += n
            log(f"  [{derived_col}]  {n:,} null(s) filled via custom rule  [Fix 50]",
                "SUCCESS")

        _logger.info("")
        if total_filled == 0:
            log("  Custom rules executed — no null cells needed filling.", "INFO")
        else:
            log(f"  Total values filled by custom rules: {total_filled:,}", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 4.9 : TOTAL SALES DERIVATION
    #
    #  [Part-3 Fix 5]: When a "Sales"-flavoured numeric column and a
    #  "Quantity"-flavoured numeric column are both present, compute
    #  "Total Sales" = Sales × Quantity and add it as a derived column.
    #
    #  [v2.9 Fix B-02]: Detection now uses anchored substring matching
    #  (kw == cl | cl.endswith(_kw) | cl.startswith(kw_) | _kw_ inside cl)
    #  so realistic names like Total_Sales, Sales_Amount, Net_Revenue,
    #  Revenue_EGP, and Order_Qty are matched.  ID-keyword columns
    #  (sales_id, qty_id, etc.) are explicitly excluded.
    #
    #  [v2.9 Fix B-04]: This step now runs at Step 4.9, AFTER Step 4
    #  imputation and Step 4.8 constraint sync, so any null Quantity
    #  values resolved earlier in the pipeline contribute to the
    #  derivation.  Sales itself is monetary (Fix 72) and may still be
    #  NaN; rows with NaN Sales correctly produce NaN Total Sales for
    #  manual review rather than fabricated values.
    # ──────────────────────────────────────────────────
    def _step_total_sales(self, df):
        log("STEP 4.9 — Total Sales Derivation  [v2.9 B-02 / B-04]", "STEP")

        SALES_KW    = {"sales", "sale", "revenue", "مبيعات"}
        QUANTITY_KW = {"quantity", "qty", "units", "الكمية"}
        # NOTE: "count" deliberately excluded from QUANTITY_KW — too generic;
        # matches click_count, row_count, error_count, etc.
        ID_KW       = {"id", "code", "index", "num", "no", "number", "key", "ref"}
        numeric_cols = set(self.report["columns_detected"]["numeric"])

        target_col = "Total Sales"
        if target_col in df.columns:
            log(f"  '{target_col}' already exists — skipping.", "INFO")
            return df

        def _anchored_match(col_name: str, keywords: set) -> bool:
            """
            Anchored substring match used by both the keyword test and the
            ID-exclusion test.  A keyword 'kw' matches column 'col_name' iff
            (after lower+underscore normalisation) one of:
              • kw equals the entire name        ('sales'         → 'sales')
              • the name ends with '_{kw}'       ('total_sales'   → 'sales')
              • the name starts with '{kw}_'     ('sales_amount'  → 'sales')
              • the name contains '_{kw}_'       ('net_sales_egp' → 'sales')
            This avoids substring false positives like 'count' inside
            'click_count' matching the 'count' keyword while still catching
            every realistic compound name an analyst would write.
            """
            cl = col_name.lower().replace(" ", "_")
            return any(
                kw == cl
                or cl.endswith(f"_{kw}")
                or cl.startswith(f"{kw}_")
                or f"_{kw}_" in f"_{cl}_"
                for kw in keywords
            )

        def _matches_kw_excluding_ids(col_name: str, keywords: set) -> bool:
            # Exclude any column whose name signals it is an identifier.
            if _anchored_match(col_name, ID_KW):
                return False
            return _anchored_match(col_name, keywords)

        sales_col = next(
            (c for c in df.columns
             if c in numeric_cols
             and c != target_col
             and _matches_kw_excluding_ids(c, SALES_KW)),
            None,
        )
        qty_col = next(
            (c for c in df.columns
             if c in numeric_cols
             and c != target_col
             and c != sales_col
             and _matches_kw_excluding_ids(c, QUANTITY_KW)),
            None,
        )

        if sales_col is None or qty_col is None:
            log("  'Sales' or 'Quantity' column not detected — skipping.  "
                "[v2.9 B-02 anchored match]", "INFO")
            return df

        df[target_col] = df[sales_col] * df[qty_col]
        if target_col not in self.report["columns_detected"]["numeric"]:
            self.report["columns_detected"]["numeric"].append(target_col)
        n_filled = int(df[target_col].notna().sum())
        n_null   = int(df[target_col].isna().sum())
        log(
            f"  '{target_col}' = [{sales_col}] × [{qty_col}]  →  "
            f"{n_filled:,} computed, {n_null:,} NaN (source NaN — Fix 72 monetary)  "
            f"[v2.9 B-02/B-04]",
            "SUCCESS",
        )
        return df

    def _apply_custom_rules_fill(self, df) -> int:
        if not self._custom_rules:
            return 0
        total = 0
        for derived_col, rule_fn in self._custom_rules.items():
            if derived_col not in df.columns:
                continue
            try:
                computed = rule_fn(df)
                if not isinstance(computed, pd.Series):
                    computed = pd.Series(computed, index=df.index)
            except (ValueError, TypeError, KeyError):
                continue
            fill_mask = df[derived_col].isna() & computed.notna()
            n         = int(fill_mask.sum())
            if n > 0:
                df.loc[fill_mask, derived_col] = computed[fill_mask]
                self.report["custom_rule_imputation"][derived_col] = (
                    self.report["custom_rule_imputation"].get(derived_col, 0) + n
                )
                total += n
                log(f"  [{derived_col}]  {n:,} null(s) re-filled via custom rule "
                    f"(constraint sync pass)  [Fix 50]", "INFO")
        return total

    # ──────────────────────────────────────────────────
    #  STEP 4 : SMART MISSING VALUE IMPUTATION
    #
    #  Fix 54 [C-01]: median and mode computed on fit-mask rows only,
    #    then applied to all rows.  Prevents test-set values from
    #    contaminating training-set imputations.
    #
    #  Fix 55 [C-02]: target column skipped entirely.
    #
    #  Fix 59 [H-02]: boolean columns skipped in the numeric branch;
    #    they fall through to the mode-fill text branch below.
    # ──────────────────────────────────────────────────
    def _step_clean_missing_values(self, df):
        _logger.info("")
        log("STEP 4 — Smart Missing Value Imputation", "STEP")
        _logger.info("  " + "─" * 52)
        total_missing = df.isnull().sum().sum()
        if total_missing == 0:
            log("  No missing values found! Dataset is complete.", "SUCCESS")
            return df

        log(f"  Total missing cells detected: {total_missing:,}", "WARNING")
        log(f"  Sparsity threshold: {self.SPARSITY_THRESHOLD:.0%}  [Fix 24]", "INFO")
        log(f"  Statistics computed on fit-mask rows only  [C-01]", "INFO")
        _logger.info("")

        ID_KEYWORDS = {"id","code","index","num","no","number","key","ref"}
        bool_cols   = set(self.report["columns_detected"]["boolean"])

        for col in df.columns:
            n_missing = df[col].isnull().sum()
            if n_missing == 0:
                continue
            pct = (n_missing / len(df)) * 100
            cl  = col.lower().replace(" ","_")

            # Fix 55 [C-02]: Never impute the prediction target.
            if self._target_col and col == self._target_col:
                log(f"  [{col}]  target column — imputation skipped  [C-02]", "INFO")
                continue

            if self._is_id_column(col) or any(
                kw == cl or cl.endswith(f"_{kw}") or cl.startswith(f"{kw}_")
                for kw in ID_KEYWORDS
            ):
                log(f"  [{col}]  ID column — {n_missing:,} missing ({pct:.1f}%)  "
                    f"→  kept as NaN  [Fix 9/16]", "WARNING")
                continue

            if pd.api.types.is_datetime64_any_dtype(df[col]):
                log(f"  [{col}]  date column — NaT kept as-is", "WARNING")
                continue

            if (pct / 100) > self.SPARSITY_THRESHOLD:
                log(f"  [{col}]  {pct:.1f}% missing — sparse (>{self.SPARSITY_THRESHOLD:.0%}) "
                    f"— skipped  [Fix 24]", "WARNING")
                self.report["skipped_sparse_cols"].append(col)
                self.report["missing_filled"][col] = {
                    "count":    int(n_missing),
                    "pct":      round(pct, 2),
                    "strategy": f"SKIPPED — sparse (>{self.SPARSITY_THRESHOLD:.0%} missing)",
                }
                continue

            was_null = df[col].isnull()

            # Fix 54 [C-01]: Compute fill statistic on fit rows only.
            fit_col = df.loc[self._fit_mask, col] if self._fit_mask is not None else df[col]

            # Fix 59 [H-02]: Boolean columns must NOT use numeric median.
            # pd.api.types.is_numeric_dtype returns True for bool dtype,
            # so without this guard a 50/50 boolean column gets median=0.5
            # which either coerces all nulls to True or corrupts the dtype.
            if col in bool_cols or pd.api.types.is_bool_dtype(df[col]):
                mode_val = fit_col.mode()
                if mode_val.empty:
                    fill_val = False
                    strategy = "False (empty mode)"
                else:
                    fill_val = mode_val[0]
                    strategy = f"mode ({fill_val})  [H-02 boolean guard]"
                df[col] = df[col].fillna(fill_val)

            elif pd.api.types.is_numeric_dtype(df[col]):
                # Fix 72 [F-08]: Financial/monetary columns must NEVER be
                # imputed with a statistical average.  Filling prd_cost,
                # price, or revenue with the dataset median silently inflates
                # or deflates aggregate financial reports and is functionally
                # incorrect.  Monetary nulls are preserved as NaN so the
                # caller can apply relational logic (e.g. unit_price × qty)
                # or leave the cell blank in the output for manual review.
                #
                # [v2.9 Fix B-01]: The previous version fell through to the
                # _track_imputation() / missing_filled[col] block at the
                # bottom of the loop, which marked monetary cells as
                # "imputed" though no fillna had run, inflating the
                # "Total Imputed Cell-Rows Tracked" KPI and producing a
                # contradictory SUCCESS log ("filled with NaN preserved").
                # Fix: record under a separate report["missing_preserved"]
                # bucket and `continue` past the shared tracking block.
                if self._is_monetary_column(col):
                    self.report.setdefault("missing_preserved", {})[col] = {
                        "count":  int(n_missing),
                        "pct":    round(pct, 2),
                        "reason": "monetary — statistical imputation never applied  [Fix 72]",
                    }
                    log(f"  [{col}]  monetary — {n_missing:,} missing value(s) "
                        f"left as NaN (no imputation)  [Fix 72 / v2.9 B-01]",
                        "WARNING")
                    continue
                else:
                    fill_val = fit_col.median()
                    df[col]  = df[col].fillna(fill_val)
                    strategy = f"median ({fill_val:.4g})  [C-01 fit-only]"

            else:
                if fit_col.mode().empty:
                    df[col]  = df[col].fillna("Unknown")
                    strategy = "Unknown"
                else:
                    fill_val = fit_col.mode()[0]
                    df[col]  = df[col].fillna(fill_val)
                    strategy = f"mode ('{fill_val}')  [C-01 fit-only]"

            self._track_imputation(df, col, was_null)
            self.report["missing_filled"][col] = {
                "count":    int(n_missing),
                "pct":      round(pct, 2),
                "strategy": strategy,
            }
            log(f"  [{col}]  {n_missing:,} missing ({pct:.1f}%)  "
                f"→  filled with {strategy}", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  SHARED CONSTRAINT ENFORCEMENT CORE
    # ──────────────────────────────────────────────────
    def _enforce_triadic_constraints(self, df) -> int:
        triads          = self._detect_triads(df)
        total_corrected = 0

        for triad in triads:
            a_col = triad["base_a"]
            b_col = triad["base_b"]
            d_col = triad["derived"]
            if not all(c in df.columns for c in [a_col, b_col, d_col]):
                continue

            a = df[a_col]; b = df[b_col]; d = df[d_col]
            both_known = a.notna() & b.notna()
            # [Part-3 Fix 1]: Dispatch formula at runtime via serialisable opkey.
            op_fns = _OP_DISPATCH.get(triad["op"])
            if op_fns is None:
                log(f"  Unknown op '{triad['op']}' in triad — skipped.", "WARNING")
                continue
            recalc = op_fns["formula"](a, b)

            diff_mask = both_known & d.isna() & recalc.notna()
            n         = int(diff_mask.sum())

            if n == 0:
                log(f"  {triad['label']}  →  already consistent ✓", "SUCCESS")
                continue

            synced = recalc[diff_mask]
            if d_col in self._integer_intent_cols:
                synced = synced.round(0)
            df.loc[diff_mask, d_col] = synced
            self.report["constraint_sync"][d_col] = (
                self.report["constraint_sync"].get(d_col, 0) + n
            )
            total_corrected += n
            log(f"  {triad['label']}", "INFO")
            log(f"  [{d_col}]  {n:,} null(s) filled to satisfy arithmetic constraint "
                f"[Fix 49]", "WARNING")

        custom_filled   = self._apply_custom_rules_fill(df)
        total_corrected += custom_filled
        return total_corrected

    def _step_post_imputation_constraint_sync(self, df):
        _logger.info("")
        log("STEP 4.8 — Post-Imputation Constraint Sync  [null-fill only]", "STEP")
        _logger.info("  " + "─" * 52)
        has_triads = bool(self._detect_triads(df))
        has_rules  = bool(self._custom_rules)
        if not has_triads and not has_rules:
            if self.report.get("non_triadic_bypass"):
                log("  Non-Triadic — bypassed.  [Fix 15]", "INFO")
            else:
                log("  No triads or custom rules — nothing to sync.", "INFO")
            return df
        log("  Filling null derived-column cells where both bases are known.  "
            "[Fix 49]", "INFO")
        total_corrected = self._enforce_triadic_constraints(df)
        _logger.info("")
        if total_corrected == 0:
            log("  All relational constraints already satisfied.", "SUCCESS")
        else:
            log(f"  Total cells corrected: {total_corrected:,}", "SUCCESS")
        return df

    def _step_post_outlier_constraint_sync(self, df):
        _logger.info("")
        log("STEP 5.5 — Post-Outlier Constraint Re-Sync  [null-fill only]  [Fix 27]",
            "STEP")
        _logger.info("  " + "─" * 52)
        has_triads = bool(self._detect_triads(df))
        has_rules  = bool(self._custom_rules)
        if not has_triads and not has_rules:
            log("  No triads or custom rules — skipping.", "INFO")
            return df
        log("  Re-enforcing arithmetic laws after IQR capping …", "INFO")
        total_corrected = self._enforce_triadic_constraints(df)
        _logger.info("")
        if total_corrected == 0:
            log("  All constraints consistent after outlier capping ✓", "SUCCESS")
        else:
            log(f"  {total_corrected:,} cell(s) re-synced after IQR capping.", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 4.5 : BUSINESS LOGIC VALIDATION
    #
    #  Fix 57 [C-04]: Vectorized return-keyword detection.
    #    The old .apply(lambda v: any(kw in v for kw in KEYWORDS)) ran a
    #    Python interpreter hop per row — ~30-90 s on 500 K rows.
    #    Replaced with str.contains(compiled_regex, na=False): single C-speed
    #    regex pass, <1 s on the same dataset.
    # ──────────────────────────────────────────────────
    def _step_business_logic_validation(self, df):
        _logger.info("")
        log("STEP 4.5 — Business Logic Validation (Non-Negative Columns)", "STEP")
        log("  Scope: physically impossible negatives only  [Fix 21]", "INFO")
        log("  Context-aware: legitimate returns/refunds preserved  [Fix 51]", "INFO")
        log("  Keyword detection: vectorized str.contains  [C-04]", "INFO")
        log("  NOTE: Runs before Step 4.8  [Fix 33]", "INFO")
        _logger.info("  " + "─" * 52)

        RETURN_STATUS_KEYWORDS = {
            "return", "returned", "refund", "refunded", "cancel", "cancelled",
            "canceled", "void", "voided", "reversal", "reversed", "credit",
            "chargeback", "مرتجع", "استرداد", "إلغاء", "مردود",
        }
        # Fix 57 [C-04]: Pre-compile a single regex from all keywords.
        # str.contains() calls the C-layer regex engine once per column,
        # not once per row × keyword.
        return_pattern = re.compile(
            "|".join(re.escape(kw) for kw in RETURN_STATUS_KEYWORDS),
            re.IGNORECASE,
        )

        FINANCIAL_CONFIRM_KEYWORDS = {
            "revenue", "total", "amount", "sales", "income", "profit",
            "payment", "price", "cost", "charge", "invoice", "earnings",
        }

        text_cols    = self.report["columns_detected"]["text"]
        numeric_cols = self.report["columns_detected"]["numeric"]
        any_fixed    = False

        financial_numeric_cols = [
            c for c in numeric_cols
            if c in df.columns
            and any(kw in c.lower() for kw in FINANCIAL_CONFIRM_KEYWORDS)
        ]
        status_text_cols = [c for c in text_cols if c in df.columns]

        for col in numeric_cols:
            if col not in df.columns:
                continue
            col_cl = col.lower().replace(" ", "_")
            if not any(kw in col_cl for kw in self.NON_NEGATIVE_KEYWORDS):
                continue

            neg_mask = df[col] < 0
            n_neg    = int(neg_mask.sum())
            if n_neg == 0:
                continue

            legitimate_mask = pd.Series(False, index=df.index)

            # (a) Financial sign agreement
            for fin_col in financial_numeric_cols:
                if fin_col == col:
                    continue
                legitimate_mask |= (neg_mask & (df[fin_col] < 0))

            # (b) Fix 57 [C-04]: Vectorized keyword check — one C-speed regex
            # pass per text column replaces N_rows × N_keywords Python hops.
            for txt_col in status_text_cols:
                keyword_hit = (
                    df[txt_col]
                    .astype(str)
                    .str.contains(return_pattern, na=False)
                )
                legitimate_mask |= (neg_mask & keyword_hit)

            error_mask   = neg_mask & ~legitimate_mask
            n_legitimate = int((neg_mask & legitimate_mask).sum())
            n_errors     = int(error_mask.sum())

            if n_legitimate > 0:
                log(f"  [{col}]  {n_legitimate:,} negative row(s) preserved "
                    f"as legitimate returns/refunds  [Fix 51]", "INFO")

            if n_errors == 0:
                if n_legitimate > 0:
                    self.report["business_logic_fixed"][col] = {
                        "negative_values_found": n_neg,
                        "preserved_as_return":   n_legitimate,
                        "replaced_as_error":     0,
                        "action": "all negatives legitimate — none replaced",
                    }
                continue

            df.loc[error_mask, col] = np.nan
            # Fix 54 [C-01]: use fit-mask median for re-imputation
            fit_col  = df.loc[self._fit_mask, col] if self._fit_mask is not None else df[col]
            fill_val = fit_col.median()
            df[col]  = df[col].fillna(fill_val)
            self._track_imputation(df, col, error_mask)

            self.report["business_logic_fixed"][col] = {
                "negative_values_found": n_neg,
                "preserved_as_return":   n_legitimate,
                "replaced_as_error":     n_errors,
                "action": f"errors replaced with fit-median ({fill_val:.4g}); "
                          f"returns preserved  [C-01+C-04]",
            }
            log(f"  [{col}]  {n_errors:,} error negative(s) → NaN → "
                f"re-imputed with fit-median ({fill_val:.4g})  [C-04]", "WARNING")
            any_fixed = True

        if not any_fixed:
            log("  No business logic violations detected.", "SUCCESS")

        # ── Fix 68 [F-04]: Temporal logic — start date must not exceed end date ──
        # Scan for date-column pairs whose names suggest a start/end relationship
        # and flag rows where start > end as NaT (rather than silently keeping
        # an impossible sequence that corrupts duration calculations downstream).
        #
        # [v2.9 Fix B-03]: The previous version used `kw in name` (unanchored
        # substring), so "to" matched "tomorrow_dt", "from" matched "frame_dt",
        # "end" matched "weekend_dt", etc.  False positives could NaT
        # legitimate date pairs whose values happened to satisfy A > B.
        # Replaced with anchored matching shared by both keyword sets.
        date_cols = self.report["columns_detected"]["date"]
        START_KW  = {"start", "begin", "from", "open", "issue", "prd_start"}
        END_KW    = {"end",   "close", "to",   "expire", "finish", "prd_end"}

        def _kw_anchored(name_lower: str, keywords: set) -> bool:
            """Anchored: equals | endswith _kw | startswith kw_ | contains _kw_."""
            return any(
                kw == name_lower
                or name_lower.endswith(f"_{kw}")
                or name_lower.startswith(f"{kw}_")
                or f"_{kw}_" in f"_{name_lower}_"
                for kw in keywords
            )

        for s_col in date_cols:
            if s_col not in df.columns:
                continue
            sc = s_col.lower().replace(" ", "_")
            if not _kw_anchored(sc, START_KW):
                continue
            for e_col in date_cols:
                if e_col == s_col or e_col not in df.columns:
                    continue
                ec = e_col.lower().replace(" ", "_")
                if not _kw_anchored(ec, END_KW):
                    continue
                both_valid = df[s_col].notna() & df[e_col].notna()
                bad_mask   = both_valid & (df[s_col] > df[e_col])
                n_bad      = int(bad_mask.sum())
                if n_bad == 0:
                    continue
                df.loc[bad_mask, s_col] = pd.NaT
                df.loc[bad_mask, e_col] = pd.NaT
                key = f"{s_col}_after_{e_col}"
                self.report["business_logic_fixed"][key] = {
                    "start_col":  s_col,
                    "end_col":    e_col,
                    "violations": n_bad,
                    "action":     "both dates set to NaT — start > end is impossible  [Fix 68]",
                }
                log(f"  [{s_col}] > [{e_col}]: {n_bad:,} temporal inversion(s) "
                    f"→ NaT  [Fix 68]", "WARNING")
                any_fixed = True

        # ── Fix 69 [F-05]: Extreme/impossible date values ────────────────────────
        # Dates far outside the realistic range (before 1900 or after 2100 for
        # birthdate/demographic columns; after today+5 years for all date columns)
        # are set to NaT.  The birthdate column receives a tighter upper bound
        # (today) because a future birthdate is always impossible.
        import datetime as _dt
        TODAY      = pd.Timestamp(_dt.date.today())
        # Fix 74 [F-10]: DATE_UPPER_BOUND_YEARS is now a configurable class
        # constant (default 10) rather than the previous hard-coded 5-year
        # ceiling.  Use-cases like credit card expiration dates legitimately
        # extend 8-10 years into the future, so 5 was too tight.  Override at
        # the class or instance level to suit the deployment context:
        #     DynamicPreprocessingPipeline.DATE_UPPER_BOUND_YEARS = 15
        MAX_FUTURE = TODAY + pd.DateOffset(years=self.DATE_UPPER_BOUND_YEARS)
        MIN_DATE   = pd.Timestamp("1900-01-01")
        BDATE_KW   = {"birth", "bdate", "dob", "born"}

        for col in date_cols:
            if col not in df.columns:
                continue
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            col_l   = col.lower().replace(" ", "_")
            is_bday = any(kw in col_l for kw in BDATE_KW)
            upper   = TODAY if is_bday else MAX_FUTURE

            extreme = df[col].notna() & ((df[col] < MIN_DATE) | (df[col] > upper))
            n_ext   = int(extreme.sum())
            if n_ext == 0:
                continue
            df.loc[extreme, col] = pd.NaT
            key = f"{col}_extreme_dates"
            self.report["business_logic_fixed"][key] = {
                "col":    col,
                "n_rows": n_ext,
                "bounds": f"{MIN_DATE.date()} – {upper.date()}",
                "action": "extreme/impossible dates → NaT  [Fix 69]",
            }
            label = "birthdate" if is_bday else "date"
            log(f"  [{col}]  {n_ext:,} extreme {label} value(s) outside "
                f"[{MIN_DATE.date()}, {upper.date()}] → NaT  [Fix 69]", "WARNING")
            any_fixed = True

        return df

    # ──────────────────────────────────────────────────
    #  STEP 5 : IQR OUTLIER DETECTION & HANDLING
    #
    #  Fix 54 [C-01]: IQR bounds computed on fit-mask rows only,
    #    then applied to all rows (clip() operates on the full df).
    #  Fix 55 [C-02]: target column never capped.
    # ──────────────────────────────────────────────────
    def _step_outlier_detection(self, df):
        _logger.info("")
        log("STEP 5 — IQR Outlier Detection & Capping", "STEP")
        log("  Financial columns fully exempt (no capping)  [Fix 31]", "INFO")
        log("  IQR bounds derived from fit-mask rows only  [C-01]", "INFO")
        _logger.info("  " + "─" * 52)
        numeric_cols = self.report["columns_detected"]["numeric"]
        if not numeric_cols:
            log("  No numeric columns — skipping.", "INFO")
            return df

        ID_KEYWORDS = {"id", "code", "index", "num", "no", "number", "key", "ref"}

        for col in numeric_cols:
            if col not in df.columns:
                continue

            col_cl = col.lower().replace(" ", "_")

            # Fix 55 [C-02]: Never cap the prediction target.
            if self._target_col and col == self._target_col:
                log(f"  [{col}]  target column — IQR capping skipped  [C-02]", "INFO")
                continue

            if self._is_id_column(col) or any(
                kw == col_cl or col_cl.endswith(f"_{kw}") or col_cl.startswith(f"{kw}_")
                for kw in ID_KEYWORDS
            ):
                log(f"  [{col}]  surrogate key — bypassed  [Fix 17]", "INFO")
                continue

            if self._is_monetary_column(col):
                log(f"  [{col}]  financial column — bypassed  [Fix 31/47]", "INFO")
                continue

            # Fix 54 [C-01]: Derive IQR statistics from fit rows only.
            fit_series = (
                df.loc[self._fit_mask, col].dropna()
                if self._fit_mask is not None
                else df[col].dropna()
            )
            if len(fit_series) < 10:
                continue
            Q1, Q3 = fit_series.quantile(0.25), fit_series.quantile(0.75)
            IQR    = Q3 - Q1
            if IQR == 0:
                continue

            skew   = abs(fit_series.skew())
            mult   = 3.0 if skew >= 2.0 else (2.0 if skew >= 1.0 else 1.5)
            lo, hi = Q1 - mult * IQR, Q3 + mult * IQR

            is_int = col in self._integer_intent_cols
            if is_int:
                lo, hi = math.floor(lo), math.ceil(hi)

            # Apply bounds to the full DataFrame (train + test)
            n_lo = int((df[col] < lo).sum())
            n_hi = int((df[col] > hi).sum())
            if n_lo + n_hi == 0:
                continue

            series_before = df[col].copy()
            df[col] = df[col].clip(lower=lo, upper=hi)
            changed_mask = series_before.notna() & (series_before != df[col])
            if changed_mask.any():
                self._capped_rows[col] = (
                    self._capped_rows.get(col, set())
                    | set(df.index[changed_mask].tolist())
                )

            if is_int:
                df[col] = df[col].round(0)
                try:    df[col] = df[col].astype(pd.Int64Dtype())
                except: df[col] = df[col].astype(np.int64)

            self.report["outliers_handled"][col] = {
                "total":     n_lo + n_hi, "low": n_lo, "high": n_hi,
                "lower_cap": round(lo, 4), "upper_cap": round(hi, 4),
                "fit_only_bounds": True,
            }
            log(f"  [{col}]  {n_lo+n_hi:,} outliers capped  "
                f"→  [{lo:.2f}, {hi:.2f}]  "
                f"[IQR×{mult} | skew={skew:.2f}]  [C-01 fit-bounds]"
                + ("  [integer bounds]" if is_int else ""), "WARNING")

        if not self.report["outliers_handled"]:
            log("  No significant outliers detected.", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 6 : AUTO DATE FEATURE ENGINEERING
    # ──────────────────────────────────────────────────
    def _step_date_feature_engineering(self, df):
        _logger.info("")
        log("STEP 6 — Auto Date Feature Engineering  [Fix 19: primary + delta]",
            "STEP")
        _logger.info("  " + "─" * 52)
        date_cols = self.report["columns_detected"]["date"]
        if not date_cols:
            log("  No date columns — skipping.", "INFO")
            return df

        primary_col         = self._select_primary_date_col(date_cols, df)
        self._primary_date_col = primary_col
        secondary_cols      = [c for c in date_cols
                               if c != primary_col and c in df.columns]

        log(f"  Primary date    : [{primary_col}]", "INFO")
        if secondary_cols:
            log(f"  Secondary dates : {secondary_cols}  → delta features only", "INFO")

        if primary_col in df.columns and pd.api.types.is_datetime64_any_dtype(
            df[primary_col]
        ):
            prefix   = primary_col.replace(" ","_")
            nat_mask = df[primary_col].isna()

            df[f"{prefix}_Year"]        = df[primary_col].dt.year
            df[f"{prefix}_Month"]       = df[primary_col].dt.month
            df[f"{prefix}_Day"]         = df[primary_col].dt.day
            df[f"{prefix}_DayOfWeek"]   = df[primary_col].dt.dayofweek
            df[f"{prefix}_Quarter"]     = df[primary_col].dt.quarter
            df[f"{prefix}_Is_Weekend"]  = (
                df[primary_col].dt.dayofweek.isin([5, 6]).fillna(False).astype(int)
            )
            df[f"{prefix}_Is_MonthEnd"] = (
                df[primary_col].dt.is_month_end.fillna(False).astype(int)
            )
            df[f"{prefix}_DayName"] = df[primary_col].dt.day_name()
            df[f"{prefix}_Season"]  = df[primary_col].dt.month.map(self._get_season)

            NUMERIC_FEATS = [
                f"{prefix}_Year", f"{prefix}_Month", f"{prefix}_Day",
                f"{prefix}_DayOfWeek", f"{prefix}_Quarter",
                f"{prefix}_Is_Weekend", f"{prefix}_Is_MonthEnd",
            ]
            TEXT_FEATS = [f"{prefix}_DayName", f"{prefix}_Season"]

            for feat in NUMERIC_FEATS:
                if nat_mask.any():
                    df.loc[nat_mask, feat] = -1
                df[feat] = df[feat].fillna(-1).astype(int)
            for feat in TEXT_FEATS:
                if nat_mask.any():
                    df.loc[nat_mask, feat] = "Unknown"

            if nat_mask.any():
                log(f"  [{primary_col}]  {int(nat_mask.sum())} NaT row(s) → "
                    f"numeric=-1 | text='Unknown'  [Fix 11]", "WARNING")

            new_features = NUMERIC_FEATS + TEXT_FEATS
            self.report["date_features_added"].extend(new_features)
            self.report["columns_detected"]["numeric"].extend(
                [f for f in NUMERIC_FEATS if f in df.columns]
            )

            log(f"  [{primary_col}]  →  {len(new_features)} features created",
                "SUCCESS")
            for feat in new_features:
                log(f"           + {feat}", "INFO")

        if secondary_cols and primary_col in df.columns:
            _logger.info("")
            log("  Secondary dates → delta_days features  [Fix 19]", "INFO")
            for col in secondary_cols:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue
                primary_name = primary_col.replace(" ","_")
                col_name     = col.replace(" ","_")
                delta_col    = f"{col_name}_days_from_{primary_name}"
                delta        = (df[col] - df[primary_col]).dt.days
                nat_either   = df[col].isna() | df[primary_col].isna()
                delta[nat_either] = -1
                df[delta_col]     = delta.fillna(-1).astype(int)
                if nat_either.any():
                    log(f"  [{col}]  {int(nat_either.sum())} NaT row(s) "
                        f"→ delta=-1  [Fix 11]", "WARNING")
                self.report["date_features_added"].append(delta_col)
                self.report["columns_detected"]["numeric"].append(delta_col)
                log(f"  [{col}]  →  [{delta_col}]  (delta days from primary)",
                    "SUCCESS")
        return df

    def _select_primary_date_col(self, date_cols: list, df) -> str:
        best_col, best_score = date_cols[0], -1.0
        for col in date_cols:
            if col not in df.columns:
                continue
            cl       = col.lower().replace(" ","_")
            kw_score = sum(2 for kw in self.PRIMARY_DATE_KEYWORDS if kw in cl)
            coverage = df[col].notna().sum() / max(len(df), 1)
            score    = kw_score + coverage
            if score > best_score:
                best_score, best_col = score, col
        log(f"  Primary date selected: [{best_col}]  (score: {best_score:.3f})",
            "INFO")
        return best_col

    @staticmethod
    def _get_season(month):
        if pd.isna(month): return np.nan
        if month in [12, 1, 2]:  return "Winter"
        elif month in [3, 4, 5]: return "Spring"
        elif month in [6, 7, 8]: return "Summer"
        else:                    return "Autumn"

    def _select_best_group_column(self, df, text_cols):
        for col in text_cols:
            if col not in df.columns:
                continue
            cl = col.lower().replace(" ","_")
            if any(kw in cl for kw in self.ENTITY_KEYWORDS):
                n = df[col].nunique()
                if n >= 2:
                    log(f"  Entity column: [{col}]  ({n:,} unique values)  "
                        f"[Fix 18]", "INFO")
                    return col
        HIGH = ["customer","client","product","category","segment","region","city",
                "brand","type","group","class","division","department","team",
                "partner","vendor","supplier","market","channel","zone","area",
                "عميل","منتج","فئة","قسم","منطقة","فرع","ماركة"]
        LOW  = ["status","state","flag","method","mode","phase","stage","level",
                "priority","rating","grade","rank","step",
                "حالة","مرحلة","طريقة","تقييم"]
        best_col, best_score = None, -1
        for col in text_cols:
            if col not in df.columns:
                continue
            n = df[col].nunique()
            if not (2 <= n <= 50):
                continue
            cl    = col.lower()
            score = (sum(10 for kw in HIGH if kw in cl)
                   + sum(-5 for kw in LOW  if kw in cl)
                   + (3 if 3 <= n <= 20 else 0))
            if score > best_score:
                best_score, best_col = score, col
        if best_col:
            log(f"  Best grouping column: [{best_col}]  "
                f"(score: {best_score}, unique: {df[best_col].nunique()})", "INFO")
        else:
            log("  No suitable grouping column — global lag.", "INFO")
        return best_col

    # ──────────────────────────────────────────────────
    #  STEP 6.5 : ADVANCED LAG FEATURES
    #
    #  Fix 56 [C-03]: Row order preserved.
    #    An __orig_order__ tracking column is added before any date-sort and
    #    dropped after a restoring sort_values at return time. The caller
    #    always receives a row-aligned DataFrame.
    #
    #  Fix 60 [H-03] + Fix 64 [M-03]: Unified -1 sentinel.
    #    ALL lag/inter-order features use -1 for every form of "no prior data
    #    available": NaT rows, first-in-group rows, and first global rows.
    #    Eliminates the 0/−1 ambiguity that made "no prior period" and
    #    "prior value was zero" indistinguishable.
    #
    #    ML NOTE: -1 is a sentinel only — it is NOT a valid lag value.
    #    Downstream, create a boolean mask feature `has_prior_{col}` from
    #    lag_{col} != -1 before feeding to a model that might misinterpret -1.
    # ──────────────────────────────────────────────────
    def _step_advanced_lag_features(self, df):
        _logger.info("")
        log("STEP 6.5 — Advanced Lag & Time-Gap Features", "STEP")
        _logger.info("  " + "─" * 52)
        date_cols    = self.report["columns_detected"]["date"]
        numeric_cols = self.report["columns_detected"]["numeric"]
        text_cols    = self.report["columns_detected"]["text"]
        if not date_cols:
            log("  No date columns — skipping.", "INFO")
            return df

        sort_col      = self._primary_date_col or date_cols[0]
        id_kw         = {"id","code","index","num","no","number","key"}
        valid_numeric = [
            c for c in numeric_cols
            if not any(kw in c.lower() for kw in id_kw)
            and not self._is_id_column(c)
            and c in df.columns
        ]
        if not valid_numeric:
            log("  No valid numeric columns for lag features — skipping.", "INFO")
            return df

        group_col = self._select_best_group_column(df, text_cols)

        # Fix 56 [C-03]: Stamp original positional order before any sorting.
        # We use a deterministic integer sequence so sort_values('__orig_order__')
        # at return time exactly reverses the sort — no index loss, no NaT
        # rows appended at the bottom.
        df = df.copy()
        df["__orig_order__"] = np.arange(len(df))

        nat_mask  = df[sort_col].isna()
        n_nat     = int(nat_mask.sum())
        df_valid  = df[~nat_mask].copy()
        df_nat    = df[nat_mask].copy()

        if n_nat > 0:
            log(f"  Temporarily excluding {n_nat} NaT rows from lag calculation",
                "WARNING")

        # Sort chronologically for lag computation; do NOT reset_index yet —
        # keeping the positional index intact lets __orig_order__ restore order.
        df_valid = df_valid.sort_values(by=sort_col)
        log(f"  Data sorted by [{sort_col}]", "INFO")

        new_features = []

        sort_col_safe    = sort_col.lower().replace(" ", "_")
        inter_order_base = f"days_since_last_{sort_col_safe}"
        inter_order_col  = self._unique_col_name(
            inter_order_base, set(df_valid.columns)
        )
        if inter_order_col != inter_order_base:
            log(f"  Column name collision — renamed '{inter_order_base}' "
                f"→ '{inter_order_col}'  [Fix 46]", "WARNING")

        if group_col:
            log(f"  Grouping by [{group_col}] "
                f"({df_valid[group_col].nunique():,} unique values)", "INFO")
            raw_gap = (
                df_valid
                .groupby(group_col)[sort_col]
                .diff()
                .dt.days
            )
        else:
            raw_gap = df_valid[sort_col].diff().dt.days

        # Fix 60 [H-03] / Fix 64 [M-03]: Use -1 for first-in-group (NaT diff
        # result), consistent with the NaT sentinel introduced in Fix 42.
        # The old fillna(0) made "first order for this customer" look identical
        # to "same-day repeat order" — a material distortion of time-series.
        df_valid[inter_order_col] = raw_gap.fillna(-1).astype(int)
        new_features.append(inter_order_col)
        log(f"  + {inter_order_col}  (sentinel -1 = first-in-group)  "
            f"[H-03]", "SUCCESS")

        for col in valid_numeric:
            cc        = col.lower().replace(" ","_")
            lag1_base = f"lag_1_{cc}"
            lag3_base = f"lag_3_{cc}"
            existing  = set(df_valid.columns)
            lag1_name = self._unique_col_name(lag1_base, existing)
            lag3_name = self._unique_col_name(lag3_base, existing | {lag1_name})

            if group_col:
                lag1_raw = df_valid.groupby(group_col)[col].shift(1)
                lag3_raw = df_valid.groupby(group_col)[col].shift(3)
            else:
                lag1_raw = df_valid[col].shift(1)
                lag3_raw = df_valid[col].shift(3)

            # Fix 60 [H-03]: fillna(-1) — first-in-group has no prior value.
            df_valid[lag1_name] = lag1_raw.fillna(-1)
            df_valid[lag3_name] = lag3_raw.fillna(-1)
            new_features.extend([lag1_name, lag3_name])
            log(f"  + {lag1_name}  |  {lag3_name}  (sentinel -1)  [H-03]",
                "SUCCESS")

        if n_nat > 0:
            for feat in new_features:
                # Fix 60 [H-03]: -1 sentinel for NaT rows (consistent with
                # both the inter-order and lag fillna convention above).
                df_nat[feat] = -1
            df_valid = pd.concat([df_valid, df_nat])
            log(f"  NaT rows re-merged with lag=-1 sentinel  [Fix 42/H-03]", "INFO")
        # else df_valid is already the complete result

        # Fix 56 [C-03]: Restore original row order.
        # sort_values('__orig_order__') exactly undoes the chronological sort
        # and the NaT-row append, returning a row-aligned DataFrame.
        df_valid = (
            df_valid
            .sort_values("__orig_order__")
            .drop(columns=["__orig_order__"])
            .reset_index(drop=True)
        )
        log(f"  Original row order restored  [C-03]", "INFO")

        numeric_lag = [
            f for f in new_features
            if f in df_valid.columns
            and pd.api.types.is_numeric_dtype(df_valid[f])
            and f not in self.report["columns_detected"]["numeric"]
        ]
        self.report["columns_detected"]["numeric"].extend(numeric_lag)
        log(f"  Registered {len(numeric_lag)} new numeric features for "
            f"correlation detection  [Fix 41]", "INFO")

        self.report["lag_features_added"] = new_features
        log(f"  Total new features: {len(new_features)}", "INFO")
        return df_valid

    # ──────────────────────────────────────────────────
    #  STEP 7 : MEMORY OPTIMIZATION
    # ──────────────────────────────────────────────────
    def _step_memory_optimization(self, df):
        _logger.info("")
        log("STEP 7 — Memory Optimization", "STEP")
        log("  Monetary columns exempt from float32 downcasting  [Fix 23]", "INFO")
        log("  Nullable integers downcast to nullable equivalents  [Fix 34]", "INFO")
        _logger.info("  " + "─" * 52)
        before_mb = df.memory_usage(deep=True).sum() / 1024**2
        optimized = []

        NULLABLE_INT_DTYPES = {'Int8','Int16','Int32','Int64',
                               'UInt8','UInt16','UInt32','UInt64'}

        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                mn, mx      = df[col].min(), df[col].max()
                dtype_str   = str(df[col].dtype)
                is_nullable = dtype_str in NULLABLE_INT_DTYPES

                if mn >= -32768 and mx <= 32767:
                    target = pd.Int16Dtype() if is_nullable else np.int16
                elif mn >= -2_147_483_648 and mx <= 2_147_483_647:
                    target = pd.Int32Dtype() if is_nullable else np.int32
                else:
                    optimized.append(col)
                    continue

                try:
                    df[col] = df[col].astype(target)
                    optimized.append(col)
                except (TypeError, ValueError, OverflowError) as e:
                    log(f"  [{col}]  downcast skipped ({e})", "WARNING")

            elif pd.api.types.is_float_dtype(df[col]):
                if self._is_monetary_column(col):
                    self.report["monetary_protected"].append(col)
                    log(f"  [{col}]  monetary — float64 preserved  [Fix 23/47]",
                        "INFO")
                else:
                    df[col] = df[col].astype(np.float32)
                    optimized.append(col)
            elif df[col].dtype == object:
                if df[col].nunique() / max(len(df), 1) < 0.5:
                    df[col] = df[col].astype("category")
                    optimized.append(col)

        after_mb  = df.memory_usage(deep=True).sum() / 1024**2
        saved_mb  = before_mb - after_mb
        saved_pct = (saved_mb / max(before_mb, 0.001)) * 100
        self.report["memory_optimization"] = {
            "before_mb":          round(before_mb, 3),
            "after_mb":           round(after_mb,  3),
            "saved_mb":           round(saved_mb,  3),
            "saved_pct":          round(saved_pct, 1),
            "cols_optimized":     len(optimized),
            "monetary_protected": len(self.report["monetary_protected"]),
        }
        log(f"  Before: {before_mb:.2f} MB  →  After: {after_mb:.2f} MB  "
            f"(saved {saved_mb:.2f} MB = {saved_pct:.1f}%)", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 8 : ENCODING REPORT
    # ──────────────────────────────────────────────────
    def _step_encoding_report(self, df):
        _logger.info("")
        log("STEP 8 — Encoding Report", "STEP")
        _logger.info("  " + "─" * 52)
        text_cols = self.report["columns_detected"]["text"]
        if not text_cols:
            log("  No text columns — skipping.", "INFO")
            return df
        bool_cols = self.report["columns_detected"]["boolean"]
        for col in text_cols:
            if col not in df.columns or col in bool_cols:
                continue
            if self._is_id_column(col):
                log(f"  [{col}]  ID column — no encoding needed  [Fix 16]", "INFO")
                continue
            n_unique = df[col].nunique()
            rec = ("One-Hot Encoding" if n_unique <= 10
                   else "Label Encoding" if n_unique <= 50
                   else "Target Encoding / Drop")
            self.report["encoding_report"][col] = {
                "unique_values": n_unique, "recommendation": rec
            }
            log(f"  [{col}]  {n_unique} unique values  →  recommend: {rec}", "INFO")
        return df

    # ──────────────────────────────────────────────────
    #  STEP 9 : CORRELATION DETECTION
    # ──────────────────────────────────────────────────
    def _step_correlation_detection(self, df):
        _logger.info("")
        log("STEP 9 — Correlation Detection", "STEP")
        _logger.info("  " + "─" * 52)
        nc = [c for c in self.report["columns_detected"]["numeric"] if c in df.columns]
        if len(nc) < 2:
            log("  Not enough numeric columns.", "INFO")
            return df
        log(f"  Evaluating {len(nc)} numeric columns  [Fix 41/43]", "INFO")
        pearson  = df[nc].corr(method="pearson").abs()
        spearman = df[nc].corr(method="spearman").abs()
        cols     = pearson.columns.tolist()
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                p, s = pearson.iloc[i, j], spearman.iloc[i, j]
                if p >= 0.85 or s >= 0.85:
                    self.report["high_correlations"].append({
                        "col_a": cols[i], "col_b": cols[j],
                        "pearson":  round(float(p), 3),
                        "spearman": round(float(s), 3),
                        "method": "pearson" if p >= s else "spearman",
                    })
                    log(f"  [{cols[i]}] & [{cols[j]}]  "
                        f"Pearson: {p:.3f}  Spearman: {s:.3f}  "
                        f"(consider dropping one)", "WARNING")
        if not self.report["high_correlations"]:
            log("  No high correlations detected.", "SUCCESS")
        return df

    # ──────────────────────────────────────────────────
    #  DATA QUALITY SCORE
    #
    #  Fix 63 [M-02]: Quality score floor REMOVED.
    #    The old code enforced after_score = max(before_score, ...).
    #    This guaranteed the score could never fall and turned the "after"
    #    number into a marketing metric rather than a real measurement.
    #    After score is now fully independent: it CAN be lower than before
    #    if the pipeline introduces new quality concerns (e.g. many new
    #    high-correlation lag features, or aggressive median imputation of
    #    a skewed column).  Operators should treat a negative delta as a
    #    signal to review imputation strategy or feature engineering settings.
    # ──────────────────────────────────────────────────
    def _calculate_quality_score(self, before_metrics: dict, df_after):
        """
        [Part-2 Fix 3]: Signature changed from (df_before, df_after) to
        (before_metrics, df_after).

        `before_metrics` is the lightweight dict captured in run() immediately
        after _load(), containing:
          - null_total  (int)   : total null cells in the raw input
          - size        (int)   : df.size (rows × cols) of the raw input
          - n_rows      (int)   : row count before deduplication
          - cols        (set)   : column names present in the raw input
          - obj_cols    (set)   : columns whose dtype was `object` at load time

        This avoids keeping a full df.copy() alive for the entire pipeline run.
        The computed "before" score is mathematically identical to the old
        df_before-based formula because:
          • null_total / size   ≡  df_before.isnull().sum().sum() / df_before.size
          • n_rows              ≡  len(df_before)
          • obj_cols filtered by columns_detected["text"] ≡ the old per-column
            dtype==object check (Step 1 populates columns_detected before this
            method is called, so the exclusion set is already complete).
        """
        # ── Before score ──────────────────────────────────────────────────
        before_missing_pct = before_metrics["null_total"] / max(before_metrics["size"], 1)
        dup_pct            = self.report["duplicates_removed"] / max(before_metrics["n_rows"], 1)
        n_outlier_cols     = len(self.report["outliers_handled"])
        n_untyped_obj      = sum(
            1 for col in before_metrics["obj_cols"]
            if col not in self.report["columns_detected"]["text"]
        )
        penalties_before = (
            before_missing_pct * 100 * 1.5
            + dup_pct           * 100 * 1.5
            + n_outlier_cols    * 5
            + n_untyped_obj     * 5
        )
        before_score = round(max(0.0, 100.0 - penalties_before), 1)

        # ── After score — restricted strictly to original columns ─────────
        #
        # "original_cols" = the intersection of the raw-input column names and
        # df_after.columns.  Every column the pipeline generated itself
        # (lag_*, Order_Date_*, delta_days_*, days_since_last_*, and any
        # custom-rule output column) is absent from before_metrics["cols"] and
        # therefore absent from this set.
        #
        # Two penalty signals must be scoped to original_cols only:
        #
        #   1. NULL penalty — engineered features may legitimately contain
        #      -1 sentinels or NaN in rows where no prior period exists.
        #      Counting those as "remaining missing values" would penalise
        #      the user for the pipeline's own design decisions.
        #
        #   2. HIGH CORRELATION penalty — date decomposition features
        #      (Year, Month, Quarter) are structurally correlated with each
        #      other, and lag features are correlated with their source
        #      column by construction.  Penalising these pairs conflates
        #      intentional feature engineering with genuine data quality
        #      problems in the user's original dataset.
        #
        # Business-logic and sparse-column penalties are unchanged: they
        # reflect decisions made exclusively on original columns in Steps 4
        # and 4.5 and are already correctly scoped.
        original_cols = before_metrics["cols"] & set(df_after.columns)

        # 1. Null penalty — original columns only
        if original_cols:
            orig_col_list    = list(original_cols)
            after_null_count = df_after[orig_col_list].isnull().sum().sum()
            after_null_denom = df_after[orig_col_list].size
        else:
            after_null_count = 0
            after_null_denom = 1
        after_null_pct = after_null_count / max(after_null_denom, 1)

        # 2. High-correlation penalty — original columns only.
        #    A correlation pair is included only when BOTH columns existed
        #    in the user's uploaded file.  Any pair that involves a pipeline-
        #    generated column (one or both sides) is silently excluded.
        n_high_corr_original = sum(
            1
            for pair in self.report.get("high_correlations", [])
            if pair["col_a"] in original_cols and pair["col_b"] in original_cols
        )

        n_sparse_skipped = len(self.report.get("skipped_sparse_cols", []))
        n_biz_violations = len(self.report.get("business_logic_fixed", {}))
        n_rule_errors    = len(self.report.get("custom_rule_errors", []))

        penalties_after = (
            after_null_pct        * 100 * 2.0
            + n_sparse_skipped    * 2.0
            + n_biz_violations    * 0.5
            + n_high_corr_original * 1.0   # engineered-feature pairs excluded
            + n_rule_errors       * 3.0
        )

        # Fix 63 [M-02]: No max(before_score, ...) floor.
        # The after score is a genuine independent measurement.
        after_score = round(min(100.0, max(0.0, 100.0 - penalties_after)), 1)

        self.report["quality_score"] = {
            "before":                    before_score,
            "after":                     after_score,
            "delta":                     round(after_score - before_score, 1),
            # Expose scoped counts so the frontend can show the breakdown
            "high_corr_original_cols":   n_high_corr_original,
            "high_corr_engineered_cols": (
                len(self.report.get("high_correlations", []))
                - n_high_corr_original
            ),
            "after_null_pct_original":   round(after_null_pct * 100, 3),
        }

    # ──────────────────────────────────────────────────
    #  JSON REPORT
    # ──────────────────────────────────────────────────
    def save_json_report(self, output_path="pipeline_report.json"):
        """
        [Part-3 Fix 1] Serialise the pipeline report to JSON without silent
        data loss.

        The previous implementation caught every serialisation failure with a
        bare `except Exception:` and fell back to str(val).  This masked the
        root cause (un-serialisable lambda closures in triad dicts) and silently
        degraded the JSON structure.

        Now that triads store only a plain string opkey the fallback is no
        longer needed for discovered_laws.  A custom encoder handles the
        remaining numpy / pandas scalar edge-cases (int64, float64, NaN, NaT)
        that json.dumps rejects by default.
        """
        class _PipelineEncoder(json.JSONEncoder):
            def default(self, obj):
                # numpy integer scalars
                if isinstance(obj, (np.integer,)):
                    return int(obj)
                # numpy floating scalars (incl. NaN → null)
                if isinstance(obj, (np.floating,)):
                    return None if np.isnan(obj) else float(obj)
                # numpy bool scalars
                if isinstance(obj, (np.bool_,)):
                    return bool(obj)
                # numpy arrays
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                # pandas Timestamp / NaT
                if isinstance(obj, pd.Timestamp):
                    return obj.isoformat() if not pd.isna(obj) else None
                if obj is pd.NaT:
                    return None
                # sets → lists
                if isinstance(obj, (set, frozenset)):
                    return sorted(obj, key=str)
                # any remaining un-serialisable object → repr string so the
                # report always writes rather than raising TypeError
                return repr(obj)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=4, ensure_ascii=False,
                      cls=_PipelineEncoder)
        log(f"JSON report saved  →  {output_path}", "SUCCESS")
        return self.report

    # ──────────────────────────────────────────────────
    #  FINAL REPORT
    # ──────────────────────────────────────────────────
    def _print_report(self, df):
        _logger.info("")
        _logger.info("=" * 60)
        _logger.info("  PIPELINE v2.9 — SUMMARY REPORT")
        _logger.info("=" * 60)
        orig  = self.report["original_shape"]
        final = self.report["final_shape"]
        qs    = self.report["quality_score"]
        ri    = sum(self.report["relational_imputation"].values())
        cri   = sum(self.report["custom_rule_imputation"].values())
        cs    = sum(self.report["constraint_sync"].values())
        nl    = len(self.report["discovered_laws"])
        nc_   = len(self.report["discovery_conflicts"])
        enc   = self.report.get("file_encoding", "N/A")
        ntb   = self.report.get("non_triadic_bypass", False)
        ssc   = len(self.report.get("skipped_sparse_cols", []))
        mpc   = len(self.report.get("monetary_protected", []))
        csc   = len(self._case_sensitive_cols)
        idc   = len(self._id_cols)
        imp_c = sum(len(v) for v in self._imputed_rows.values())
        cap_c = sum(len(v) for v in self._capped_rows.values())
        n_cr  = len(self._custom_rules)
        n_cre = len(self.report.get("custom_rule_errors", []))
        fm    = self.report.get("fit_mask_rows")
        tot   = self.report.get("total_rows_at_fit")
        tgt   = self.report.get("target_col_excluded")

        total_preserved = sum(
            v.get("preserved_as_return", 0)
            for v in self.report["business_logic_fixed"].values()
        )
        total_err_replaced = sum(
            v.get("replaced_as_error", 0)
            for v in self.report["business_logic_fixed"].values()
        )

        _logger.info(f"  {'Dataset Shape':<52} {str(orig):<20} →  {str(final)}")
        _logger.info(f"  {'File Encoding Detected':<52} {enc}  [Fix 26]")
        _logger.info(
            f"  {'Fit-mask Rows / Total Rows':<52} "
            f"{fm:,} / {tot:,}  [C-01]"
            if fm is not None else
            f"  {'Fit-mask Rows / Total Rows':<52} N/A"
        )
        if tgt:
            _logger.info(f"  {'Target Column Excluded':<52} {tgt}  [C-02]")
        _logger.info(f"  {'Duplicates Removed':<52} {self.report['duplicates_removed']}")
        _logger.info(f"  {'ID Columns Protected':<52} {idc}  [Fix 16/17]")
        _logger.info(f"  {'Case-Sensitive Columns':<52} {csc}  [Fix 25]")
        _logger.info(f"  {'Laws Discovered (Step 3.7)':<52} {nl}")
        if nc_:
            _logger.info(f"  {'Discovery Conflicts / Dedups':<52} {nc_}  [Fix 28]")
        if ntb:
            _logger.info(f"  {'Non-Triadic Bypass':<52} YES  [Fix 15]")
        _logger.info(f"  {'Custom Rules Registered':<52} {n_cr}  [Fix 50]")
        if n_cre:
            _logger.info(
                f"  {'Custom Rule Pre-validation Errors':<52} "
                f"{n_cre}  [M-01] ← CHECK REPORT"
            )
        _logger.info(f"  {'Values Filled by Custom Rules':<52} {cri}  [Fix 50]")
        _logger.info(f"  {'Relational Values Recovered':<52} {ri}")
        _logger.info(f"  {'Constraint Violations Fixed':<52} {cs}")
        _logger.info(f"  {'Total Imputed Cell-Rows Tracked':<52} {imp_c}  [Fix 32]")
        _logger.info(f"  {'Total IQR-Capped Cell-Rows Tracked':<52} {cap_c}  [Fix 32]")
        _logger.info(
            f"  {'Columns with Missing Values Filled':<52} "
            f"{len(self.report['missing_filled'])}"
        )
        _logger.info(f"  {'Sparse Columns Skipped (>30%)':<52} {ssc}  [Fix 24]")
        _logger.info(
            f"  {'Negatives Preserved as Returns':<52} "
            f"{total_preserved}  [Fix 51]"
        )
        _logger.info(
            f"  {'Negatives Replaced as Errors':<52} "
            f"{total_err_replaced}  [Fix 51]"
        )
        _logger.info(
            f"  {'Columns with Outliers Handled':<52} "
            f"{len(self.report['outliers_handled'])}"
        )
        _logger.info(
            f"  {'New Date Features Created':<52} "
            f"{len(self.report['date_features_added'])}"
        )
        if self._primary_date_col:
            _logger.info(
                f"  {'Primary Date Column':<52} "
                f"[{self._primary_date_col}]  [Fix 19]"
            )
        _logger.info(
            f"  {'Type Conversions Done':<52} "
            f"{len(self.report['type_conversions'])}"
        )
        _logger.info(
            f"  {'Lag Features Created (row-order preserved)':<52} "
            f"{len(self.report['lag_features_added'])}  [C-03]"
        )
        _logger.info(f"  {'Lag Sentinel Convention':<52} -1 = no prior period  [H-03]")
        _logger.info(
            f"  {'Monetary Columns Protected (float64)':<52} "
            f"{mpc}  [Fix 23/47]"
        )
        mem = self.report.get("memory_optimization", {})
        if mem:
            _logger.info(
                f"  {'Memory Saved':<52} {mem.get('saved_mb',0):.2f} MB  "
                f"({mem.get('saved_pct',0):.1f}%)"
            )
        n_corr_total = len(self.report['high_correlations'])
        n_corr_orig  = qs.get("high_corr_original_cols", 0)
        n_corr_eng   = qs.get("high_corr_engineered_cols", 0)
        _logger.info(f"  {'High Correlations Found (total)':<52} {n_corr_total}")
        _logger.info(f"  {'  ↳ in original columns (penalised)':<52} {n_corr_orig}")
        _logger.info(f"  {'  ↳ in engineered columns (excluded)':<52} {n_corr_eng}")
        _logger.info(
            f"  {'Encoding Recommendations':<52} "
            f"{len(self.report['encoding_report'])}"
        )
        _logger.info("")
        delta_str = (
            f"  ({qs.get('delta', 0):+.1f})"
            if "delta" in qs else ""
        )
        # Quality score without ANSI colour codes — formatters handle presentation
        _logger.info(
            f"  {'Data Quality Score':<52} "
            f"{qs['before']}/100  →  {qs['after']}/100"
            f"{delta_str}  [M-02: no floor | engineered cols excluded]"
        )
        _logger.info("")
        log("Pipeline v2.9 completed. Dataset is clean and production-ready.",
            "SUCCESS")
        _logger.info("")


# ══════════════════════════════════════════════════════
#  RUN DIRECTLY
# ══════════════════════════════════════════════════════
if __name__ == "__main__":
    """
    Without Database:
        python dynamic_preprocessing_pipeline_v2_9.py data.csv

    With Database + ML split:
        python dynamic_preprocessing_pipeline_v2_9.py data.csv \\
               postgresql://user:pass@localhost:5432/db 1 \\
               --fit_on 0.8 --target_col next_month_sales

    Thread safety:
        Instantiate DynamicPreprocessingPipeline() ONCE PER REQUEST.
        Do not share instances across concurrent threads.
    """
    import argparse

    # [Part-3 Fix 3]: Configure a basic StreamHandler for CLI runs so all
    # pipeline log() calls actually appear in the terminal.  In library use
    # the NullHandler registered above silently discards records unless the
    # caller configures their own handlers.
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(levelname)-8s %(name)s — %(message)s",
        stream  = sys.stdout,
    )

    parser = argparse.ArgumentParser(
        description="DynamicPreprocessingPipeline v2.9"
    )
    parser.add_argument("file",       help="Input CSV / XLSX file path")
    parser.add_argument("db_url",     nargs="?", default=None,
                        help="SQLAlchemy DB URL (optional)")
    parser.add_argument("user_id",    nargs="?", default=None, type=int,
                        help="Registered user ID (optional)")
    parser.add_argument("--fit_on",   default=None, type=float,
                        help="Fraction of rows used for statistics (e.g. 0.8)")
    parser.add_argument("--target_col", default=None,
                        help="Target column to exclude from imputation/discovery")
    args = parser.parse_args()

    if args.user_id is not None and args.user_id <= 0:
        log(f"Invalid user_id '{args.user_id}': must be a positive integer", "ERROR")
        sys.exit(1)

    my_egypt_rules = {
        "Total_Revenue": lambda df: (
            df["Unit_Price_EGP"] * df["Quantity"]
        ) - df["Discount_EGP"]
    }

    pipeline = DynamicPreprocessingPipeline()
    cleaned_df, final_report = pipeline.run(
        source       = args.file,
        db_url       = args.db_url,
        user_id      = args.user_id,
        custom_rules = my_egypt_rules,
        fit_on       = args.fit_on,
        target_col   = args.target_col,
    )

    _logger.info("\nPreview of Cleaned Dataset (first 5 rows):")
    _logger.info("\n" + cleaned_df.head().to_string())