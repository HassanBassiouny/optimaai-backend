"""
app/services/odoo_service.py
═══════════════════════════════════════════════════════════════════════════
ODOO CONNECTOR (XML-RPC)
═══════════════════════════════════════════════════════════════════════════

Thin connector between FastAPI and Odoo 17/18/19 via the official XML-RPC
external API. Treats Odoo as a read-only data source — no records are ever
created, updated, or deleted by this layer.

Reads from four Odoo models:
  - crm.lead       : pipeline / opportunities
  - sale.order     : confirmed and quotation orders
  - account.move   : customer invoices
  - res.partner    : customers / accounts

Configuration (environment variables):
  ODOO_URL          e.g. http://localhost:8069
  ODOO_DB           e.g. optimaai_db
  ODOO_USERNAME     e.g. admin
  ODOO_PASSWORD     e.g. admin   (or ODOO_API_KEY for key auth)
  ODOO_API_KEY      Odoo user API key (preferred over password in prod)

The connector is intentionally framework-agnostic — no FastAPI imports here,
so it can be reused from Celery workers, scripts, and tests.
"""
from __future__ import annotations

import os
import logging
import xmlrpc.client
from datetime import datetime, timedelta
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

_logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
#  Field selections per model
#  Mirrors the data dictionary in Chapter 3, §3.3.5.
# ══════════════════════════════════════════════════════

CRM_LEAD_FIELDS = [
    "id", "name", "partner_id", "expected_revenue", "probability",
    "stage_id", "date_deadline", "date_open", "date_conversion",
    "date_closed", "user_id", "team_id", "type", "active",
    "create_date", "write_date",
]

SALE_ORDER_FIELDS = [
    "id", "name", "partner_id", "amount_total", "amount_untaxed",
    "amount_tax", "date_order", "state", "team_id", "user_id",
    "payment_term_id", "currency_id", "company_id",
    "create_date", "write_date",
]

ACCOUNT_MOVE_FIELDS = [
    "id", "name", "partner_id", "amount_total", "amount_untaxed",
    "amount_residual", "invoice_date", "invoice_date_due",
    "payment_state", "state", "move_type", "currency_id",
    "create_date", "write_date",
]

RES_PARTNER_FIELDS = [
    "id", "name", "customer_rank", "supplier_rank",
    "country_id", "industry_id", "city", "email",
    "is_company", "active", "create_date", "write_date",
]


# ══════════════════════════════════════════════════════
#  Exceptions
# ══════════════════════════════════════════════════════

class OdooConnectionError(Exception):
    """Raised when the connector cannot reach Odoo or auth fails."""


class OdooConfigError(Exception):
    """Raised when required env vars are missing or malformed."""


# ══════════════════════════════════════════════════════
#  Connector
# ══════════════════════════════════════════════════════

class OdooConnector:
    """
    Stateless-ish XML-RPC client.

    Each instance authenticates once on construction (via ``connect()``) and
    caches the resulting uid + models proxy. Re-instantiate to refresh
    credentials.
    """

    def __init__(
        self,
        url:      Optional[str] = None,
        db:       Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.url      = (url      or os.getenv("ODOO_URL", "")).rstrip("/")
        self.db       =  db       or os.getenv("ODOO_DB", "")
        self.username =  username or os.getenv("ODOO_USERNAME", "")
        # API key takes priority; falls back to password.
        self.password = (
            password
            or os.getenv("ODOO_API_KEY")
            or os.getenv("ODOO_PASSWORD", "")
        )

        if not all([self.url, self.db, self.username, self.password]):
            raise OdooConfigError(
                "Missing Odoo configuration. Set ODOO_URL, ODOO_DB, "
                "ODOO_USERNAME, and ODOO_PASSWORD (or ODOO_API_KEY)."
            )

        self.uid: Optional[int] = None
        self._common = None
        self._models = None

    # ──────────────────────────────────────────────────
    #  Authentication
    # ──────────────────────────────────────────────────

    def connect(self) -> int:
        """
        Authenticate against Odoo and cache the uid.

        Returns the authenticated user id. Raises OdooConnectionError on any
        network or credential failure.
        """
        try:
            self._common = xmlrpc.client.ServerProxy(
                f"{self.url}/xmlrpc/2/common", allow_none=True
            )
            # Liveness probe — also tells us the Odoo version.
            version = self._common.version()
            _logger.info(
                "Odoo reachable at %s — server_version=%s",
                self.url, version.get("server_version"),
            )

            self.uid = self._common.authenticate(
                self.db, self.username, self.password, {}
            )
            if not self.uid:
                raise OdooConnectionError(
                    "Odoo authentication failed: invalid credentials or "
                    "database name."
                )

            self._models = xmlrpc.client.ServerProxy(
                f"{self.url}/xmlrpc/2/object", allow_none=True
            )
            return self.uid

        except OdooConnectionError:
            raise
        except xmlrpc.client.Fault as e:
            raise OdooConnectionError(f"Odoo XML-RPC fault: {e.faultString}") from e
        except Exception as e:
            raise OdooConnectionError(f"Cannot reach Odoo at {self.url}: {e}") from e

    # ──────────────────────────────────────────────────
    #  Generic query helpers
    # ──────────────────────────────────────────────────

    def execute_kw(
        self,
        model:   str,
        method:  str,
        args:    list,
        kwargs:  Optional[dict] = None,
    ) -> Any:
        """
        Low-level wrapper around Odoo's ``execute_kw`` RPC method.

        All higher-level helpers route through here so we have one place to
        log timings and retry on transient failures (future work).
        """
        if self.uid is None:
            self.connect()

        try:
            return self._models.execute_kw(
                self.db, self.uid, self.password,
                model, method, args, kwargs or {}
            )
        except xmlrpc.client.Fault as e:
            raise OdooConnectionError(
                f"Odoo {model}.{method} failed: {e.faultString}"
            ) from e

    def search_read(
        self,
        model:  str,
        domain: list,
        fields: list[str],
        limit:  Optional[int] = None,
        offset: int = 0,
        order:  Optional[str] = None,
    ) -> list[dict]:
        """
        Convenience wrapper for ``search_read`` — returns matching records as
        plain dicts.

        ``domain`` follows Odoo's polish-notation syntax, e.g.
        ``[("state", "=", "sale"), ("date_order", ">=", "2024-01-01")]``.
        """
        kwargs: dict = {"fields": fields, "offset": offset}
        if limit is not None:
            kwargs["limit"] = limit
        if order is not None:
            kwargs["order"] = order
        return self.execute_kw(model, "search_read", [domain], kwargs)

    def count(self, model: str, domain: list) -> int:
        """Total record count for a domain — use before paging large reads."""
        return self.execute_kw(model, "search_count", [domain])

    # ──────────────────────────────────────────────────
    #  Domain-specific extractors
    #  Each returns a list of raw Odoo dicts; transformation into a tidy
    #  DataFrame happens in odoo_extractor.py.
    # ──────────────────────────────────────────────────

    def fetch_leads(
        self,
        since:  Optional[datetime] = None,
        limit:  Optional[int] = None,
    ) -> list[dict]:
        """Fetch CRM leads/opportunities. ``since`` filters on write_date."""
        domain: list = [("active", "=", True)]
        if since is not None:
            domain.append(("write_date", ">=", since.strftime("%Y-%m-%d %H:%M:%S")))
        return self.search_read(
            "crm.lead", domain, CRM_LEAD_FIELDS,
            limit=limit, order="create_date desc",
        )

    def fetch_sale_orders(
        self,
        since:  Optional[datetime] = None,
        limit:  Optional[int] = None,
        states: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Fetch sale orders.

        ``states`` filters on the order state (default: confirmed-or-done so
        we ignore stale drafts and cancelled orders, matching the forecasting
        contract in Chapter 3).
        """
        if states is None:
            states = ["sale", "done"]
        domain: list = [("state", "in", states)]
        if since is not None:
            domain.append(("date_order", ">=", since.strftime("%Y-%m-%d %H:%M:%S")))
        return self.search_read(
            "sale.order", domain, SALE_ORDER_FIELDS,
            limit=limit, order="date_order desc",
        )

    def fetch_invoices(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Fetch posted customer invoices (out_invoice / out_refund)."""
        domain: list = [
            ("move_type", "in", ["out_invoice", "out_refund"]),
            ("state",     "=",  "posted"),
        ]
        if since is not None:
            domain.append(("invoice_date", ">=", since.strftime("%Y-%m-%d")))
        return self.search_read(
            "account.move", domain, ACCOUNT_MOVE_FIELDS,
            limit=limit, order="invoice_date desc",
        )

    def fetch_partners(
        self,
        only_customers: bool = True,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Fetch partners. By default restricts to customer_rank > 0."""
        domain: list = [("active", "=", True)]
        if only_customers:
            domain.append(("customer_rank", ">", 0))
        return self.search_read(
            "res.partner", domain, RES_PARTNER_FIELDS,
            limit=limit, order="create_date desc",
        )

    # ──────────────────────────────────────────────────
    #  Health
    # ──────────────────────────────────────────────────

    def ping(self) -> dict:
        """
        Cheap health check — used by the ``/odoo/test-connection`` endpoint.

        Returns counts of the four core models so the user knows their
        instance actually has data to extract.
        """
        if self.uid is None:
            self.connect()
        return {
            "connected":  True,
            "url":        self.url,
            "db":         self.db,
            "uid":        self.uid,
            "counts": {
                "crm.lead":     self.count("crm.lead",     [("active", "=", True)]),
                "sale.order":   self.count("sale.order",   [("state", "in", ["sale", "done"])]),
                "account.move": self.count("account.move", [("move_type", "in", ["out_invoice", "out_refund"])]),
                "res.partner":  self.count("res.partner",  [("customer_rank", ">", 0)]),
            },
        }


# ══════════════════════════════════════════════════════
#  Module-level singleton helper
# ══════════════════════════════════════════════════════

_default: Optional[OdooConnector] = None


def get_connector() -> OdooConnector:
    """
    Return a process-wide OdooConnector. Lazily authenticates on first use.

    Reset by calling ``reset_connector()`` (e.g. after rotating credentials).
    """
    global _default
    if _default is None:
        _default = OdooConnector()
        _default.connect()
    return _default


def reset_connector() -> None:
    """Drop the cached connector — next call to ``get_connector`` re-auths."""
    global _default
    _default = None
