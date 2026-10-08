"""Streamlit page: look up one order in the MongoDB serving layer.

Reads ``olist_serving.order_status`` and ``olist_serving.order_alerts`` (kept by
the Kafka Connect MongoDB sink) with a read-only user. The query and shaping
logic lives in :mod:`order_lookup`; this file only renders.
"""

from __future__ import annotations

import os
from typing import Any

import streamlit as st
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

import order_lookup

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/olist_serving")
MONGO_DB = os.environ.get("MONGO_DB", "olist_serving")
# seed_data.py --sample names orders order_000000, order_000001, ...
EXAMPLE_ORDER_ID = "order_000000"

st.set_page_config(page_title="Consulta de pedido", page_icon="🔎", layout="wide")


@st.cache_resource(show_spinner=False)
def get_db() -> Database[Any]:
    """Connect (once) to the serving database.

    Returns:
        The ``olist_serving`` database handle.
    """
    client: MongoClient[Any] = MongoClient(
        MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True
    )
    return client[MONGO_DB]


def _money(value: float | None) -> str:
    return "—" if value is None else f"R$ {value:,.2f}"


def render_order(db: Database[Any], order_id: str) -> None:
    """Render status, timeline, payments and alerts of one order."""
    order = order_lookup.fetch_order(db.order_status, order_id)
    if order is None:
        st.warning(
            f"Pedido `{order_id}` não encontrado no MongoDB. Ele pode ainda não "
            "ter passado pelo order-status-service e pelo conector."
        )
        return

    cols = st.columns(4)
    cols[0].metric("Status", order.get("status", "—"))
    cols[1].metric("Estado (UF)", order.get("customer_state") or "—")
    cols[2].metric("Categoria", order.get("product_category") or "—")
    cols[3].metric("Eventos agregados", order.get("event_count", 0))

    left, right = st.columns(2)
    with left:
        st.subheader("Linha do tempo")
        timeline = order_lookup.build_timeline(order)
        if timeline.empty:
            st.info("Nenhum marco registrado ainda.")
        else:
            st.dataframe(timeline, hide_index=True, use_container_width=True)
        st.caption(
            "Ordenada pelo horário dos eventos. Os produtores de pedidos, "
            "pagamentos e entregas rodam em ritmos independentes, então os "
            "horários entre fontes nem sempre seguem a ordem do negócio."
        )
        if order.get("last_delivery_status"):
            st.write(
                f"Último status da transportadora: "
                f"**{order['last_delivery_status']}**"
            )

    with right:
        st.subheader("Pagamentos")
        pay = order_lookup.payment_summary(order)
        p1, p2, p3 = st.columns(3)
        p1.metric("Valor do pedido", _money(pay["order_value"]))
        p2.metric("Pago", _money(pay["paid_amount"]))
        p3.metric("Diferença", _money(pay["difference"]))
        st.write(
            f"{pay['payment_count']} pagamento(s): "
            f"{', '.join(pay['payment_types']) or '—'}"
        )

        st.subheader("Alertas")
        alerts = order_lookup.fetch_alerts(db.order_alerts, order_id)
        if alerts:
            st.dataframe(
                order_lookup.alerts_frame(alerts),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.success("Nenhum alerta para este pedido.")

    with st.expander("Documento completo (order_status)"):
        st.json(order, expanded=False)


def render_recent(db: Database[Any]) -> None:
    """Render the most recently updated orders, filterable by status."""
    st.subheader("Pedidos atualizados recentemente")
    status = st.selectbox("Status", ["(todos)", *order_lookup.STATUSES])
    recent = order_lookup.fetch_recent_orders(
        db.order_status, None if status == "(todos)" else status
    )
    if not recent:
        st.info("Nenhum pedido no MongoDB ainda.")
        return
    columns = ["order_id", "status", "customer_state", "order_value", "updated_at"]
    st.dataframe(
        [{c: doc.get(c) for c in columns} for doc in recent],
        hide_index=True,
        use_container_width=True,
    )


st.title("🔎 Consulta de pedido")
st.caption("Estado atual por pedido servido pelo MongoDB (camada de serviço).")

order_id = order_lookup.normalize_order_id(
    st.text_input("ID do pedido", value=EXAMPLE_ORDER_ID)
)

try:
    db = get_db()
    if order_id:
        render_order(db, order_id)
    st.divider()
    render_recent(db)
except PyMongoError as exc:
    st.error(f"Não foi possível consultar o MongoDB: {type(exc).__name__}")
