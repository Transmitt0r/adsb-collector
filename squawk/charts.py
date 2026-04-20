"""Generate flight traffic charts for the daily digest."""

from __future__ import annotations

import io
import logging

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

from squawk.queries.charts import HourlyCount

logger = logging.getLogger(__name__)


def render_traffic_chart(
    hourly: list[HourlyCount],
) -> bytes | None:
    """Render a single-panel hourly PNG chart.

    Returns PNG bytes, or None if there is no data.
    """
    if not hourly:
        return None

    with plt.style.context("fivethirtyeight"):
        fig, ax = plt.subplots(figsize=(7, 3))

        hour_map = {h.hour: h.flights for h in hourly}
        hours = list(range(24))
        counts = [hour_map.get(h, 0) for h in hours]

        ax.plot(hours, counts, color="#008fd5", linewidth=2)
        ax.fill_between(hours, counts, color="#008fd5", alpha=0.3)
        ax.set_xticks(range(0, 24, 3))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
        ax.set_title("Flüge nach Uhrzeit")
        ax.set_ylabel("Flüge")
        ax.set_xlim(0, 23)

        fig.tight_layout(pad=1.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
