"""Generate flight traffic charts for the weekly digest."""

from __future__ import annotations

import io
import logging

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

from squawk.queries.charts import DailyCount, HourlyCount

logger = logging.getLogger(__name__)

_DE_WEEKDAYS = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"]


def render_traffic_chart(
    daily: list[DailyCount],
    hourly: list[HourlyCount],
) -> bytes | None:
    """Render a two-panel PNG chart.

    Returns PNG bytes, or None if there is no data.
    """
    if not daily and not hourly:
        return None

    with plt.style.context("fivethirtyeight"):
        fig, (ax_day, ax_hour) = plt.subplots(2, 1, figsize=(7, 5.5))

        if daily:
            x = list(range(len(daily)))
            counts = [d.flights for d in daily]
            labels = [_DE_WEEKDAYS[d.weekday] for d in daily]
            ax_day.bar(x, counts, color="#008fd5", width=0.6, zorder=3)
            ax_day.set_xticks(x)
            ax_day.set_xticklabels(labels)

            first = daily[0].day
            last = daily[-1].day
            date_range = f"{first} – {last}" if len(daily) > 1 else first
            ax_day.set_title(f"Flüge pro Tag ({date_range})")
            ax_day.set_ylabel("Flüge")

        if hourly:
            hour_map = {h.hour: h.flights for h in hourly}
            hours = list(range(24))
            counts = [hour_map.get(h, 0) for h in hours]

            ax_hour.plot(hours, counts, color="#008fd5", linewidth=2)
            ax_hour.fill_between(hours, counts, color="#008fd5", alpha=0.3)
            ax_hour.set_xticks(range(0, 24, 3))
            ax_hour.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
            ax_hour.set_title("Flüge nach Uhrzeit")
            ax_hour.set_ylabel("Flüge")
            ax_hour.set_xlim(0, 23)

        fig.tight_layout(pad=1.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
