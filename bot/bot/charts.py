"""Generate flight traffic charts for the weekly digest."""

from __future__ import annotations

import io
import logging
from datetime import date

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import psycopg2

matplotlib.use("Agg")  # headless, no display needed

logger = logging.getLogger(__name__)

_DE_WEEKDAYS = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]


def generate_traffic_chart(database_url: str, days: int = 7) -> bytes | None:
    """
    Generate a two-panel PNG chart:
      - Top: bar chart of flights per day (weekday labels, date range in title)
      - Bottom: line chart of flights by hour of day (aggregated across the period)

    Returns PNG bytes, or None on failure.
    """
    try:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        DATE(started_at AT TIME ZONE 'Europe/Berlin') AS day,
                        COUNT(*) AS flights
                    FROM sightings
                    WHERE started_at > now() - (%(days)s || ' days')::interval
                    GROUP BY day
                    ORDER BY day
                """,
                    {"days": days},
                )
                daily = cur.fetchall()

                # Flights per hour split by weekday vs weekend (local time)
                # DOW: 0=Sunday, 6=Saturday in PostgreSQL
                cur.execute(
                    """
                    SELECT
                        EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')::int AS hour,
                        CASE WHEN EXTRACT(DOW FROM started_at AT TIME ZONE 'Europe/Berlin') IN (0, 6)
                             THEN 'weekend' ELSE 'weekday' END AS day_type,
                        COUNT(*) AS flights
                    FROM sightings
                    WHERE started_at > now() - (%(days)s || ' days')::interval
                    GROUP BY hour, day_type
                    ORDER BY hour
                """,
                    {"days": days},
                )
                hourly = cur.fetchall()

        if not daily and not hourly:
            return None

        with plt.style.context("fivethirtyeight"):
            fig, (ax_day, ax_hour) = plt.subplots(2, 1, figsize=(7, 5.5))

            # --- daily bars ---
            if daily:
                dates: list[date] = [row[0] for row in daily]
                counts = [row[1] for row in daily]
                x = list(range(len(dates)))
                labels = [_DE_WEEKDAYS[d.weekday()] for d in dates]

                colors = ["#e05c2a" if d.weekday() >= 5 else "#008fd5" for d in dates]
                ax_day.bar(x, counts, color=colors, width=0.6, zorder=3)
                ax_day.set_xticks(x)
                ax_day.set_xticklabels(labels)

                # Date range in title
                def fmt(d):
                    return f"{d.day}.{d.month}."

                date_range = (
                    f"{fmt(dates[0])} – {fmt(dates[-1])}"
                    if len(dates) > 1
                    else fmt(dates[0])
                )
                ax_day.set_title(f"Flüge pro Tag ({date_range})")
                ax_day.set_ylabel("Flüge")
                ax_day.legend(
                    handles=[
                        Patch(color="#008fd5", label="Wochentag"),
                        Patch(color="#e05c2a", label="Wochenende"),
                    ],
                    fontsize=7,
                    loc="upper left",
                )

            # --- hourly stacked area (weekday vs weekend) ---
            if hourly:
                wd_map: dict[int, int] = {}
                we_map: dict[int, int] = {}
                for hour, day_type, count in hourly:
                    if day_type == "weekend":
                        we_map[hour] = count
                    else:
                        wd_map[hour] = count

                hours = list(range(24))
                wd = [wd_map.get(h, 0) for h in hours]
                we = [we_map.get(h, 0) for h in hours]
                total = [wd[h] + we[h] for h in hours]

                ax_hour.fill_between(
                    hours, 0, wd, color="#008fd5", alpha=0.8, label="Wochentag"
                )
                ax_hour.fill_between(
                    hours, wd, total, color="#e05c2a", alpha=0.8, label="Wochenende"
                )
                ax_hour.plot(hours, total, color="black", linewidth=1, alpha=0.4)
                ax_hour.set_xticks(range(0, 24, 3))
                ax_hour.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
                ax_hour.set_title("Flüge nach Uhrzeit")
                ax_hour.set_ylabel("Flüge")
                ax_hour.set_xlim(0, 23)
                ax_hour.legend(fontsize=7, loc="upper left")

            fig.tight_layout(pad=1.5)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception:
        logger.exception("Failed to generate traffic chart")
        return None
