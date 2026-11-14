"""
make_charts.py -- Part 4: static visualisations from the CSV results of the earlier steps.

Reads  results/*.csv   (written by aggregations.py, batch_analysis.py,
                         spark_sql_analytics.py and integrated_funnel.py)
Writes results/figures/*.png   (embed these in the technical report)

Charts
  1_revenue_by_category   MongoDB pipeline 1   top categories by revenue
  2_monthly_revenue       MongoDB pipeline 4   revenue and orders per month (partial months removed)
  3_customer_segments     MongoDB pipeline 3   users and average spend per purchase-frequency segment
  4_conversion_funnel     HBase + MongoDB + Spark   sessions -> product view -> cart -> checkout -> order
  5_conversion_by_segment HBase + MongoDB + Spark   session conversion by device and by traffic source
  6_top_products          MongoDB pipeline 2   top 10 products by revenue
  7_cohort_heatmap        Spark batch job      revenue per user by registration cohort and month offset

Usage:
    pip install matplotlib pandas
    python make_charts.py                       # reads ./results
    python make_charts.py --results results --out results/figures
A chart whose CSV file is missing is skipped with a message; the others still run.
"""

import argparse
import math
import os

import matplotlib
matplotlib.use("Agg")            # no window needed: just write PNG files
import matplotlib.pyplot as plt
import pandas as pd

BLUE, ORANGE, GREEN, GREY = "#2F6DB5", "#E07B39", "#3A9A6B", "#8A8F98"
plt.rcParams.update({"figure.dpi": 110, "savefig.dpi": 160, "font.size": 10,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titleweight": "bold", "axes.titlesize": 12})


def money(x, _=None):
    return f"${x / 1e6:.1f}M" if abs(x) >= 1e6 else f"${x / 1e3:.0f}K" if abs(x) >= 1e3 else f"${x:.0f}"


SEGMENT_NAMES = {
    "1_never_purchased": "Never purchased", "2_one_time": "One-time",
    "3_occasional_2_3": "Occasional (2-3)", "4_regular_4_6": "Regular (4-6)",
    "5_loyal_7_plus": "Loyal (7+)",
}


def pretty(label):
    """'2_one_time' -> 'One-time'; unknown labels fall back to a cleaned-up version."""
    if label in SEGMENT_NAMES:
        return SEGMENT_NAMES[label]
    parts = str(label).split("_", 1)
    text = parts[1] if parts[0].isdigit() and len(parts) > 1 else str(label)
    return text.replace("_", " ").capitalize()


def thousands(x, _=None):
    return f"{x:,.0f}"


def read(results, name):
    path = os.path.join(results, name)
    if not os.path.exists(path):
        print(f"  skipped (missing {path})")
        return None
    return pd.read_csv(path)


def save(fig, out, name):
    os.makedirs(out, exist_ok=True)
    path = os.path.join(out, f"{name}.png")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    print(f"  wrote {path}")


# ------------------------------------------------------------------------------------------
def chart_revenue_by_category(results, out):
    df = read(results, "mongo_revenue_by_category.csv")
    if df is None:
        return
    top = df.sort_values("revenue", ascending=False).head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    bars = ax.barh(top["category_name"], top["revenue"], color=BLUE)
    ax.bar_label(bars, labels=[money(v) for v in top["revenue"]], padding=3, fontsize=9)
    ax.xaxis.set_major_formatter(money)
    ax.set_xlim(0, top["revenue"].max() * 1.15)
    ax.set_title("Revenue by product category (top 10)")
    ax.set_xlabel("Gross revenue (before discounts), completed, shipped and delivered orders")
    share = top["revenue"].sum() / df["revenue"].sum() * 100
    ax.text(0.99, 0.02, f"Top 10 = {share:.0f}% of all revenue", transform=ax.transAxes,
            ha="right", color=GREY, fontsize=9)
    save(fig, out, "1_revenue_by_category")


def chart_monthly_revenue(results, out):
    df = read(results, "mongo_monthly_revenue.csv")
    if df is None:
        return
    df = df.sort_values("month")
    cutoff = df["orders"].median() * 0.25           # months with far fewer orders = partial months
    partial = df[df["orders"] < cutoff]
    full = df[df["orders"] >= cutoff]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(full["month"], full["revenue"], color=BLUE, label="Revenue", width=0.55)
    ax.set_ylim(0, full["revenue"].max() * 1.3)
    ax.yaxis.set_major_formatter(money)
    ax.set_ylabel("Revenue")
    ax2 = ax.twinx()
    ax2.plot(full["month"], full["orders"], color=ORANGE, marker="o", label="Orders")
    ax2.set_ylabel("Orders")
    ax2.spines["right"].set_visible(True)
    ax2.set_ylim(0, full["orders"].max() * 1.3)
    ax.set_title("Monthly revenue and order count")
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left", frameon=False)
    if len(partial):
        ax.text(0.99, -0.16, "Partial months excluded: " + ", ".join(partial["month"]),
                transform=ax.transAxes, ha="right", color=GREY, fontsize=8)
    save(fig, out, "2_monthly_revenue")


def chart_customer_segments(results, out):
    df = read(results, "mongo_user_segmentation.csv")
    if df is None:
        return
    df = df.sort_values("segment")
    labels = [pretty(s) for s in df["segment"]]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4.2))
    b1 = a1.bar(labels, df["users"], color=BLUE)
    a1.bar_label(b1, labels=[f"{v:,}" for v in df["users"]], padding=2, fontsize=9)
    a1.yaxis.set_major_formatter(thousands)
    a1.set_ylim(0, df["users"].max() * 1.12)
    a1.set_title("Users per purchase-frequency segment")
    a1.set_ylabel("Users")
    b2 = a2.bar(labels, df["avg_spend_per_user"], color=GREEN)
    a2.bar_label(b2, labels=[f"${math.floor(v + 0.5):,}" for v in df["avg_spend_per_user"]], padding=2, fontsize=9)
    a2.yaxis.set_major_formatter(money)
    a2.set_title("Average spend per user")
    a2.set_ylim(0, df["avg_spend_per_user"].max() * 1.12)
    for a in (a1, a2):
        a.tick_params(axis="x", rotation=25)
    save(fig, out, "3_customer_segments")


def chart_funnel(results, out):
    df = read(results, "integrated_funnel_overall.csv")
    if df is None:
        return
    r = df.iloc[0]
    stages = [("All sessions", r["sessions"]), ("Viewed a product", r["viewed_product"]),
              ("Reached cart", r["reached_cart"]), ("Reached checkout", r["reached_checkout"]),
              ("Placed an order", r["purchased"])]
    names = [s[0] for s in stages][::-1]
    vals = [s[1] for s in stages][::-1]
    prev = [None] + [stages[i - 1][1] for i in range(1, len(stages))]
    prev = prev[::-1]
    total = stages[0][1]
    fig, ax = plt.subplots(figsize=(9, 4.4))
    bars = ax.barh(names, vals, color=[ORANGE, BLUE, BLUE, BLUE, GREEN])   # bottom -> top
    for b, v, pv in zip(bars, vals, prev):
        step = "" if pv is None else f" | {v / pv * 100:.1f}% of previous stage"
        ax.text(v + total * 0.01, b.get_y() + b.get_height() / 2,
                f"{int(v):,}  ({v / total * 100:.1f}% of sessions{step})", va="center", fontsize=8.5)
    ax.set_xlim(0, total * 1.75)
    ax.xaxis.set_major_formatter(thousands)
    ax.set_title("Conversion funnel: browsing (HBase) to orders (MongoDB)")
    ax.set_xlabel("Sessions")
    save(fig, out, "4_conversion_funnel")


def chart_conversion_by_segment(results, out):
    dev = read(results, "integrated_funnel_by_device.csv")
    ref = read(results, "integrated_funnel_by_referrer.csv")
    if dev is None or ref is None:
        return
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
    for ax, df, key, title in ((axes[0], dev, "device_type", "By device"),
                               (axes[1], ref, "referrer", "By traffic source")):
        df = df.sort_values("session_conversion_pct", ascending=False)
        bars = ax.bar(df[key].str.replace("_", " "), df["session_conversion_pct"], color=ORANGE)
        ax.bar_label(bars, labels=[f"{v:.1f}%" for v in df["session_conversion_pct"]],
                     padding=2, fontsize=9)
        ax.set_title(title)
        ax.tick_params(axis="x", rotation=20)
    axes[0].set_ylabel("Sessions that ended in an order (%)")
    axes[0].set_ylim(0, max(dev["session_conversion_pct"].max(),
                            ref["session_conversion_pct"].max()) * 1.25)
    fig.suptitle("Session conversion rate", fontweight="bold")
    save(fig, out, "5_conversion_by_segment")


def chart_top_products(results, out):
    df = read(results, "mongo_top_products.csv")
    if df is None:
        return
    top = df.sort_values("revenue", ascending=False).head(10).iloc[::-1]
    labels = [f"{n[:28]}…" if len(str(n)) > 28 else str(n) for n in top["name"]]
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    bars = ax.barh(labels, top["revenue"], color=BLUE)
    ax.bar_label(bars, labels=[f"{money(r)} · {u} units" for r, u in zip(top["revenue"], top["units_sold"])],
                 padding=3, fontsize=8)
    ax.xaxis.set_major_formatter(money)
    ax.set_xlim(0, top["revenue"].max() * 1.35)
    ax.set_title("Top 10 products by revenue")
    save(fig, out, "6_top_products")


def chart_cohort_heatmap(results, out):
    df = read(results, "spark_cohort.csv")
    if df is None:
        return
    # The last calendar month of the data is only a few days long. Its cells have a tiny buyer
    # rate and would look like a collapse in spending, so they are hidden and explained.
    partial = df["buyer_rate_pct"] < df["buyer_rate_pct"].median() * 0.25
    shown = df[~partial]
    pv = shown.pivot(index="registration_month", columns="month_offset",
                     values="revenue_per_cohort_user").sort_index()
    fig, ax = plt.subplots(figsize=(9, max(3.8, 0.4 * len(pv) + 1.8)))
    im = ax.imshow(pv.values, aspect="auto", cmap="Blues",
                   vmin=0, vmax=pv.max().max() * 1.15)
    ax.set_xticks(range(len(pv.columns)))
    ax.set_xticklabels(pv.columns)
    ax.set_yticks(range(len(pv.index)))
    ax.set_yticklabels(pv.index)
    for i in range(pv.shape[0]):
        for j in range(pv.shape[1]):
            v = pv.values[i, j]
            if v == v:                                    # not NaN
                ax.text(j, i, f"${v:,.0f}", ha="center", va="center", fontsize=8,
                        color="white" if v > pv.max().max() * 0.6 else "black")
    ax.set_xlabel("Months since registration")
    ax.set_ylabel("Registration cohort")
    ax.set_title("Revenue per cohort user and month, by registration cohort")
    ax.spines[:].set_visible(False)
    fig.colorbar(im, ax=ax, label="Revenue per user ($)")
    if partial.any():
        ax.text(0, -0.2, f"Hidden: {int(partial.sum())} cells from the final, partial month of data.",
                transform=ax.transAxes, color=GREY, fontsize=8)
    save(fig, out, "7_cohort_heatmap")


def main():
    ap = argparse.ArgumentParser(description="Create the project charts")
    ap.add_argument("--results", default="results")
    ap.add_argument("--out", default=None, help="default: <results>/figures")
    args, _ = ap.parse_known_args()
    out = args.out or os.path.join(args.results, "figures")
    print(f"reading CSVs from '{args.results}', writing PNGs to '{out}'")
    for fn in (chart_revenue_by_category, chart_monthly_revenue, chart_customer_segments,
               chart_funnel, chart_conversion_by_segment, chart_top_products, chart_cohort_heatmap):
        try:
            fn(args.results, out)
        except Exception as e:           # one bad chart must not stop the others
            print(f"  {fn.__name__} FAILED: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
