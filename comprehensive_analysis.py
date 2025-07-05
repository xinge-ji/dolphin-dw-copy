import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np

# 设置中文字体支持和图表样式
plt.style.use("seaborn-v0_8")
plt.rcParams["font.sans-serif"] = ["SimHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.facecolor"] = "white"

# 读取数据
df = pd.read_csv("log/turnover.csv")
df["stat_month"] = pd.to_datetime(df["stat_month"])

# 指定要分析的门店ID
target_stores = [16887, 16890, 16898, 16908, 16913, 16918, 16920, 16922, 58158, 219343, 226990]

# 定义分界日期
cutoff_date = pd.to_datetime("2023-11-01")

# 过滤指定门店数据，并且只保留社区店类型
store_data = df[(df["storeid"].isin(target_stores)) & (df["store_type"] == "社区店")].copy()

# 按月份和门店聚合数据
store_monthly = (
    store_data.groupby(["stat_month", "storeid"])
    .agg(
        {
            "sales_amount": "sum",
            "total_sales_qty": "sum",
            "total_begin_qty": "sum",
            "total_end_qty": "sum",
            "avg_inventory_qty": "sum",
            "turnover_ratio": "first",
        }
    )
    .reset_index()
)

# 计算所有社区店的平均数据
community_stores = df[df["store_type"] == "社区店"].copy()
community_monthly = community_stores.groupby("stat_month").agg({"turnover_ratio": "mean", "sales_amount": "mean"}).reset_index()
community_monthly["avg_turnover_ratio"] = community_monthly["turnover_ratio"]
# community_monthly = (
#     community_stores.groupby("stat_month")
#     .agg({"sales_amount": "sum", "total_sales_qty": "sum", "avg_inventory_qty": "sum"})
#     .reset_index()
# )

# # 计算社区店平均周转率
# community_monthly["avg_turnover_ratio"] = community_monthly["total_sales_qty"] / community_monthly["avg_inventory_qty"]
# community_monthly["avg_turnover_ratio"] = community_monthly["avg_turnover_ratio"].replace(
#     [float("inf"), -float("inf")], 0
# )

# 创建综合分析图表
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
colors = sns.color_palette("Set3", len(target_stores))

# 1. 库存周转率时间趋势对比
ax1 = axes[0, 0]
for i, store_id in enumerate(target_stores):
    store_subset = store_monthly[store_monthly["storeid"] == store_id].copy()
    if not store_subset.empty:
        store_subset["turnover_ratio"] = pd.Series(store_subset["turnover_ratio"]).replace(
            [float("inf"), -float("inf")], 0
        )
        ax1.plot(
            store_subset["stat_month"],
            store_subset["turnover_ratio"],
            marker="o",
            linewidth=2,
            markersize=4,
            label=f"门店{store_id}",
            color=colors[i],
            alpha=0.8,
        )
# 添加社区店平均线
ax1.plot(
    community_monthly["stat_month"],
    community_monthly["avg_turnover_ratio"],
    marker="s",
    linewidth=3,
    markersize=6,
    label="社区店平均",
    color="red",
    linestyle="--",
    alpha=0.9,
)
# 添加分界线（恢复为datetime类型）
ax1.axvline(x=cutoff_date, color="orange", linestyle=":", linewidth=2, alpha=0.8, label="2023-11-01")
ax1.set_title("指定门店库存周转率时间趋势对比", fontsize=14, fontweight="bold")
ax1.set_xlabel("月份", fontsize=12)
ax1.set_ylabel("库存周转率", fontsize=12)
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax1.tick_params(axis="x", rotation=45)
ax1.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
ax1.grid(True, alpha=0.3, linestyle="-", linewidth=0.5)

# 2. 销售额时间趋势对比（增加社区店平均）
ax2 = axes[0, 1]
for i, store_id in enumerate(target_stores):
    store_subset = store_monthly[store_monthly["storeid"] == store_id].copy()
    if not store_subset.empty:
        store_subset["sales_amount_wan"] = store_subset["sales_amount"] / 10000
        ax2.plot(
            store_subset["stat_month"],
            store_subset["sales_amount_wan"],
            marker="o",
            linewidth=2,
            markersize=4,
            label=f"门店{store_id}",
            color=colors[i],
            alpha=0.8,
        )
# 增加社区店平均线
community_monthly["avg_sales_amount_wan"] = community_monthly["sales_amount"] / 10000
ax2.plot(
    community_monthly["stat_month"],
    community_monthly["avg_sales_amount_wan"],
    marker="s",
    linewidth=3,
    markersize=6,
    label="社区店平均",
    color="red",
    linestyle="--",
    alpha=0.9,
)
# 添加分界线
ax2.axvline(x=cutoff_date, color="orange", linestyle=":", linewidth=2, alpha=0.8, label="2023-11-01")
ax2.set_title("指定门店销售额时间趋势对比", fontsize=14, fontweight="bold")
ax2.set_xlabel("月份", fontsize=12)
ax2.set_ylabel("销售额(万元)", fontsize=12)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax2.tick_params(axis="x", rotation=45)
ax2.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
ax2.grid(True, alpha=0.3, linestyle="-", linewidth=0.5)

# 3. 周转率2023-11-01前后对比（含社区店基准）
ax3 = axes[1, 0]
before_data = store_monthly[store_monthly["stat_month"] < cutoff_date].groupby("storeid")["turnover_ratio"].mean()
after_data = store_monthly[store_monthly["stat_month"] >= cutoff_date].groupby("storeid")["turnover_ratio"].mean()
community_before = community_monthly[community_monthly["stat_month"] < cutoff_date]["avg_turnover_ratio"].mean()
community_after = community_monthly[community_monthly["stat_month"] >= cutoff_date]["avg_turnover_ratio"].mean()
valid_stores = [
    store
    for store in before_data.index
    if store in after_data.index and before_data[store] > 0 and after_data[store] > 0
]
before_values = [before_data[store] for store in valid_stores]
after_values = [after_data[store] for store in valid_stores]
store_labels = [f"门店{store}" for store in valid_stores]
# 插入社区店基准
before_values = [community_before] + before_values
after_values = [community_after] + after_values
store_labels = ["社区店平均"] + store_labels
x = np.arange(len(store_labels))
width = 0.35
bars1 = ax3.bar(x - width / 2, before_values, width, label="2023-11前", color="lightblue", alpha=0.7)
bars2 = ax3.bar(x + width / 2, after_values, width, label="2023-11后", color="lightcoral", alpha=0.7)
for bar in bars1:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width() / 2.0, height + 0.01, f"{height:.3f}", ha="center", va="bottom", fontsize=8)
for bar in bars2:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width() / 2.0, height + 0.01, f"{height:.3f}", ha="center", va="bottom", fontsize=8)
ax3.set_title("各门店2023-11-01前后平均库存周转率对比", fontsize=14, fontweight="bold")
ax3.set_xlabel("门店", fontsize=12)
ax3.set_ylabel("平均库存周转率", fontsize=12)
ax3.set_xticks(x)
ax3.set_xticklabels(store_labels, rotation=45)
ax3.legend()
ax3.grid(True, alpha=0.3, axis="y")

# 4. 销售额2023-11-01前后对比（增加社区店平均）
ax4 = axes[1, 1]
before_sales = store_monthly[store_monthly["stat_month"] < cutoff_date].groupby("storeid")["sales_amount"].mean()
after_sales = store_monthly[store_monthly["stat_month"] >= cutoff_date].groupby("storeid")["sales_amount"].mean()
# 计算社区店平均
community_sales_before = community_monthly[community_monthly["stat_month"] < cutoff_date]["sales_amount"].mean() / 10000
community_sales_after = community_monthly[community_monthly["stat_month"] >= cutoff_date]["sales_amount"].mean() / 10000
valid_stores_sales = [store for store in before_sales.index if store in after_sales.index]
before_sales_values = [before_sales[store] / 10000 for store in valid_stores_sales]
after_sales_values = [after_sales[store] / 10000 for store in valid_stores_sales]
store_labels_sales = [f"门店{store}" for store in valid_stores_sales]
# 插入社区店平均
before_sales_values = [community_sales_before] + before_sales_values
after_sales_values = [community_sales_after] + after_sales_values
store_labels_sales = ["社区店平均"] + store_labels_sales
x_sales = np.arange(len(store_labels_sales))
bars3 = ax4.bar(x_sales - width / 2, before_sales_values, width, label="2023-11前", color="lightgreen", alpha=0.7)
bars4 = ax4.bar(x_sales + width / 2, after_sales_values, width, label="2023-11后", color="lightsalmon", alpha=0.7)
for bar in bars3:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width() / 2.0, height + 0.1, f"{height:.2f}", ha="center", va="bottom", fontsize=8)
for bar in bars4:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width() / 2.0, height + 0.1, f"{height:.2f}", ha="center", va="bottom", fontsize=8)
ax4.set_title("各门店2023-11-01前后平均销售额对比", fontsize=14, fontweight="bold")
ax4.set_xlabel("门店", fontsize=12)
ax4.set_ylabel("平均销售额(万元)", fontsize=12)
ax4.set_xticks(x_sales)
ax4.set_xticklabels(store_labels_sales, rotation=45)
ax4.legend()
ax4.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig("comprehensive_analysis_chart.png", dpi=300, bbox_inches="tight", facecolor="white")
plt.show()

# 生成详细分析报告
print("=" * 80)
print("                    库存周转率与销售额综合分析报告")
print("=" * 80)

print("\n📊 数据概览:")
print(f"   分析期间: {df['stat_month'].min().strftime('%Y年%m月')} 至 {df['stat_month'].max().strftime('%Y年%m月')}")
print("   分界时间: 2023年11月01日")
print(f"   目标门店数量: {len(target_stores)}")
print(f"   实际有数据门店: {store_monthly['storeid'].nunique()}")

print("\n🔄 库存周转率分析:")
print("   2023-11-01前后对比:")

for store_id in valid_stores:
    if store_id in before_data.index and store_id in after_data.index:
        before_val = before_data[store_id]
        after_val = after_data[store_id]
        change = after_val - before_val
        change_pct = (change / before_val) * 100 if before_val != 0 else 0
        trend = "↗️" if change > 0 else "↘️" if change < 0 else "→"
        print(f"   门店{store_id}: {before_val:.4f} → {after_val:.4f} ({change:+.4f}, {change_pct:+.1f}%) {trend}")

print("\n💰 销售额分析:")
print("   2023-11-01前后对比:")

for store_id in valid_stores_sales:
    if store_id in before_sales.index and store_id in after_sales.index:
        before_val = before_sales[store_id] / 10000
        after_val = after_sales[store_id] / 10000
        change = after_val - before_val
        change_pct = (change / before_val) * 100 if before_val != 0 else 0
        trend = "↗️" if change > 0 else "↘️" if change < 0 else "→"
        print(
            f"   门店{store_id}: {before_val:.2f}万 → {after_val:.2f}万 ({change:+.2f}万, {change_pct:+.1f}%) {trend}"
        )

print("\n📈 社区店基准对比:")
community_turnover_before = community_monthly[community_monthly["stat_month"] < cutoff_date][
    "avg_turnover_ratio"
].mean()
community_turnover_after = community_monthly[community_monthly["stat_month"] >= cutoff_date][
    "avg_turnover_ratio"
].mean()
community_sales_after = community_monthly[community_monthly["stat_month"] >= cutoff_date]["sales_amount"].mean() / 10000

print(f"   周转率 - 2023-11前: {community_turnover_before:.4f}")
print(f"   周转率 - 2023-11后: {community_turnover_after:.4f}")
print(f"   周转率变化: {(community_turnover_after - community_turnover_before):+.4f}")
# print(f"   销售额 - 2023-11前: {community_sales_before:.2f}万")
# print(f"   销售额 - 2023-11后: {community_sales_after:.2f}万")
# print(f"   销售额变化: {(community_sales_after - community_sales_before):+.2f}万")

print("\n💡 关键发现:")
# 分析周转率改善最大的门店
turnover_improvements = []
for store_id in valid_stores:
    if store_id in before_data.index and store_id in after_data.index:
        change = after_data[store_id] - before_data[store_id]
        turnover_improvements.append((store_id, change))

turnover_improvements.sort(key=lambda x: x[1], reverse=True)
if turnover_improvements:
    best_improvement = turnover_improvements[0]
    worst_improvement = turnover_improvements[-1]
    print(f"   • 周转率改善最大: 门店{best_improvement[0]} ({best_improvement[1]:+.4f})")
    print(f"   • 周转率下降最大: 门店{worst_improvement[0]} ({worst_improvement[1]:+.4f})")

# 分析销售额变化最大的门店
sales_improvements = []
for store_id in valid_stores_sales:
    if store_id in before_sales.index and store_id in after_sales.index:
        change = (after_sales[store_id] - before_sales[store_id]) / 10000
        sales_improvements.append((store_id, change))

sales_improvements.sort(key=lambda x: x[1], reverse=True)
if sales_improvements:
    best_sales = sales_improvements[0]
    worst_sales = sales_improvements[-1]
    print(f"   • 销售额增长最大: 门店{best_sales[0]} ({best_sales[1]:+.2f}万)")
    print(f"   • 销售额下降最大: 门店{worst_sales[0]} ({worst_sales[1]:+.2f}万)")

print("\n" + "=" * 80)
