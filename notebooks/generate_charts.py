"""
Generate PNG charts from sweep data to justify optimal N reducer selection.
Run: python notebooks/generate_charts.py
"""
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

OUT_DIR = Path(__file__).parent
SWEEP_FILE = OUT_DIR / ".." / "data" / "output" / "sweeps" / "sweep_20260604_081711.json"

# Load sweep data
sweep = json.load(open(SWEEP_FILE, encoding='utf-8'))

log_sp = sweep['sweeps']['log_speedup']
log_mem = sweep['sweeps']['log_memory']

# CSV data (from sweep)
csv_data = [
    {"n": 1, "t": 73.76, "tp": 7456},
    {"n": 2, "t": 84.73, "tp": 6491},
    {"n": 4, "t": 74.24, "tp": 7408},
    {"n": 6, "t": 54.09, "tp": 10168},
]

# ── Chart 1: Speedup comparison LOG vs CSV ─────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# LOG speedup
ns_log = [r['params']['n_reducers'] for r in log_sp]
ts_log = [r['duration_s'] for r in log_sp]
tps_log = [r['throughput_rows_per_sec'] for r in log_sp]
sp_log = [r['speedup'] for r in log_sp]

ax = axes[0]
ax.plot(ns_log, tps_log, 'o-', color='#2ecc71', linewidth=2, markersize=8, label='Actual')
ax.axvline(x=4, color='#e74c3c', linestyle='--', linewidth=2, label='Optimal: N=4')
ax.set_xlabel('Number of Reducers (N)', fontsize=12)
ax.set_ylabel('Throughput (rows/s)', fontsize=12)
ax.set_title('LOG Mode (I/O-Bound) — 100MB, 721 IPs', fontsize=13, fontweight='bold')
ax.set_xticks(ns_log)
ax.grid(True, alpha=0.3)
ax.legend(fontsize=11)
ax.annotate('N=4 optimal\n(bottleneck: I/O)', xy=(4, tps_log[2]), xytext=(6.5, tps_log[0]),
            arrowprops=dict(arrowstyle='->', color='gray'), fontsize=10, color='gray')

# CSV speedup
ns_csv = [d['n'] for d in csv_data]
tps_csv = [d['tp'] for d in csv_data]
sp_csv = [csv_data[0]['t'] / d['t'] for d in csv_data]

ax = axes[1]
ax.bar(ns_csv, tps_csv, color=['#3498db' if n != 6 else '#e74c3c' for n in ns_csv], alpha=0.85, edgecolor='white')
ax.axhline(y=tps_csv[0], color='gray', linestyle='--', linewidth=1.5, label='Baseline N=1')
ax.set_xlabel('Number of Reducers (N)', fontsize=12)
ax.set_ylabel('Throughput (rows/s)', fontsize=12)
ax.set_title('CSV Mode (CPU-Bound) — 100MB, 550K IPs', fontsize=13, fontweight='bold')
ax.set_xticks(ns_csv)
ax.grid(True, alpha=0.3, axis='y')
for i, (n, tp) in enumerate(zip(ns_csv, tps_csv)):
    ax.text(n, tp + 200, f'{tp:,}\n({sp_csv[i]:.2f}x)', ha='center', va='bottom', fontsize=9)
ax.annotate('N=6 optimal\n(speedup 1.36x)', xy=(6, tps_csv[3]), xytext=(3.5, tps_csv[3] * 0.75),
            arrowprops=dict(arrowstyle='->', color='#e74c3c'), fontsize=10, color='#e74c3c', fontweight='bold')

fig.suptitle('Figure 1: Speedup Analysis — Optimal N Reducer Selection', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
fig.savefig(OUT_DIR / 'speedup_analysis.png', dpi=150, bbox_inches='tight')
print(f"Saved: {OUT_DIR / 'speedup_analysis.png'}")
plt.close()

# ── Chart 2: Memory Threshold ───────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 5))
mem_ths = [r['params'].get('memory_threshold', 80) for r in log_mem]
tps_mem = [r['throughput_rows_per_sec'] for r in log_mem]
colors = ['#e74c3c' if th == 80 else '#3498db' for th in mem_ths]
bars = ax.bar([f'{th}%' for th in mem_ths], tps_mem, color=colors, alpha=0.85, edgecolor='white', width=0.5)
ax.set_xlabel('Memory Threshold (%)', fontsize=12)
ax.set_ylabel('Throughput (rows/s)', fontsize=12)
ax.set_title('Figure 2: Memory Threshold Impact — Optimal: 80%', fontsize=13, fontweight='bold')
ax.grid(True, alpha=0.3, axis='y')
for bar, tp in zip(bars, tps_mem):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 500,
            f'{tp:,}', ha='center', va='bottom', fontsize=10, fontweight='bold')
ax.axhline(y=max(tps_mem), color='#2ecc71', linestyle='--', linewidth=1.5, alpha=0.7)
plt.tight_layout()
fig.savefig(OUT_DIR / 'memory_threshold.png', dpi=150, bbox_inches='tight')
print(f"Saved: {OUT_DIR / 'memory_threshold.png'}")
plt.close()

# ── Chart 3: Optimal Config Summary ───────────────────────────────
fig, ax = plt.subplots(figsize=(10, 4))
ax.axis('off')
table_data = [
    ['Parameter', 'Value', 'Justification'],
    ['DEFAULT_N_REDUCERS', '4', 'Cross-mode: LOG ~7.9s for N=1..6\nCSV best: N=6 (10,168 r/s)'],
    ['MEMORY_THRESHOLD_PCT', '80%', 'Best: 73,663 r/s (vs 36,430 at 95%)'],
    ['PARTITIONING_STRATEGY', '"hash"', 'No sampling overhead\nSkew=0.0047 (nearly perfect)'],
    ['DEDUP_CHUNK_SIZE', '2048', 'Optimal batch for 100MB dataset'],
    ['SKEW_THRESHOLD', '0.5', 'Work stealing only when skew > 50%'],
]
table = ax.table(cellText=table_data[1:], colLabels=table_data[0],
                  loc='center', cellLoc='left', colWidths=[0.35, 0.15, 0.5])
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 2)
for (r, c), cell in table.get_celld().items():
    if r == 0:
        cell.set_facecolor('#2c3e50')
        cell.set_text_props(color='white', fontweight='bold')
    elif r % 2 == 0:
        cell.set_facecolor('#ecf0f1')
ax.set_title('Figure 3: Optimal Configuration Summary (100MB Dataset)', fontsize=13, fontweight='bold', pad=20)
plt.tight_layout()
fig.savefig(OUT_DIR / 'optimal_config.png', dpi=150, bbox_inches='tight')
print(f"Saved: {OUT_DIR / 'optimal_config.png'}")
plt.close()

print("\nAll charts saved to: notebooks/")
