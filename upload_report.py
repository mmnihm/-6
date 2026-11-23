"""Per-batch upload failure reporting.

Pure logic only: no Telegram, database or asyncio dependencies, so it can be
unit-tested without a running bot. The in-memory bucket itself lives in main.py;
this module owns the bounded record shape and the admin-facing rendering.
"""

MAX_STORED = 50


def add_failure(bucket, name, step, error, cap=MAX_STORED):
    """Append one failure record to the per-user bucket, keeping it bounded."""
    if bucket is None:
        return bucket
    if len(bucket) < cap:
        bucket.append({
            "name": str(name or "未命名文件")[:120],
            "step": str(step or "")[:40],
            "error": str(error or "")[:200],
        })
    return bucket


def render_failures(items, limit=8):
    """Render failure records into a finish-summary suffix; empty when none."""
    if not items:
        return ""
    lines = ["\n⚠️ 失败明细："]
    for item in items[:limit]:
        name = item.get("name", "未命名文件")
        step = item.get("step", "")
        error = item.get("error", "")
        suffix = f"（{step}）" if step else ""
        lines.append(f"• {name}{suffix} {error}".strip())
    if len(items) > limit:
        lines.append(f"…其余 {len(items) - limit} 条见部署日志")
    return "\n".join(lines)
