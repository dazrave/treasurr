"""Email templates for Treasurr notifications.

Each function returns (subject, html, text) tuple.
"""

from __future__ import annotations

_STYLE = """
<style>
body { margin:0; padding:0; background:#0e1117; color:#e6edf3; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif; }
.container { max-width:560px; margin:0 auto; padding:32px 24px; }
.header { text-align:center; padding-bottom:24px; border-bottom:1px solid #30363d; margin-bottom:24px; }
.header h1 { color:#c9a84c; font-size:24px; margin:0; letter-spacing:1px; }
.content { padding:0; }
.content h2 { color:#e6edf3; font-size:18px; margin:0 0 12px; }
.content p { color:#8b949e; font-size:14px; line-height:1.6; margin:8px 0; }
.badge { display:inline-block; padding:6px 16px; border-radius:6px; font-weight:600; font-size:14px; }
.badge-warning { background:#d29922; color:#0e1117; }
.badge-danger { background:#f85149; color:#fff; }
.meter { background:#161b22; border-radius:8px; height:20px; overflow:hidden; margin:16px 0; border:1px solid #30363d; }
.meter-fill { height:100%; border-radius:8px; transition:width 0.3s; }
.meter-75 { background:linear-gradient(90deg,#c9a84c,#d29922); }
.meter-95 { background:linear-gradient(90deg,#d29922,#f85149); }
.meter-100 { background:#f85149; }
.footer { margin-top:32px; padding-top:16px; border-top:1px solid #30363d; text-align:center; }
.footer p { color:#484f58; font-size:12px; margin:0; }
.highlight { color:#c9a84c; font-weight:600; }
</style>
"""


def _wrap_html(body: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{_STYLE}</head>
<body>
<div class="container">
  <div class="header"><h1>Treasurr</h1></div>
  <div class="content">{body}</div>
  <div class="footer"><p>Treasurr - Your treasure. Your crew. Your plunder.</p></div>
</div>
</body>
</html>"""


def quota_warning_template(
    username: str,
    threshold: int,
    usage_percent: float,
    used_display: str,
    total_display: str,
) -> tuple[str, str, str]:
    """Warning email at 75% or 95% quota usage."""
    meter_class = "meter-95" if threshold >= 95 else "meter-75"
    badge_class = "badge-danger" if threshold >= 95 else "badge-warning"
    fill_pct = min(usage_percent, 100)

    subject = f"Treasurr: You've used {threshold}% of your storage"

    html = _wrap_html(f"""
  <h2>Ahoy, {username}!</h2>
  <p>You've used <span class="{badge_class} badge">{usage_percent:.0f}%</span> of your storage quota.</p>
  <div class="meter"><div class="meter-fill {meter_class}" style="width:{fill_pct}%"></div></div>
  <p><span class="highlight">{used_display}</span> used of <span class="highlight">{total_display}</span></p>
  <p>Consider deleting content you no longer watch to free up space. If your quota fills up, new requests will be declined.</p>
""")

    text = (
        f"Ahoy, {username}!\n\n"
        f"You've used {usage_percent:.0f}% of your Treasurr storage quota.\n"
        f"{used_display} used of {total_display}.\n\n"
        f"Consider deleting content you no longer watch to free up space. "
        f"If your quota fills up, new requests will be declined.\n"
    )

    return subject, html, text


def quota_exceeded_template(
    username: str,
    title: str,
    usage_percent: float,
    used_display: str,
    total_display: str,
) -> tuple[str, str, str]:
    """Request declined because quota is full."""
    subject = f"Treasurr: Request declined - {title}"

    html = _wrap_html(f"""
  <h2>Request Declined</h2>
  <p>Ahoy, {username}. Your request for <span class="highlight">{title}</span> was declined because your storage quota is full.</p>
  <div class="meter"><div class="meter-fill meter-100" style="width:100%"></div></div>
  <p><span class="highlight">{used_display}</span> used of <span class="highlight">{total_display}</span> ({usage_percent:.0f}%)</p>
  <p>Free up space by deleting content you no longer watch, then try requesting again.</p>
""")

    text = (
        f"Request Declined\n\n"
        f"Ahoy, {username}. Your request for \"{title}\" was declined because your "
        f"storage quota is full.\n\n"
        f"{used_display} used of {total_display} ({usage_percent:.0f}%).\n\n"
        f"Free up space by deleting content you no longer watch, then try requesting again.\n"
    )

    return subject, html, text


def download_cancelled_template(
    username: str,
    title: str,
    reason: str,
) -> tuple[str, str, str]:
    """Download was cancelled due to quota enforcement."""
    subject = f"Treasurr: Download cancelled - {title}"

    html = _wrap_html(f"""
  <h2>Download Cancelled</h2>
  <p>Ahoy, {username}. The download of <span class="highlight">{title}</span> has been cancelled.</p>
  <p><strong>Reason:</strong> {reason}</p>
  <p>Free up space by deleting content you no longer watch, then try requesting again.</p>
""")

    text = (
        f"Download Cancelled\n\n"
        f"Ahoy, {username}. The download of \"{title}\" has been cancelled.\n\n"
        f"Reason: {reason}\n\n"
        f"Free up space by deleting content you no longer watch, then try requesting again.\n"
    )

    return subject, html, text
