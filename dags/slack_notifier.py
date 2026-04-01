import requests
from datetime import datetime

import os
WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def notify(message: str, level: str = "info", fields: list = None):
    colours = {
        "info":    "#06B6D4",
        "success": "#22C55E",
        "warning": "#F59E0B",
        "error":   "#EF4444",
    }
    icons = {
        "info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "🚨"
    }
    
    payload = {
        "attachments": [{
            "color": colours.get(level, "#06B6D4"),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{icons.get(level, 'ℹ️')} *Glasgow Traders AutoPipe*\n{message}"
                    }
                },
                *([{
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*{f['title']}*\n{f['value']}"}
                        for f in fields
                    ]
                }] if fields else []),
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", 
                                  "text": f"🏴󠁧󠁢󠁳󠁣󠁴󠁿 {datetime.now().strftime('%d %b %Y, %H:%M')} UTC"}]
                }
            ]
        }]
    }
    
    resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
    if not resp.ok:
        print(f"Slack Error: {resp.text}")
    resp.raise_for_status()
