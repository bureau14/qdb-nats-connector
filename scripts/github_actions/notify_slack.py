import os
from slack_sdk import WebClient


def get_slack_data() -> dict[str, str]:
    """Get Slack credentials from environment variables."""
    token = os.environ.get("SLACK_BOT_TOKEN")
    channel = os.environ.get("SLACK_BOT_CHANNEL")
    
    if not token:
        raise ValueError("SLACK_BOT_TOKEN environment variable is required")
    if not channel:
        raise ValueError("SLACK_BOT_CHANNEL environment variable is required")
    
    return {"token": token, "channel": channel}


def get_job_data() -> dict[str, str]:
    """Get GitHub Actions job data from environment variables."""
    return {
        "repository": os.environ.get("GITHUB_REPOSITORY", "unknown"),
        "workflow": os.environ.get("GITHUB_WORKFLOW", "unknown"),
        "actor": os.environ.get("GITHUB_ACTOR", "unknown"),
        "url": f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    }


def get_user(github_user: str) -> str:
    """Map GitHub username to Slack mention."""
    qdb_users = {
        # Add team members mapping here
        # "github_username": "<@slack_user_id>",
    }
    
    return qdb_users.get(github_user, f"@{github_user}")


def send_notification(client: WebClient, channel: str, blocks: list):
    """Send a Slack notification with custom blocks."""
    try:
        response = client.chat_postMessage(
            channel=channel,
            blocks=blocks
        )
        if response["ok"]:
            print(f"Slack notification sent successfully to {channel}")
        else:
            print(f"Failed to send Slack notification: {response['error']}")
    except Exception as e:
        print(f"Error sending Slack notification: {e}")