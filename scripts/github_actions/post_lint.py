#!/usr/bin/env python3

import sys
from notify_slack import get_slack_data, get_job_data, send_notification, get_user
from slack_sdk import WebClient


def parse_outcomes(args):
    """Parse key=value arguments generically."""
    outcomes = {}
    for arg in args[1:]:  # Skip script name
        if '=' in arg:
            key, value = arg.split('=', 1)
            outcomes[key] = value
    return outcomes


def humanize_linter_name(name):
    """Convert go_fmt to Go Format, etc."""
    name_map = {
        'go_fmt': 'Go Format',
        'go_vet': 'Go Vet',
        'staticcheck': 'Staticcheck',
        'ineffassign': 'Ineffassign',
        'golangci_lint': 'GolangCI-Lint'
    }
    return name_map.get(name, name.replace('_', ' ').title())


def main():
    """Main function to handle linting notification."""
    # Parse all linter outcomes generically
    outcomes = parse_outcomes(sys.argv)
    
    if not outcomes:
        print("No linter outcomes provided")
        return 0

    # Find failed linters
    failed_linters = [
        humanize_linter_name(linter)
        for linter, outcome in outcomes.items()
        if outcome == "failure"
    ]

    # Only send notification if there are failures
    if failed_linters:
        try:
            slack_data = get_slack_data()
            client = WebClient(token=slack_data["token"])
            job_data = get_job_data()

            # Build message blocks
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":x: Linting failed for {len(failed_linters)} tool(s)",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Failed Linters:*\n" + "\n".join(f"• {linter}" for linter in failed_linters),
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Repository:* {job_data['repository']}\n"
                            f"*Workflow:* {job_data['workflow']}\n"
                            f"*Triggered by:* {get_user(job_data['actor'])}\n"
                            f"*Job link:* <{job_data['url']}|View details>"
                        ),
                    },
                },
            ]

            send_notification(client, slack_data["channel"], blocks)
        except Exception as e:
            print(f"Error sending Slack notification: {e}")
            return 1
    else:
        print("All linting checks passed - no notification needed")

    return 0


if __name__ == "__main__":
    sys.exit(main())