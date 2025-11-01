# Alerting

# Alert System

The alert system enables automated notifications when pipeline errors occur. Configure channels (where alerts go) and triggers (when alerts are sent) through YAML or JSON configuration files.

## Configuration Formats

Alert configurations can be written in either **YAML** or **JSON** format. Both formats are functionally equivalent:

- **YAML** (`.yaml`, `.yml`): More readable, supports comments natively
- **JSON/JSONC** (`.json`, `.jsonc`): Structured format, widely supported in tools

The framework automatically detects the format based on the file extension.

## How It Works

1. An error occurs in your pipeline
2. All triggers evaluate against the error using AND logic (all rules must match)
3. Matching triggers apply template formatting to the alert title and body
4. Formatted alerts are sent to all channels referenced by the trigger

## Configuration Structure

Alert configuration can be defined inline within your job or in a separate file for reuse across pipelines:

```bash
# Separate alert configuration for reuse
python -m samara run \
    --alert-filepath="examples/alerts.jsonc" \
    --workflow-filepath="examples/job.jsonc"
```

The configuration requires this structure:

```jsonc
{
    "alert": {
        "channels": [/* channel definitions */],
        "triggers": [/* trigger definitions */]
    }
}
```

Channels without associated triggers never send alerts.

## Channels

Channels are reusable notification endpoints. Define once, reference from any trigger. Multiple triggers can use the same channel, and one trigger can send to multiple channels.

**Available channel types**:
- **Email**: SMTP delivery to email recipients
- **HTTP**: Webhook requests to APIs (Slack, PagerDuty, Teams, custom endpoints)
- **File**: Local filesystem logging

**Email channel example**:

```jsonc
{
    "alert": {
        "channels": [
            {
                "name": "ops-email",
                "description": "Production operations team alerts",
                "enabled": true,
                "channel_type": "email",
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "username",
                "password": "password",
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com", "oncall@company.com"]
            }
            // ... Add more channels
        ],
        "triggers": [
            /* Trigger definitions */
            // A channel without trigger does nothing.
        ]
    }
}
```

See [channels documentation](channels.md) for all configurable channels.

## Triggers

Triggers determine when to send alerts and to which channels. All rules within a trigger use AND logic — every rule must match for the trigger to fire. Multiple triggers can evaluate the same exception, each routing to different channels with custom message formatting.

**Trigger example**:

```jsonc
{
    "alert": {
        "channels": [
            {
                "name": "network-ops-email",
                "description": "Network operations team alerts",
                "enabled": true,
                "channel_type": "email",
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "username",
                "password": "password",
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com", "oncall@company.com"]
            }
            // ... Add more channels
        ],
        "triggers": [
            {
                "id": "network-errors",
                "description": "Network errors to network-ops-email",
                "enabled": true,
                "channel_ids": ["network-ops-email"],
                "template": {
                    "prepend_title": "NET: ",
                    "append_title": "",
                    "prepend_body": "",
                    "append_body": ""
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["acceptance", "production"]},
                    {"rule": "exception_regex", "pattern": ".*network.*|.*connection.*|.*timeout.*"}
                ]
            }
            // ... Add more triggers 
        ]
    }
}
```

See [triggers documentation](triggers.md) for rule types and configuration details.

## Testing Alert Configuration

Test alert configuration without running a full pipeline using the `validate` command:

```bash
python -m samara validate \
    --alert-filepath="examples/alerts.jsonc" \
    --workflow-filepath="examples/job.jsonc" \
    --test-exception="database connection failed" \
    --test-env-var=ENVIRONMENT=production
```

This simulates an error, evaluates all triggers, and sends alerts through matching channels to verify configuration.

## Example Configuration

This example demonstrates intelligent error routing to quickly direct issues to the right team:

- **Production errors** are sent to PagerDuty so the on-call person is notified immediately for critical issues requiring urgent response
- **Development errors** are posted to Slack (slack-dev channel) so developers can see and address issues in their development environment
- **Database errors** (matching patterns like "database", "sql", "connection pool") are emailed to the operations team for specialized troubleshooting by the networking or operations team

This routing strategy ensures that each team receives only the alerts relevant to their responsibilities, reducing noise and accelerating response times.

```jsonc
{
    "alert": {
        "channels": [
            {
                "name": "pagerduty-oncall",
                "description": "PagerDuty webhook for critical production issues",
                "enabled": true,
                "channel_type": "http",
                "url": "https://events.pagerduty.com/v2/enqueue",
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "timeout": 30,
                "retry": {
                    "max_attempts": 3,
                    "delay_in_seconds": 30
                }
            },
            {
                "name": "ops-email",
                "description": "Operations team email alerts",
                "enabled": true,
                "channel_type": "email",
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "alerts@company.com",
                "password": "${SMTP_PASSWORD}",  // Use environment variables for credentials
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com", "oncall@company.com"]
            },
            {
                "name": "slack-dev",
                "description": "Development team Slack channel",
                "enabled": true,
                "channel_type": "http",
                "url": "https://hooks.slack.com/services/DEV/WEBHOOK/URL",
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "timeout": 30,
                "retry": {
                    "max_attempts": 3,
                    "delay_in_seconds": 30
                }
            }
        ],
        "triggers": [
            {
                "name": "prod-critical-errors",
                "description": "Route production errors to PagerDuty for immediate response",
                "enabled": true,
                "channel_ids": ["pagerduty-oncall"],
                "template": {
                    "prepend_title": "🚨 PRODUCTION: ",
                    "append_title": "",
                    "prepend_body": "Critical error in production environment:\n\n",
                    "append_body": "\n\nImmediate action required."
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]}
                ]
            },
            {
                "name": "dev-errors",
                "description": "Development environment errors to Slack",
                "enabled": true,
                "channel_ids": ["slack-dev"],
                "template": {
                    "prepend_title": "DEV: ",
                    "append_title": "",
                    "prepend_body": "",
                    "append_body": ""
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["development", "dev"]}
                ]
            },
            {
                "name": "database-errors",
                "description": "Database-related issues to operations email",
                "enabled": true,
                "channel_ids": ["ops-email"],
                "template": {
                    "prepend_title": "DB: ",
                    "append_title": "",
                    "prepend_body": "",
                    "append_body": ""
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]}
                    {"rule": "exception_regex", "pattern": ".*database.*|.*sql.*|.*connection pool.*"}
                ]
            }
        ]
    }
}
```
