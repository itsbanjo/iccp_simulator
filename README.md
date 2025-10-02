# ICCP Simulator v2.4.0 - Elastic APM Integration

## What This Does

The ICCP simulator now sends logs to Elasticsearch with automatic correlation to APM traces. Every log line includes trace IDs so you can jump from a trace in APM straight to the logs and back.

## Version History

- **v2.4.0** (Oct 1, 2025): Added APM log correlation, ECS formatting, performance tracking
- **v2.3.1** (Oct 1, 2025): Fixed traceparent propagation for distributed tracing
- **v2.3.0**: Initial APM integration

## How It Works

The simulator uses Elastic APM's built-in log correlation. When you initialize the APM client, it automatically adds trace IDs to your log records:
```python
from elasticapm.handlers.logging import Formatter as ElasticFormatter

handler = logging.StreamHandler()
elastic_formatter = ElasticFormatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(elastic_formatter)
```

This appends transaction.id, trace.id, and span.id to every log line automatically.
## Configuration
# Required Environment Variables

```
bash
ELASTIC_APM_SERVICE_NAME=iccp-simulator
ELASTIC_APM_SERVER_URL=https://apm-server-sample-apm-http.elastic-observability.svc.cluster.local:8200
ELASTIC_APM_SECRET_TOKEN=your-token-here
```

# Optional Settings
```
bash
# Log level (default: INFO)
LOG_LEVEL=DEBUG

# ECS log reformatting - experimental feature that reformats logs to Elasticsearch Common Schema
# Options: override (reformat everything), replace (only unformatted logs), off (disabled)
ELASTIC_APM_LOG_ECS_REFORMATTING=override

# APM sampling rate (default: 1.0 = 100%)
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=1.0

# Site configuration
SITE_NAME=auckland-penrose
KAFKA_BROKERS=transpower-kafka-kafka-bootstrap:9092
```
# What Gets Logged
## Startup

Site configuration
Kafka broker connection
APM server URL and sampling rate

# During Operation

**Every message (DEBUG)**: Type, customer, message number  
**Every send (INFO)**: Topic, partition, offset, latency  
**Throughput (INFO)**: Messages/sec every 10 seconds  
**Protection events (WARNING)**: Uncleared HIGH/CRITICAL alarms  
**Milestones (INFO)**: At 10, 50, 100, 500, 1000 messages  
**Errors (ERROR)**: Full stack trace with correlation IDs  

# Shutdown

Total messages sent
Average throughput
Total uptime

# Log Correlation in Action
Here's what a correlated log looks like:
```
2025-10-01 10:30:45 - iccp_simulator - INFO - ✓ PROTECTION_EVENT → iccp-protection-events partition=1 offset=4523 msg_num=847 latency=12.3ms | elasticapm transaction.id=abc123 trace.id=xyz789 span.id=def456
```

In Kibana, you can:

Click the trace.id to see the full distributed trace
Filter logs by transaction.id to see all logs for that message
Search for specific sites: labels.site_id:AKL_PENROSE

# Searching Logs in Kibana
## Find All Logs for a Trace  
`trace.id:"xyz789"  `  
Find Logs for a Specific Site  
`labels.site_id:"AKL_PENROSE"  `
Find All Protection Events  
`message:"PROTECTION_EVENT" AND severity:"CRITICAL"`  
Find High Latency Sends  
`message:"latency" AND message:>100ms`  
Find Throughput Stats  
`message:"Throughput"`  

Performance Impact  
The logging overhead is minimal:  

DEBUG logs (disabled by default): ~2% overhead  
INFO logs (default): <1% overhead  
APM trace correlation: negligible  
ECS reformatting: <1% overhead  

You can reduce overhead further:  

```
bash
LOG_LEVEL=WARNING  # Only log warnings and errors
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=0.5  # Sample 50% of transactions
```
# Troubleshooting
##Logs Don't Show Trace IDs
Check that APM is initialized before logging:

```
python
# CORRECT ORDER:
from elasticapm import Client
apm_client = Client({...})
elasticapm.instrument()
# Then configure logging
import logging
```
ECS Reformatting Not Working
Set the environment variable explicitly:

```
bash
ELASTIC_APM_LOG_ECS_REFORMATTING=override
```
Or disable it if causing issues:

```
bash
ELASTIC_APM_LOG_ECS_REFORMATTING=off
```

# Can't Find Logs in Kibana
Make sure you're ingesting logs to Elasticsearch. The APM agent doesn't send logs directly - you need Filebeat or another log shipper.
Example Filebeat config:


```
yaml
filebeat.inputs:
- type: container
  paths:
    - /var/log/containers/iccp-simulator-*.log
  json.keys_under_root: true
  json.add_error_key: true

output.elasticsearch:
  hosts: ["observability-cluster-es-http:9200"]
  username: elastic
  password: ${ELASTICSEARCH_PASSWORD}
```
# Deployment on OpenShift
The deployment YAML needs these environment variables:


```
yaml
env:
- name: ELASTIC_APM_SERVICE_NAME
  value: "iccp-simulator"
- name: ELASTIC_APM_SERVER_URL
  value: "https://apm-server-sample-apm-http.elastic-observability.svc.cluster.local:8200"
- name: ELASTIC_APM_SECRET_TOKEN
  valueFrom:
    secretKeyRef:
      name: apm-token
      key: secret-token
- name: ELASTIC_APM_VERIFY_SERVER_CERT
  value: "false"
- name: ELASTIC_APM_TRANSACTION_SAMPLE_RATE
  value: "1.0"
- name: LOG_LEVEL
  value: "INFO"
- name: ELASTIC_APM_LOG_ECS_REFORMATTING
  value: "override"
```

# Next Steps  

1. Deploy the updated simulator  
2. Wait for logs to appear in Elasticsearch  
3. In Kibana, go to Observability → APM → Services → iccp-simulator  
4. Click any transaction  
5. Scroll down to "Logs" tab - you'll see correlated logs with the trace  
6. Click "View in Logs" to see full log context  

# References

[Elastic APM Python - Logs Documentation](https://www.elastic.co/docs/reference/apm/agents/python/logs)  
[ECS Logging Format](https://www.elastic.co/docs/reference/ecs/logging/intro)  
[Filebeat Setup](https://www.elastic.co/beats/filebeat)  
