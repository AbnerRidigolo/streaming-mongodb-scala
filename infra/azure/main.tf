# Phase 5: the cloud half of the pipeline on Azure.
#
#   Container Apps Job (producers) --SASL_SSL--> Event Hubs Standard (Kafka endpoint)
#                                                   |
#                     delivery-sla-job (Scala, local) <--+  same code, configuration only
#
# Kafka Streams (order-status-service) stays local: on Event Hubs it needs the
# Premium or Dedicated tier (and is in preview there). Destroy with
# `terraform destroy` as soon as the run and screenshots are done.

locals {
  name = "${var.prefix}-${random_string.suffix.result}"

  tags = {
    project     = "streaming-mongodb-scala"
    phase       = "5-azure"
    environment = "demo"
    managed_by  = "terraform"
    owner       = var.owner
  }

  # Same names as the local topics, so only the bootstrap server changes.
  event_hubs = {
    "orders-raw"      = 3
    "delivery-events" = 2
  }

  kafka_bootstrap = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
}

resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------- Event Hubs

resource "azurerm_eventhub_namespace" "this" {
  name                = "evhns-${local.name}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "Standard" # Kafka endpoint; Basic has none.
  capacity            = 1          # 1 throughput unit: 1 MB/s or 1,000 events/s in.
  minimum_tls_version = "1.2"
  tags                = local.tags
}

resource "azurerm_eventhub" "topics" {
  for_each = local.event_hubs

  name              = each.key
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = each.value
  message_retention = 1 # days
}

# Least privilege: the producer job can only send, the local Scala job can only
# listen. Each rule has its own connection string.
resource "azurerm_eventhub_namespace_authorization_rule" "producer" {
  name                = "producer-send"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  send                = true
  listen              = false
  manage              = false
}

resource "azurerm_eventhub_namespace_authorization_rule" "consumer" {
  name                = "consumer-listen"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  send                = false
  listen              = true
  manage              = false
}

# ------------------------------------------------------------ Container Apps

resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-${local.name}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  daily_quota_gb      = 0.5 # cost guard: a few MB of producer logs are expected
  tags                = local.tags
}

resource "azurerm_container_app_environment" "this" {
  name                       = "cae-${local.name}"
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
  tags                       = local.tags
}

resource "azurerm_container_app_job" "producer" {
  name                         = "caj-producer-${random_string.suffix.result}"
  location                     = azurerm_resource_group.this.location
  resource_group_name          = azurerm_resource_group.this.name
  container_app_environment_id = azurerm_container_app_environment.this.id
  tags                         = local.tags

  replica_timeout_in_seconds = 3600
  replica_retry_limit        = 0

  # Started by hand (az containerapp job start), runs once and stops: billed
  # only while it runs.
  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }

  secret {
    name  = "eventhubs-send"
    value = azurerm_eventhub_namespace_authorization_rule.producer.primary_connection_string
  }

  template {
    container {
      name   = "producer"
      image  = var.producer_image
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "KAFKA_BOOTSTRAP_SERVERS"
        value = local.kafka_bootstrap
      }
      env {
        name  = "VALUE_FORMAT"
        value = "json"
      }
      env {
        name  = "SAMPLE_ORDERS"
        value = tostring(var.sample_orders)
      }
      env {
        name  = "EVENTS_PER_SECOND"
        value = tostring(var.events_per_second)
      }
      # librdkafka settings for the Event Hubs Kafka endpoint (KAFKA_CFG_*
      # -> dotted property names). Only gzip compression is accepted.
      env {
        name  = "KAFKA_CFG_SECURITY_PROTOCOL"
        value = "SASL_SSL"
      }
      env {
        name  = "KAFKA_CFG_SASL_MECHANISM"
        value = "PLAIN"
      }
      env {
        name  = "KAFKA_CFG_SASL_USERNAME"
        value = "$ConnectionString" # literal: Event Hubs expects this user name
      }
      env {
        name        = "KAFKA_CFG_SASL_PASSWORD"
        secret_name = "eventhubs-send"
      }
      env {
        name  = "KAFKA_CFG_COMPRESSION_TYPE"
        value = "gzip"
      }
      env {
        name  = "KAFKA_CFG_REQUEST_TIMEOUT_MS"
        value = "60000"
      }
      env {
        name  = "KAFKA_CFG_METADATA_MAX_AGE_MS"
        value = "180000"
      }
      env {
        name  = "KAFKA_CFG_SOCKET_KEEPALIVE_ENABLE"
        value = "true"
      }
    }
  }

  depends_on = [azurerm_eventhub.topics]
}

# -------------------------------------------------------------------- Budget

resource "azurerm_consumption_budget_resource_group" "this" {
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.this.id
  amount            = var.budget_amount
  time_grain        = "Monthly"

  time_period {
    # Must be the first day of a month; fixed at creation (see lifecycle).
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  notification {
    enabled        = true
    operator       = "GreaterThanOrEqualTo"
    threshold      = 50
    threshold_type = "Actual"
    contact_emails = var.budget_alert_emails
  }

  notification {
    enabled        = true
    operator       = "GreaterThanOrEqualTo"
    threshold      = 100
    threshold_type = "Actual"
    contact_emails = var.budget_alert_emails
  }

  notification {
    enabled        = true
    operator       = "GreaterThanOrEqualTo"
    threshold      = 100
    threshold_type = "Forecasted"
    contact_emails = var.budget_alert_emails
  }

  lifecycle {
    ignore_changes = [time_period]
  }
}
