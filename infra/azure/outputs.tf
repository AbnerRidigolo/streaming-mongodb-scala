output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "producer_job" {
  description = "Start it with: az containerapp job start -g <resource_group> -n <producer_job>"
  value       = azurerm_container_app_job.producer.name
}

output "kafka_bootstrap" {
  description = "Event Hubs Kafka endpoint (EVENTHUBS_BOOTSTRAP for the local Scala job)."
  value       = local.kafka_bootstrap
}

output "listen_connection_string" {
  description = "Listen-only connection string for the local Scala job (EVENTHUBS_LISTEN_CONNECTION_STRING). Read with terraform output -raw; never commit it."
  value       = azurerm_eventhub_namespace_authorization_rule.consumer.primary_connection_string
  sensitive   = true
}
