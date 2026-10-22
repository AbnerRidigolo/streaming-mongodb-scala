variable "subscription_id" {
  description = "Azure subscription to deploy into (az account show --query id -o tsv)."
  type        = string
}

variable "location" {
  description = "Azure region. East US is among the cheapest; the data is synthetic, so residency does not matter."
  type        = string
  default     = "eastus"
}

variable "prefix" {
  description = "Prefix for resource names; a random suffix keeps the Event Hubs namespace globally unique."
  type        = string
  default     = "olist"
}

variable "budget_amount" {
  description = "Monthly budget (in the subscription's billing currency) for the resource group; alerts at 50% and 100%."
  type        = number
  default     = 5
}

variable "budget_alert_emails" {
  description = "Who receives the budget alerts. Keep it in terraform.tfvars (git-ignored)."
  type        = list(string)

  validation {
    condition     = length(var.budget_alert_emails) > 0
    error_message = "Set at least one e-mail for the budget alerts."
  }
}

variable "producer_image" {
  description = "Cloud producer image (producers/Dockerfile.cloud), published to GHCR by the cloud-producer-image workflow. Must be public."
  type        = string
  default     = "ghcr.io/abnerridigolo/olist-cloud-producer:latest"
}

variable "sample_orders" {
  description = "Synthetic orders the producer job generates and replays once."
  type        = number
  default     = 2000
}

variable "events_per_second" {
  description = "Publish rate of each producer (orders and deliveries run in parallel)."
  type        = number
  default     = 10
}

variable "owner" {
  description = "Value of the owner tag on every resource."
  type        = string
  default     = "portfolio"
}
