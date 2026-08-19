# Define variables
COMPOSE := docker compose --env-file ../docker/.env --env-file .env -f docker-compose.yaml
CONNECTION_IDS := minio sec_api massive_api alphavantage_api

# Indicate that these are commands/actions, not file names
.PHONY: build compose-config compose-init compose-up compose-down compose-reset check-connections help

build:
	$(COMPOSE) build

compose-config:
	$(COMPOSE) config -q

compose-init: build
	$(COMPOSE) up airflow-init
	$(COMPOSE) up -d

compose-up:
	$(COMPOSE) up -d
	$(COMPOSE) ps

compose-down:
	$(COMPOSE) down --remove-orphans

compose-reset: compose-down compose-up

# Run check_connections.py inside airflow-cli container
check-connections:
	@$(COMPOSE) run --rm --no-deps airflow-cli python /opt/airflow/plugins/check_connections.py

help:
	@echo "build              Build the Airflow image"
	@echo "compose-config     Validate the resolved Docker Compose configuration"
	@echo "compose-init       Build, initialize, and start Airflow"
	@echo "compose-up         Start Airflow"
	@echo "compose-down       Stop Airflow without deleting volumes"
	@echo "compose-reset      Restart Airflow without deleting volumes"
	@echo "check-connections  Verify all pipeline connection IDs"
