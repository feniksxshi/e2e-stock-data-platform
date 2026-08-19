# Makefile to manage dataplatfom docker images

# VARIABLES
# -----------------------------------------------------------------------------
# Use docker.exe from WSL to connect to Docker Desktop
# Use .ONESHELL to handle paths with spaces properly
.ONESHELL:
SHELL := /bin/bash
DOCKER := docker.exe
SPARK_APP_NAME=create_schema.py

# Create docker network if not exists
# -----------------------------------------------------------------------------
.PHONY: create-network
create-network:
	@$(DOCKER) network inspect lakehouse-network >/dev/null 2>&1 || $(DOCKER) network create --driver bridge lakehouse-network >/dev/null 2>&1 || true
	@echo "Network lakehouse-network ready"

# compose up with all services with 1 spark worker by default
# -----------------------------------------------------------------------------
.PHONY: compose-up-all
compose-up-all: create-network
	$(DOCKER) compose --profile spark --env-file .env up -d --build
	@$(DOCKER) ps 

# Incrementally process all API landing datasets into Iceberg bronze.
# -----------------------------------------------------------------------------
.PHONY: load-bronze
load-bronze:
	$(DOCKER) compose --profile spark --env-file .env exec spark-master \
		/opt/spark/bin/spark-submit --master 'local[2]' \
		/opt/spark-batch/jobs/bronze.py --dataset all

# compose up with all services with 2 spark worker
# -----------------------------------------------------------------------------
.PHONY: compose-up-spark2
compose-up-spark2: create-network
	@$(DOCKER) compose --profile spark up -d --scale spark-worker=2

# compose up with all services with 3 spark worker
# -----------------------------------------------------------------------------
.PHONY: compose-up-spark3
compose-up-spark3: create-network
	@$(DOCKER) compose --profile spark up -d --scale spark-worker=3

# restart (keeps volumes and network)
# -----------------------------------------------------------------------------
.PHONY: compose-restart
compose-restart: 
	$(DOCKER) compose --profile spark restart

# compose down
# -----------------------------------------------------------------------------
.PHONY: compose-down
compose-down:
	@$(DOCKER) compose --env-file .env --profile spark down

# compose down clean (remove volumes and images)
# -----------------------------------------------------------------------------
.PHONY: compose-down-clean
compose-down-clean:
	@$(DOCKER) compose --env-file .env --profile spark down --volumes --rmi all --remove-orphans

# down then up
# -----------------------------------------------------------------------------
.PHONY: compose-down-up
compose-full-restart: compose-down compose-up-all
