.PHONY: d-homework-i-run
d-homework-i-run:
	@COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 \
		docker-compose up --build


.PHONY: d-homework-i-purge
d-homework-i-purge:
	@COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 \
		docker-compose down --volumes --remove-orphans --rmi local --timeout 0


.PHONY: homework-i-run
homework-i-run:
	make init-dev && \
	python main.py

.PHONY: init-dev
init-dev:
	pip install --upgrade pip && \
	pip install --requirement requirements.txt && \
	pre-commit install