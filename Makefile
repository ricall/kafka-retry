SHELL=bash

.PHONY: clean start-dev stop-dev start stop db-terminal run build image

clean: stop
	@docker container ls -q --filter status=exited --filter status=created | xargs -I {} docker rm -v {} || true

start-dev:
	@docker-compose -f docker-compose.yml up database zookeeper broker schema-registry control-center

stop-dev:
	@docker-compose -f docker-compose.yml down

broker-terminal:
	@docker-compose -f docker-compose.yml exec broker bash

db-terminal:
	@docker-compose -f docker-compose.yml exec database bash

run:
	@./gradlew bootRun

build:
	@./gradlew build

image: build
	@docker build -t kafka-performance .