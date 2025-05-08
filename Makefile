up: docker-up
down: docker-down
downv: docker-down-clear
stop: docker-stop
build: docker-build
rest: down up
restb: down build up
bash: docker-bash

docker-up:
	docker compose up -d

docker-down:
	docker compose stop

docker-down-clear:
	docker compose down -v --remove-orphans

docker-stop:
	docker compose down --remove-orphans

docker-build:
	docker compose build --pull
