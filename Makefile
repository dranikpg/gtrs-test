run:
	docker compose down --volume
	docker compose build
	docker compose up