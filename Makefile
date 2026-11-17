.PHONY: up down logs test psql reset

up:
	docker compose up -d --build
	@echo "Dashboard: http://localhost:8501"

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f producer consumer

test:
	pip install -q -r requirements-dev.txt && pytest -q

psql:
	docker compose exec postgres psql -U pipeline -d analytics
