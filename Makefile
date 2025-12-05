.PHONY: help install dev up down logs test clean

help:
	@echo "Commandes disponibles:"
	@echo "  make install    - Installer les dépendances Python"
	@echo "  make dev        - Démarrer en mode développement"
	@echo "  make up         - Démarrer tous les services Docker"
	@echo "  make down       - Arrêter tous les services Docker"
	@echo "  make logs       - Voir les logs des services"
	@echo "  make test       - Exécuter les tests"
	@echo "  make clean      - Nettoyer les fichiers temporaires"

install:
	pip install -r requirements.txt

dev:
	uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
