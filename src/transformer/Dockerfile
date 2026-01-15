# Image de base légère (Python 3.9)
FROM python:3.9-slim

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installation des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY *.py .

CMD ["python", "transformation.py"]