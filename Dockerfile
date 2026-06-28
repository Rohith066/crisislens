# CrisisLens — serving API (lean image: Leaflet map + /hazards geo feed).
# The /ask RAG (torch/faiss/Ollama) is intentionally NOT in this image so it fits
# a free-tier instance; /ask degrades to a friendly message when its deps are absent.
FROM python:3.12-slim

WORKDIR /app

COPY requirements-serving.txt .
RUN pip install --no-cache-dir -r requirements-serving.txt

COPY serving/ serving/
COPY lakehouse/gold/ lakehouse/gold/

EXPOSE 8000
CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
