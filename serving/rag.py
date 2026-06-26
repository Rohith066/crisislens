"""
CrisisLens — Phase 3, geo-aware RAG answer generation (local LLM via Ollama).

answer() = retrieve nearby + relevant hazards (serving/retrieval.py) -> build a
grounded prompt -> ask a local Ollama model -> return a CITED answer.

Guardrails (this is a life-safety app):
  - the model may use ONLY the hazards we retrieved (grounding, no world knowledge)
  - it cites hazards by number [1], [2]; we return id/source/url for each
  - if no hazards are nearby, we skip the LLM and return a safe, honest message
  - every answer defers to official local authorities
No API keys, no cost — Ollama runs locally.
"""

import requests
from serving.retrieval import retrieve

OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "llama3.2:3b"

SYSTEM_PROMPT = (
    "You are CrisisLens, a disaster-information assistant. "
    "Answer the user's question using ONLY the numbered hazards provided. "
    "Cite the hazards you rely on by their number in brackets, e.g. [1]. "
    "Do NOT invent hazards or details that are not in the list. "
    "Be concise (2-4 sentences). "
    "Always end by reminding the user to follow official local authorities and emergency services."
)


def _format_hazards(hazards):
    blocks = []
    for i, h in enumerate(hazards, 1):
        desc = (h.get("description") or "")[:400]
        blocks.append(
            f"[{i}] {h['hazard_type']} (severity: {h['severity_level']}) — "
            f"{h['place']}, {h['distance_km']} km away\n{desc}"
        )
    return "\n\n".join(blocks)


def answer(question, lat, lon, radius_km=300, k=5):
    hazards = retrieve(question, lat, lon, radius_km, k)

    # Guardrail: nothing nearby -> don't call the LLM, don't risk a hallucination.
    if not hazards:
        return {
            "answer": (
                f"I don't see any active hazards within {radius_km} km of your location "
                "in the current data. This isn't a guarantee of safety — always follow "
                "official local authorities and emergency services."
            ),
            "citations": [],
            "used_llm": False,
        }

    user_msg = (
        f"Question: {question}\n\n"
        f"Active hazards near this location:\n{_format_hazards(hazards)}\n\nAnswer:"
    )

    resp = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            "stream": False,
            "options": {"temperature": 0.2},  # low temp = factual, grounded
        },
        timeout=120,
    )
    resp.raise_for_status()
    text = resp.json()["message"]["content"].strip()

    citations = [
        {"n": i, "event_id": h["event_id"], "source": h["source"],
         "hazard_type": h["hazard_type"], "place": h["place"],
         "distance_km": h["distance_km"], "url": h["url"]}
        for i, h in enumerate(hazards, 1)
    ]
    return {"answer": text, "citations": citations, "used_llm": True}
