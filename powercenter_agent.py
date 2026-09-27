import json
import os
import re
import requests

# ============================================================
# CONFIGURATION
# ============================================================
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL = "qwen3:4b"

# Some Ollama builds don't fully honor "think": False for every model/route,
# and Qwen3 can still emit its reasoning inside "content" itself (sometimes
# missing the opening <think> tag but keeping the closing </think>). Strip
# it defensively regardless of which shape it comes back in.
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_TRAILING_THINK_RE = re.compile(r".*?</think>", re.DOTALL | re.IGNORECASE)

def strip_reasoning(content):
    """Removes any leaked chain-of-thought from a model response, returning
    only the final answer."""
    if not content:
        return content
    cleaned = _THINK_BLOCK_RE.sub("", content)
    if "</think>" in cleaned:
        cleaned = _TRAILING_THINK_RE.sub("", cleaned, count=1)
    return cleaned.strip()


class PowerCenterAgent:
    def __init__(self, metadata_file="powercenter_metadata.json"):
        self.metadata_file = metadata_file
        self.metadata = {"workflows": []}
        self.current_workflow = None
        self.load_metadata()

    def load_metadata(self):
        """Loads workflow metadata from the local JSON file if it exists."""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, "r", encoding="utf-8") as f:
                    self.metadata = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load metadata from {self.metadata_file}: {e}")
        else:
            self.metadata = {"workflows": []}

    def save_metadata(self):
        """Saves current workflow metadata back to the local JSON file."""
        try:
            with open(self.metadata_file, "w", encoding="utf-8") as f:
                json.dump(self.metadata, f, indent=4)
        except Exception as e:
            print(f"Error saving metadata: {e}")

    def get_workflow(self, name):
        """Retrieves a specific workflow by name from the metadata list."""
        for wf in self.metadata.get("workflows", []):
            if wf.get("name") == name:
                return wf
        return None

    def chat(self, prompt, retries=2):
        """
        Sends the user's prompt along with the current workflow context
        to the local Ollama LLM to generate an explanation.
        """
        context = ""
        if self.current_workflow:
            wf = self.get_workflow(self.current_workflow)
            if wf:
                context = (
                    f"### CURRENT WORKFLOW CONTEXT ###\n"
                    f"Name: {wf.get('name')}\n"
                    f"Tasks: {wf.get('tasks')}\n"
                    f"Sessions: {wf.get('sessions')}\n"
                    f"Dependencies/Links: {wf.get('links')}\n"
                    f"################################\n\n"
                )

        system_prompt = (
            "You are an expert Enterprise Data Architect and Informatica PowerCenter Consultant. "
            "Your job is to analyze the provided XML workflow context and answer the user's questions. "
            "Be precise, reference the specific tasks and sessions in the context, and relate them "
            "to standard enterprise data architecture patterns when applicable. "
            "Answer directly in plain text -- do not show your reasoning, only the final answer. "
            "Never output <think> tags or any chain-of-thought; output only the final answer text."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": context + prompt},
        ]

        payload = {
            "model": MODEL,
            "messages": messages,
            "stream": False,
            # Disabling thinking is the fix: without it, Qwen3 can spend
            # its whole num_predict budget on internal reasoning tokens
            # and never emit any actual "content" -- that's exactly what
            # "Qwen returned no response content" was.
            "think": False,
            "options": {
                "temperature": 0.2,  # low temperature for factual analysis
                "num_predict": 1500,
                "num_ctx": 4096,
            },
        }

        last_error = None

        for attempt in range(retries):
            try:
                response = requests.post(OLLAMA_URL, json=payload, timeout=(10, 300))
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                last_error = f"Could not reach Ollama at {OLLAMA_URL}: {e}"
                continue

            message = data.get("message", {})
            content = message.get("content", "")

            if content:
                cleaned = strip_reasoning(content)
                if cleaned:
                    return cleaned

            last_error = (
                "Qwen returned no response content "
                f"(done_reason: {data.get('done_reason')}, "
                f"thinking length: {len(message.get('thinking', ''))})."
            )

        raise RuntimeError(
            f"{last_error}\n\nTried {retries} time(s). If this keeps happening, "
            "the local Ollama server may be overloaded or the model may need a "
            "restart (`ollama stop qwen3:4b` then ask again)."
        )