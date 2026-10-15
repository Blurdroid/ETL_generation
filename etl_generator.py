import json
import requests
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Dict, Any

# ============================================================
# CONFIGURATION & SCHEMAS
# ============================================================

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL = "qwen3:4b"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

import os
_provider = "ollama"
_gemini_key = os.environ.get("GEMINI_API_KEY", "")

def set_provider(provider: str, gemini_key: str = ""):
    global _provider, _gemini_key
    _provider = provider.lower()
    _gemini_key = gemini_key

class ETLField(BaseModel):
    name: str
    datatype: str
    key: Optional[bool] = False
    expression: Optional[str] = ""
    precision: Optional[int] = None
    scale: Optional[int] = None

class ETLComponent(BaseModel):
    id: str
    name: str
    type: str
    description: Optional[str] = ""
    fields: List[ETLField] = []
    condition: Optional[str] = ""
    join_condition: Optional[str] = ""
    join_type: Optional[str] = "Normal Join"
    master_source: Optional[str] = ""
    master_upstream: Optional[str] = ""
    groups: Optional[List[Dict[str, Any]]] = None
    branches: Optional[List[str]] = None
    sort_keys: Optional[List[str]] = None

class ETLConnection(BaseModel):
    from_id: str = Field(alias="from")
    to: str
    label: Optional[str] = ""

class ETLSourceTarget(BaseModel):
    id: Optional[str] = ""
    name: str
    type: str = "SOURCE"
    owner: Optional[str] = "dbo"
    dbdname: Optional[str] = ""
    fields: List[ETLField] = []

class ETLPipeline(BaseModel):
    needs_more_info: bool = False
    clarifying_question: Optional[str] = ""
    pipeline_name: str
    description: str
    source: Optional[ETLSourceTarget] = None
    target: Optional[ETLSourceTarget] = None
    sources: Optional[List[ETLSourceTarget]] = None
    targets: Optional[List[ETLSourceTarget]] = None
    components: List[ETLComponent] = []
    connections: List[ETLConnection] = []

class ETLGenerator:
    def __init__(self):
        self.current_etl = None
        self.conversation = []

    def _pre_validate_heal(self, raw_json):
        """
        Bulldozes malformed JSON. Fixes empty IDs, renames bad keys,
        and guarantees every Pydantic-required top-level field exists.
        Supports both single source/target and enterprise multi-source topologies.
        """
        if not isinstance(raw_json, dict): return raw_json

        raw_json.pop("error", None)

        def fix_fields(fields_list):
            if not isinstance(fields_list, list): return []
            fixed = []
            for f in fields_list:
                if isinstance(f, dict):
                    if "type" in f and "datatype" not in f: f["datatype"] = str(f.pop("type"))
                    if "datatype" not in f: f["datatype"] = "string"
                    if "name" not in f: f["name"] = "UNKNOWN_FIELD"
                    fixed.append(f)
                elif isinstance(f, str):
                    fixed.append({"name": f, "datatype": "string", "key": False, "expression": ""})
            return fixed

        # 1. Multi-source / Single-source healing
        if "sources" in raw_json and isinstance(raw_json["sources"], list) and raw_json["sources"]:
            for i, s in enumerate(raw_json["sources"]):
                if isinstance(s, dict):
                    if not s.get("name"): s["name"] = f"SRC_{i+1}"
                    if not s.get("id"): s["id"] = s["name"]
                    s["type"] = "SOURCE"
                    s["fields"] = fix_fields(s.get("fields", []))
            if not raw_json.get("source"):
                raw_json["source"] = raw_json["sources"][0]
        else:
            if not isinstance(raw_json.get("source"), dict):
                raw_json["source"] = {"name": "SRC", "id": "SRC", "type": "SOURCE", "fields": []}
            else:
                s = raw_json["source"]
                if not s.get("name"): s["name"] = "SRC"
                if not s.get("id"): s["id"] = s["name"]
                s["type"] = "SOURCE"
                s["fields"] = fix_fields(s.get("fields", []))

        # 2. Multi-target / Single-target healing
        if "targets" in raw_json and isinstance(raw_json["targets"], list) and raw_json["targets"]:
            for i, t in enumerate(raw_json["targets"]):
                if isinstance(t, dict):
                    if not t.get("name"): t["name"] = f"TGT_{i+1}"
                    if not t.get("id"): t["id"] = t["name"]
                    t["type"] = "TARGET"
                    t["fields"] = fix_fields(t.get("fields", []))
            if not raw_json.get("target"):
                raw_json["target"] = raw_json["targets"][0]
        else:
            if not isinstance(raw_json.get("target"), dict):
                raw_json["target"] = {"name": "TGT", "id": "TGT", "type": "TARGET", "fields": []}
            else:
                t = raw_json["target"]
                if not t.get("name"): t["name"] = "TGT"
                if not t.get("id"): t["id"] = t["name"]
                t["type"] = "TARGET"
                t["fields"] = fix_fields(t.get("fields", []))

        if not isinstance(raw_json.get("components"), list):
            raw_json["components"] = []
        if not isinstance(raw_json.get("connections"), list):
            raw_json["connections"] = []
        if "needs_more_info" not in raw_json:
            raw_json["needs_more_info"] = False

        if not str(raw_json.get("description") or "").strip():
            raw_json["description"] = "Auto-generated enterprise ETL pipeline"

        if not str(raw_json.get("pipeline_name") or "").strip():
            src_n = str(raw_json["source"].get("name") or "SRC")
            tgt_n = str(raw_json["target"].get("name") or "TGT")
            safe = lambda s: "".join(ch if ch.isalnum() else "_" for ch in s).strip("_") or "PIPELINE"
            raw_json["pipeline_name"] = f"ETL_{safe(src_n)}_TO_{safe(tgt_n)}".upper()

        if "clarifying_question" not in raw_json:
            raw_json["clarifying_question"] = ""

        # Heal Components
        for i, comp in enumerate(raw_json["components"]):
            if isinstance(comp, dict):
                if not comp.get("id") or not str(comp.get("id")).strip(): comp["id"] = f"auto_comp_{i}"
                if not comp.get("name") or not str(comp.get("name")).strip(): comp["name"] = f"Transformation_{i}"
                if "type" not in comp: comp["type"] = "EXPRESSION"
                comp["fields"] = fix_fields(comp.get("fields", []))

        # Heal Connections
        for c in raw_json["connections"]:
            if isinstance(c, dict):
                if "from" not in c and "from_id" in c: c["from"] = c.pop("from_id")
                if "to" not in c: c["to"] = raw_json["target"]["id"]

        # Check if fields exist
        all_src_fields = raw_json["source"].get("fields", [])
        if raw_json.get("sources"):
            for s in raw_json["sources"]:
                all_src_fields.extend(s.get("fields", []))
        if all_src_fields:
            raw_json["needs_more_info"] = False

        return raw_json

    def _auto_fix_etl(self, etl):
        if not etl: return etl
        comps = etl.get("components", [])
        conns = etl.get("connections", [])

        sources = etl.get("sources") or ([etl["source"]] if etl.get("source") else [])
        targets = etl.get("targets") or ([etl["target"]] if etl.get("target") else [])

        src_ids = [s.get("id") or s.get("name") for s in sources]
        tgt_ids = [t.get("id") or t.get("name") for t in targets]

        is_multi_source = len(sources) > 1 or any(str(c.get("type", "")).upper() in ("JOINER", "UNION", "ROUTER") for c in comps)

        # 1. Source Qualifier Injection for single-source standard pipelines
        if not is_multi_source and sources and sources[0].get("fields"):
            src = sources[0]
            src_id = src_ids[0]
            has_sq = any(c.get("type", "").upper() in ("SOURCE_QUALIFIER", "SOURCE QUALIFIER", "SQ") for c in comps)
            if not has_sq:
                sq_id = f"sq_{src_id.lower()}"
                sq_comp = {
                    "id": sq_id, "name": f"SQ_{src.get('name')}", "type": "SOURCE_QUALIFIER",
                    "description": "Auto-injected SQ",
                    "fields": [{"name": f.get("name", ""), "datatype": f.get("datatype", ""), "expression": ""} for f in src.get("fields", [])],
                    "condition": ""
                }
                comps.insert(0, sq_comp)
                for conn in conns:
                    if conn.get("from") in (src.get("name"), src_id): conn["from"] = sq_id
                conns.insert(0, {"from": src_id, "to": sq_id, "label": ""})

        # 2. Graph Continuity Healer: only connect completely orphaned components
        all_connected_targets = {c.get("to") for c in conns}
        all_connected_sources = {c.get("from") for c in conns}

        if not is_multi_source:
            for i, comp in enumerate(comps):
                comp_id = comp.get("id")
                if comp_id not in all_connected_targets:
                    prev_id = comps[i-1].get("id") if i > 0 else (src_ids[0] if src_ids else "SRC")
                    conns.append({"from": prev_id, "to": comp_id, "label": "auto-healed"})

        # 3. Target Wiring
        if tgt_ids:
            primary_tgt = tgt_ids[0]
            target_aliases = set(tgt_ids) | {"TARGET", "target", "TGT", "UNKNOWN_TARGET"}
            for c in conns:
                if c.get("to") in target_aliases and c.get("to") not in tgt_ids:
                    c["to"] = primary_tgt

            has_tgt_conn = any(c.get("to") in tgt_ids for c in conns)
            if not has_tgt_conn and comps:
                last_ref = comps[-1].get("id")
                conns.append({"from": last_ref, "to": primary_tgt, "label": "auto-healed"})

        etl["components"] = comps
        etl["connections"] = conns
        return etl

    def load_ofsaa_enterprise_preset(self, database_type="Oracle"):
        """Directly loads the full OFSAA AFU Enterprise Banking Pipeline."""
        from powercenter_xml_generator import generate_ofsaa_enterprise_pipeline
        xml_content, etl_dict = generate_ofsaa_enterprise_pipeline(database_type=database_type)
        etl_dict["pregenerated_xml"] = xml_content
        self.current_etl = etl_dict
        return etl_dict

    def _ask_gemini(self, messages, retries=2):
        import requests as _rq
        combined = "\n\n".join(f"[{m['role'].upper()}]\n{m['content']}" for m in messages)
        combined += "\n\nRespond ONLY with valid JSON for the ETL pipeline."
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": combined}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 4000,
                                 "responseMimeType": "application/json"},
        }
        url = f"{GEMINI_URL}?key={_gemini_key}"
        for attempt in range(retries):
            resp = _rq.post(url, json=payload, headers=headers, timeout=(10, 120))
            resp.raise_for_status()
            content = (resp.json()
                       .get("candidates", [{}])[0]
                       .get("content", {})
                       .get("parts", [{}])[0]
                       .get("text", ""))
            try:
                text = content.strip().lstrip("```json").rstrip("```").strip()
                raw = json.loads(text)
                healed = self._pre_validate_heal(raw)
                validated = ETLPipeline(**healed)
                clean = validated.model_dump(by_alias=True)
                return self._auto_fix_etl(clean)
            except (json.JSONDecodeError, ValidationError) as e:
                if attempt < retries - 1:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({"role": "user", "content": f"Fix: {e}. Output corrected JSON only."})
                else:
                    raise RuntimeError(f"Gemini JSON failure: {e}")

    def ask_qwen(self, messages, retries=2):
        if _provider == "gemini" and _gemini_key.strip():
            return self._ask_gemini(messages, retries)

        from powercenter_agent import resolve_model, get_installed_ollama_models
        active_model = resolve_model(MODEL)
        payload = {"model": active_model, "messages": messages, "stream": False, "format": "json",
                   "think": False, "options": {"temperature": 0.1, "num_predict": 4000, "num_ctx": 8192}}

        for attempt in range(retries):
            response = requests.post(OLLAMA_URL, json=payload, timeout=(10, 900))
            if response.status_code == 404:
                installed = get_installed_ollama_models()
                if installed and payload["model"] not in installed:
                    payload["model"] = installed[0]
                    response = requests.post(OLLAMA_URL, json=payload, timeout=(10, 900))
            response.raise_for_status()
            content = response.json().get("message", {}).get("content", "")

            try:
                text = content.strip()
                if text.startswith("```"): text = text.strip("`").replace("json\n", "", 1)

                raw_json = json.loads(text.strip())
                healed_json = self._pre_validate_heal(raw_json)

                validated_data = ETLPipeline(**healed_json)
                clean_dict = validated_data.model_dump(by_alias=True)

                return self._auto_fix_etl(clean_dict)

            except (json.JSONDecodeError, ValidationError) as e:
                if attempt < retries - 1:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({"role": "user", "content": f"Validation Error:\n{e}\n\nCRITICAL FIX REQUIRED: You MUST use 'datatype' (NEVER 'type') for columns. Every component MUST have an 'id' string and a 'fields' array. Output ONLY the corrected JSON."})
                else: raise RuntimeError(f"JSON failure: {e}")

    def generate_from_requirement(self, requirement, database_type="Oracle", domain="General"):
        # Check if the user specifically requested OFSAA AFU enterprise pipeline
        req_lower = requirement.lower()
        if "ofsaa" in req_lower or "afu" in req_lower or ("enterprise" in req_lower and "pipeline" in req_lower and "joiner" in req_lower):
            return self.load_ofsaa_enterprise_preset(database_type=database_type)

        system_prompt = f"""
You are an Expert Enterprise Data Architect and Informatica PowerCenter Consultant.
Your task is to design a logical ETL pipeline based on business requirements. Target Database: {database_type}. Domain: {domain}.

ENTERPRISE ARCHITECTURE SUPPORT:
- Multiple Sources: You may output "sources": [...] (array of source tables with fields).
- Transformations supported:
  * "Joiner" with "join_condition" (e.g. "SRC_ID = TRN_SRC_ID"), "join_type" ("Normal Join", "Master Outer Join"), "master_source"
  * "Router" with "groups": [{{"name": "ACTIVE", "expression": "STATUS='ACTIVE'"}}, {{"name": "INACTIVE", "expression": "STATUS='INACTIVE'"}}]
  * "Custom Transformation" / "Union" with "branches": ["BRANCH1", "BRANCH2"]
  * "Sorter" with "sort_keys": ["KEY_COL"]
  * "Sequence" generating surrogate keys (NEXTVAL, CURRVAL)
  * "Expression" for derivations and business rules

CRITICAL RULE FOR MISSING DDL:
If the prompt DOES NOT contain column names, fields, or a DDL statement, and is not a general enterprise request, return:
{{
    "needs_more_info": true,
    "clarifying_question": "Please provide your Source fields or DDL.",
    "pipeline_name": "", "description": "", "source": {{"name": "SRC", "type": "SOURCE", "fields": []}},
    "components": [], "connections": [], "target": {{"name": "TGT", "type": "TARGET", "fields": []}}
}}
"""
        messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": requirement}]
        result = self.ask_qwen(messages)
        if not result.get("needs_more_info"):
            self.current_etl = result
        self.conversation = [{"role": "user", "content": requirement}, {"role": "assistant", "content": json.dumps(result)}]
        return result

    def modify_etl(self, instruction, database_type="Oracle", domain="General"):
        if not self.current_etl: return self.generate_from_requirement(instruction, database_type, domain)
        system_prompt = f"""
You are an Enterprise Data Architect modifying an existing ETL design. Target: {database_type}.

CRITICAL JSON SCHEMA RULES:
1. Field data types MUST use the key "datatype" (e.g., {{"name": "CUST_ID", "datatype": "integer"}}). FORBIDDEN from using the key "type" inside fields.
2. Every component MUST have a unique "id" string and "fields" array.
3. For Joiners include "join_condition" and "join_type". For Routers include "groups". For Unions include "branches".
4. "needs_more_info" MUST BE FALSE.
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Current ETL:\n" + json.dumps(self.current_etl)},
            {"role": "user", "content": "Instruction:\n" + instruction}
        ]
        result = self.ask_qwen(messages)
        if not result.get("needs_more_info"):
            self.current_etl = result
        self.conversation.extend([{"role": "user", "content": instruction}, {"role": "assistant", "content": json.dumps(result)}])
        return result

    def reset(self):
        self.current_etl = None
        self.conversation = []
