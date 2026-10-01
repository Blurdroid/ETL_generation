import json
import requests
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional

# ============================================================
# CONFIGURATION & SCHEMAS
# ============================================================
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL = "qwen3:4b"

class ETLField(BaseModel):
    name: str
    datatype: str
    key: Optional[bool] = False
    expression: Optional[str] = ""

class ETLComponent(BaseModel):
    id: str
    name: str
    type: str
    description: Optional[str] = ""
    fields: List[ETLField]
    condition: Optional[str] = ""

class ETLConnection(BaseModel):
    from_id: str = Field(alias="from")
    to: str
    label: Optional[str] = ""

class ETLSourceTarget(BaseModel):
    # "id" must be part of the schema: without it model_dump() silently drops
    # the id the healer assigns, and _auto_fix_etl (which keys off it) bails
    # out immediately -- skipping SQ injection and target wiring entirely.
    id: Optional[str] = ""
    name: str
    type: str
    fields: List[ETLField]

class ETLPipeline(BaseModel):
    needs_more_info: bool = False
    clarifying_question: Optional[str] = ""
    pipeline_name: str
    description: str
    source: ETLSourceTarget
    components: List[ETLComponent]
    connections: List[ETLConnection]
    target: ETLSourceTarget

class ETLGenerator:
    def __init__(self):
        self.current_etl = None
        self.conversation = []

    def _pre_validate_heal(self, raw_json):
        """
        Bulldozes malformed JSON. Fixes empty IDs which cause the disconnects,
        renames bad keys, and guarantees every Pydantic-required top-level
        field exists before validation — a model response that's missing
        'pipeline_name'/'description' (or entire 'source'/'target'/
        'components'/'connections' keys) used to hard-crash with a raw
        Pydantic ValidationError; now it's backfilled with sensible defaults.
        """
        if not isinstance(raw_json, dict): return raw_json

        # 0. Backfill top-level required fields BEFORE anything else touches
        # them, so every subsequent step can assume they exist.
        stray_error = raw_json.pop("error", None)

        if not isinstance(raw_json.get("source"), dict):
            raw_json["source"] = {"name": "SOURCE", "type": "SOURCE", "fields": []}
        if not isinstance(raw_json.get("target"), dict):
            raw_json["target"] = {"name": "TARGET", "type": "TARGET", "fields": []}
        if not isinstance(raw_json.get("components"), list):
            raw_json["components"] = []
        if not isinstance(raw_json.get("connections"), list):
            raw_json["connections"] = []
        if "needs_more_info" not in raw_json:
            raw_json["needs_more_info"] = False

        if not str(raw_json.get("description") or "").strip():
            # A stray "error"/refusal string from the model is genuinely
            # useful context -- surface it as the description instead of
            # silently dropping it.
            raw_json["description"] = stray_error or "Auto-generated ETL pipeline"

        if not str(raw_json.get("pipeline_name") or "").strip():
            src_n = str(raw_json["source"].get("name") or "SRC")
            tgt_n = str(raw_json["target"].get("name") or "TGT")
            safe = lambda s: "".join(ch if ch.isalnum() else "_" for ch in s).strip("_") or "PIPELINE"
            raw_json["pipeline_name"] = f"ETL_{safe(src_n)}_TO_{safe(tgt_n)}".upper()

        if "clarifying_question" not in raw_json:
            raw_json["clarifying_question"] = ""

        def fix_fields(fields_list):
            if not isinstance(fields_list, list): return []
            fixed = []
            for f in fields_list:
                if isinstance(f, dict):
                    if "type" in f and "datatype" not in f: f["datatype"] = str(f.pop("type"))
                    if "datatype" not in f: f["datatype"] = "string"
                    if "name" not in f: f["name"] = "UNKNOWN_FIELD"
                    fixed.append(f)
            return fixed

        # 1. Heal Source and Target & FIX EMPTY ID BUG
        has_fields = False
        for section in ["source", "target"]:
            obj = raw_json.get(section)
            if isinstance(obj, dict):
                obj["fields"] = fix_fields(obj.get("fields", []))
                if obj["fields"]: has_fields = True
                
                # If name or ID is empty string "", forcefully replace it
                if not obj.get("name") or not str(obj.get("name")).strip(): obj["name"] = f"Unnamed_{section.upper()}"
                if not obj.get("id") or not str(obj.get("id")).strip(): obj["id"] = obj["name"]
                if "type" not in obj: obj["type"] = section.upper()

        # Break the pop-up loop if fields actually exist
        if has_fields:
            raw_json["needs_more_info"] = False

        # 2. Heal Components
        comps = raw_json.get("components")
        if isinstance(comps, list):
            for i, comp in enumerate(comps):
                if isinstance(comp, dict):
                    if not comp.get("id") or not str(comp.get("id")).strip(): comp["id"] = f"auto_comp_{i}"
                    if not comp.get("name") or not str(comp.get("name")).strip(): comp["name"] = f"Transformation_{i}"
                    if "type" not in comp: comp["type"] = "EXPRESSION"
                    
                    if "fields" not in comp:
                        comp["fields"] = []
                        if "output" in comp: comp["fields"].append({"name": str(comp.pop("output")), "datatype": "string", "expression": ""})
                    comp["fields"] = fix_fields(comp.get("fields", []))

        # 3. Heal Connections
        conns = raw_json.get("connections")
        if isinstance(conns, list):
            for c in conns:
                if isinstance(c, dict):
                    if "from" not in c and "from_id" in c: c["from"] = c.pop("from_id")
                    if "to" not in c: c["to"] = "UNKNOWN_TARGET"

        return raw_json

    def _auto_fix_etl(self, etl):
        if not etl: return etl
        comps = etl.get("components", [])
        conns = etl.get("connections", [])
        src = etl.get("source", {})
        tgt = etl.get("target", {})
        
        src_id = src.get("id") or src.get("name")
        tgt_id = tgt.get("id") or tgt.get("name")
        
        if not src_id or not tgt_id: return etl
        src["id"], tgt["id"] = src_id, tgt_id

        # 1. SQ Injection
        has_sq = any(c.get("type", "").upper() == "SOURCE_QUALIFIER" for c in comps)
        if not has_sq and src.get("fields"):
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
            
        # 2. Graph Continuity Healer
        for i, comp in enumerate(comps):
            comp_id = comp.get("id")
            has_in = any(c.get("to") == comp_id for c in conns)
            if not has_in:
                prev_id = comps[i-1].get("id") if i > 0 else src_id
                conns.append({"from": prev_id, "to": comp_id, "label": "auto-healed"})
                
        # 3. Target Wiring (Strict Fallback to connect SQ/Transform to Target)
        target_aliases = [tgt_id, tgt.get("name"), "TARGET", "target", "TGT", "UNKNOWN_TARGET"]
        has_tgt_conn = False
        for c in conns:
            if c.get("to") in target_aliases:
                c["to"] = tgt_id
                has_tgt_conn = True
                
        if not has_tgt_conn and comps:
            last_ref = comps[-1].get("id")
            conns.append({"from": last_ref, "to": tgt_id, "label": "auto-healed"})
            
        etl["components"] = comps
        etl["connections"] = conns
        return etl

    def ask_qwen(self, messages, retries=2):
        payload = {"model": MODEL, "messages": messages, "stream": False, "format": "json", "think": False, "options": {"temperature": 0.1, "num_predict": 3000, "num_ctx": 4096}}
    
        for attempt in range(retries):
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
                
                src_fields = clean_dict.get("source", {}).get("fields", [])
                tgt_fields = clean_dict.get("target", {}).get("fields", [])
                
                # Only trigger popup if no fields exist AND needs_more_info is true
                if not clean_dict.get("needs_more_info"):
                    if not src_fields or (len(src_fields) == 1 and src_fields[0].get("name").upper() in ["ID", "DUMMY"]):
                        clean_dict["needs_more_info"] = True
                        clean_dict["clarifying_question"] = "Please provide your specific Source fields or DDL to generate an accurate mapping."
                    elif not tgt_fields:
                        # The model gave up on deriving the target (e.g. returned an
                        # "error" instead of a design). An empty target would export
                        # a useless mapping, so ask for the columns explicitly.
                        clean_dict["needs_more_info"] = True
                        clean_dict["clarifying_question"] = "I couldn't determine the target columns from your requirement. Please provide the Target fields or DDL."

                return self._auto_fix_etl(clean_dict)
                
            except (json.JSONDecodeError, ValidationError) as e:
                if attempt < retries - 1:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({"role": "user", "content": f"Validation Error:\n{e}\n\nCRITICAL FIX REQUIRED: You MUST use 'datatype' (NEVER 'type') for columns. Every component MUST have an 'id' string and a 'fields' array. Output ONLY the corrected JSON."})
                else: raise RuntimeError(f"JSON failure: {e}")

    def generate_from_requirement(self, requirement, database_type="Teradata", domain="General"):
        system_prompt = f"""
You are an Expert Enterprise Data Architect and Informatica PowerCenter Consultant.
Your task is to design a logical ETL pipeline based on business requirements. Target Database: {database_type}. Domain: {domain}.

CRITICAL RULE FOR MISSING DDL:
If the prompt DOES NOT contain column names, fields, or a DDL statement, DO NOT guess dummy columns. 
Stop immediately and return exactly:
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

    def modify_etl(self, instruction, database_type="Teradata", domain="General"):
        if not self.current_etl: return self.generate_from_requirement(instruction, database_type, domain)
        system_prompt = f"""
You are an Enterprise Data Architect modifying an existing ETL design. Target: {database_type}.

CRITICAL JSON SCHEMA RULES YOU MUST FOLLOW:
1. Field data types MUST use the key "datatype" (e.g., {{"name": "CUST_ID", "datatype": "integer"}}). YOU ARE FORBIDDEN FROM USING THE KEY "type" INSIDE FIELDS.
2. Every component MUST have a unique "id" string.
3. Every component MUST have a "fields" array showing the columns passing through it.
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
