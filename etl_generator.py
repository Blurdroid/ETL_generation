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

# ============================================================
# ETL GENERATOR
# ============================================================
class ETLGenerator:
    def __init__(self):
        self.current_etl = None
        self.conversation = []

    def _auto_fix_etl(self, etl):
        # [Identical healing logic for Source Qualifier & Target as provided previously]
        if not etl: return etl
        comps = etl.get("components", [])
        conns = etl.get("connections", [])
        src = etl.get("source", {})
        tgt = etl.get("target", {})
        src_name = src.get("name")
        tgt_id = tgt.get("id", tgt.get("name"))
        
        if not src_name or not tgt_id: return etl

        has_sq = any(c.get("type", "").upper() == "SOURCE_QUALIFIER" for c in comps)
        if not has_sq and src.get("fields"):
            sq_id = f"sq_{src_name.lower()}"
            sq_comp = {"id": sq_id, "name": f"SQ_{src_name}", "type": "SOURCE_QUALIFIER", "description": "Auto-injected Source Qualifier", "fields": [{"name": f.get("name", ""), "datatype": f.get("datatype", ""), "expression": ""} for f in src.get("fields", [])], "condition": ""}
            comps.insert(0, sq_comp)
            for conn in conns:
                if conn.get("from") in (src_name, src.get("id")): conn["from"] = sq_id
            conns.insert(0, {"from": src_name, "to": sq_id, "label": ""})
            
        has_tgt_conn = any(c.get("to") == tgt_id for c in conns)
        if not has_tgt_conn and comps:
            last_ref = comps[-1].get("id", comps[-1].get("name"))
            conns.append({"from": last_ref, "to": tgt_id, "label": ""})
            
        etl["components"] = comps
        etl["connections"] = conns
        return etl

    def ask_qwen(self, messages, retries=2):
        payload = {"model": MODEL, "messages": messages, "stream": False, "format": "json", "think": False, "options": {"temperature": 0.1, "num_predict": 3000, "num_ctx": 8192}}
    
        for attempt in range(retries):
            response = requests.post(OLLAMA_URL, json=payload, timeout=(10, 300))
            response.raise_for_status()
            content = response.json().get("message", {}).get("content", "")

            try:
                text = content.strip()
                if text.startswith("```"): text = text.strip("`").replace("json\n", "", 1)
                validated_data = ETLPipeline(**json.loads(text.strip()))
                return self._auto_fix_etl(validated_data.model_dump(by_alias=True))
            except (json.JSONDecodeError, ValidationError) as e:
                if attempt < retries - 1:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({"role": "user", "content": f"Validation Error:\n{e}\nOutput ONLY the corrected JSON."})
                else: raise RuntimeError(f"JSON failure: {e}")

    def generate_from_requirement(self, requirement, database_type="Teradata", domain="General"):
        system_prompt = f"""
You are an Expert Enterprise Data Architect and Informatica PowerCenter Consultant.
Your task is to design a highly mature, logical ETL pipeline based on business requirements.

Context constraints:
1. Target Database: {database_type}. Use specific functions (e.g., COALESCE, MD5).
2. Business Domain: {domain}. Use standardized naming conventions for this sector (e.g., if T24, expect core banking tables; if CARD400, expect credit card transactions; if OFSAA, expect risk/compliance metrics).
3. Architecture Layer Awareness: Adapt transformations if the target is Data Vault (generate Hash Keys, Load Dates), Business Vault, or iMART (Dimensional modeling).

Output ONLY strict JSON matching this schema:
{{
    "needs_more_info": false,
    "clarifying_question": "",
    "pipeline_name": "ETL_T24_ACCOUNT_HUB",
    "description": "Loads T24 Accounts to Data Vault Hub",
    "source": {{"name": "STG_T24_ACCOUNT", "type": "SOURCE", "fields": [{{"name": "ACCT_ID", "datatype": "integer", "key": true}}]}},
    "components": [
        {{"id": "sq_1", "name": "SQ_STG_T24_ACCOUNT", "type": "SOURCE_QUALIFIER", "fields": [{{"name": "ACCT_ID", "datatype": "integer", "expression": ""}}]}},
        {{"id": "exp_hash", "name": "EXP_GENERATE_KEYS", "type": "EXPRESSION", "fields": [{{"name": "ACCT_ID", "datatype": "integer", "expression": ""}}, {{"name": "HK_ACCT_ID", "datatype": "string", "expression": "MD5(TO_CHAR(ACCT_ID))"}}]}}
    ],
    "connections": [
        {{"from": "STG_T24_ACCOUNT", "to": "sq_1"}}, {{"from": "sq_1", "to": "exp_hash"}}, {{"from": "exp_hash", "to": "HUB_ACCOUNT"}}
    ],
    "target": {{"name": "HUB_ACCOUNT", "type": "TARGET", "fields": [{{"name": "HK_ACCT_ID", "datatype": "string", "key": true}}, {{"name": "ACCT_ID", "datatype": "integer", "key": false}}]}}
}}

RULES:
1. SOURCE must connect to SOURCE_QUALIFIER first.
2. If the prompt lacks DDL or source fields, set "needs_more_info": true and ask for DDL.
"""
        messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": requirement}]
        result = self.ask_qwen(messages)
        self.current_etl = result
        self.conversation = [{"role": "user", "content": requirement}, {"role": "assistant", "content": json.dumps(result)}]
        return result

    def modify_etl(self, instruction, database_type="Teradata", domain="General"):
        if not self.current_etl: return self.generate_from_requirement(instruction, database_type, domain)
        system_prompt = f"You are an Enterprise Data Architect modifying an existing {domain} ETL design for {database_type}. Maintain JSON schema strictly. Ensure logic matches {domain} standards."
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Current ETL:\n" + json.dumps(self.current_etl)},
            {"role": "user", "content": "Instruction:\n" + instruction}
        ]
        result = self.ask_qwen(messages)
        self.current_etl = result
        self.conversation.extend([{"role": "user", "content": instruction}, {"role": "assistant", "content": json.dumps(result)}])
        return result

    def reset(self):
        self.current_etl = None
        self.conversation = []
