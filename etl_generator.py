import json
import requests
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional

# ============================================================
# CONFIGURATION
# ============================================================
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL = "qwen3:4b"

# ============================================================
# PYDANTIC SCHEMAS FOR VALIDATION
# ============================================================
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

    def ask_qwen(self, messages, retries=2):
        payload = {
            "model": MODEL,
            "messages": messages,
            "stream": False,
            "format": "json",
            "think": False,
            "options": {
                "temperature": 0,
                "num_predict": 2500,
                "num_ctx": 4096
            },
            "keep_alive": "10m"
        }
    
        for attempt in range(retries):
            response = requests.post(OLLAMA_URL, json=payload, timeout=(10, 300))
            response.raise_for_status()
            data = response.json()
            content = data.get("message", {}).get("content", "")

            if not content:
                raise RuntimeError("Qwen returned no final content.")

            try:
                text = content.strip()
                if text.startswith("```"):
                    text = text.strip("`")
                    if text.lower().startswith("json"):
                        text = text[4:]
                json_data = json.loads(text.strip())

                validated_data = ETLPipeline(**json_data)
                return validated_data.model_dump(by_alias=True)
                
            except (json.JSONDecodeError, ValidationError) as e:
                print(f"\n[Attempt {attempt + 1}] Validation Failed: {e}")
                if attempt < retries - 1:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({
                        "role": "user", 
                        "content": f"Your JSON output failed validation with this error:\n{e}\n\nPlease output ONLY the corrected JSON."
                    })
                else:
                    raise RuntimeError(f"Qwen failed to produce valid JSON after {retries} attempts. Error: {e}")

    def generate_from_requirement(self, requirement, database_type="Generic"):
        system_prompt = f"""
You are an ETL Architecture AI Agent.
Your task is to design a logical ETL pipeline from a natural-language requirement.

IMPORTANT: Target Database is {database_type}. Use vendor-specific functions in expressions if necessary (e.g., NVL for Oracle, ISNULL for SQL Server).

============================================================
SUPPORTED COMPONENTS
============================================================
SOURCE, SOURCE_QUALIFIER, EXPRESSION, FILTER, JOINER, LOOKUP, SORTER, AGGREGATOR, ROUTER, UPDATE_STRATEGY, SEQUENCE_GENERATOR, DATA_VAULT, BUSINESS_VAULT, TARGET

============================================================
OUTPUT FORMAT (MUST MATCH EXACTLY)
============================================================
{{
    "needs_more_info": false,
    "clarifying_question": "",
    "pipeline_name": "ETL_CUSTOMERS",
    "description": "Loads customer data",
    "source": {{
        "name": "SRC_CUSTOMERS",
        "type": "SOURCE",
        "fields": [{{"name": "ID", "datatype": "integer", "key": true}}]
    }},
    "components": [
        {{
            "id": "sq_1",
            "name": "SQ_SRC_CUSTOMERS",
            "type": "SOURCE_QUALIFIER",
            "description": "Extracts from source",
            "fields": [{{"name": "ID", "datatype": "integer", "expression": ""}}],
            "condition": ""
        }},
        {{
            "id": "exp_1",
            "name": "EXP_Transform",
            "type": "EXPRESSION",
            "description": "Cleanse names",
            "fields": [{{"name": "ID", "datatype": "integer", "expression": ""}}],
            "condition": ""
        }}
    ],
    "connections": [
        {{"from": "SRC_CUSTOMERS", "to": "sq_1", "label": ""}},
        {{"from": "sq_1", "to": "exp_1", "label": ""}}
    ],
    "target": {{
        "name": "TGT_CUSTOMERS",
        "type": "TARGET",
        "fields": [{{"name": "ID", "datatype": "integer", "key": true}}]
    }}
}}

============================================================
RULES
============================================================
1. CRITICAL: A SOURCE cannot connect directly to an EXPRESSION or TARGET. You MUST create a SOURCE_QUALIFIER immediately after the SOURCE.
2. Every component must have a unique ID.
3. Every component's "fields" list must show EVERY field that exists at that point. Passthrough fields use "expression": "".
4. CRITICAL: If the user request lacks source fields, DDL, or specific column names, you MUST set "needs_more_info" to true and ask for the DDL/fields in "clarifying_question".
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": requirement}
        ]

        result = self.ask_qwen(messages)
        self.current_etl = result
        self.conversation = [{"role": "user", "content": requirement}, {"role": "assistant", "content": json.dumps(result)}]
        return result

    def modify_etl(self, instruction, database_type="Generic"):
        if not self.current_etl:
            return self.generate_from_requirement(instruction, database_type)

        system_prompt = f"""
You are an ETL Architecture AI Agent. You are modifying an existing ETL design.
Target Database is {database_type}.

RULES:
1. Preserve existing components unless asked to remove them.
2. Maintain strict JSON schema. 
3. Passthrough fields must have "expression": "".
4. If missing DDL for new sources/targets, set "needs_more_info": true.
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Here is the current ETL design:\n\n" + json.dumps(self.current_etl, indent=2)},
            {"role": "user", "content": "Modify the ETL according to this instruction:\n\n" + instruction}
        ]

        result = self.ask_qwen(messages)
        self.current_etl = result
        self.conversation.append({"role": "user", "content": instruction})
        self.conversation.append({"role": "assistant", "content": json.dumps(result)})
        return result

    def reset(self):
        self.current_etl = None
        self.conversation = []