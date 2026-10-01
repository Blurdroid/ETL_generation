import html
import json
import re
import requests
import streamlit as st
import graphviz
import sqlparse
import xml.etree.ElementTree as ET
import subprocess
import tempfile
import os
import glob

from powercenter_agent import PowerCenterAgent
from etl_generator import ETLGenerator
from powercenter_xml_generator import generate_powercenter_xml
# compute_field_flow

# ============================================================
# CONFIGURATION & LOAD AGENTS
# ============================================================
OLLAMA_URL = "http://127.0.0.1:11434"
MODEL = "qwen3:4b"
METADATA_FILE = "powercenter_metadata.json"

st.set_page_config(page_title="AI PowerCenter Architect", page_icon="🤖", layout="wide", initial_sidebar_state="expanded")

@st.cache_resource
def load_workflow_agent():
    return PowerCenterAgent(METADATA_FILE)

@st.cache_resource
def load_etl_generator():
    return ETLGenerator()

workflow_agent = load_workflow_agent()
etl_generator = load_etl_generator()

# ============================================================
# SESSION STATE & CSS
# ============================================================
if "page" not in st.session_state: st.session_state.page = "Workflow Intelligence"
if "workflow_messages" not in st.session_state: st.session_state.workflow_messages = []
if "etl_messages" not in st.session_state: st.session_state.etl_messages = []
if "current_etl" not in st.session_state: st.session_state.current_etl = None
if "etl_generation_mode" not in st.session_state: st.session_state.etl_generation_mode = "Form Builder"
if "awaiting_ddl" not in st.session_state: st.session_state.awaiting_ddl = False

st.markdown("""
    <style>
    .main-title { font-size: 32px; font-weight: 700; margin-bottom: 2px; }
    .subtitle { color: #777; font-size: 15px; margin-bottom: 22px; }
    footer { visibility: hidden; }
    div[data-testid="stExpander"] { border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# DOMAIN WORKSPACE INITIALIZER
# ============================================================
DOMAINS = ["T24", "CARD400", "CR2", "OFSAA", "AS400"]

PC_TRANSFORMATIONS = [
    "Aggregator", "Application Source Qualifier", "Custom", "Expression",
    "External Procedure", "Filter", "HTTP", "Java", "Joiner", "Lookup",
    "MQ Source Qualifier", "Normalizer", "Rank", "Router", "Sequence Generator",
    "Sorter", "Source Qualifier", "SQL", "Stored Procedure", "Transaction Control",
    "Union", "Unstructured Data", "Update Strategy", "Web Services Consumer",
    "XML Generator", "XML Parser", "XML Source Qualifier",
]

DOMAIN_SOURCE_FIELDS = {
    "T24": [("ACCOUNT_ID", "integer", 15), ("CUSTOMER_ID", "integer", 15), ("BALANCE", "decimal", 18), ("CURRENCY", "string", 3), ("ACCOUNT_STATUS", "string", 10), ("OPEN_DATE", "date", 19)],
    "CARD400": [("CARD_NUMBER", "string", 19), ("TXN_ID", "integer", 15), ("TXN_AMOUNT", "decimal", 18), ("TXN_CURRENCY", "string", 3), ("MERCHANT_CODE", "string", 15), ("TXN_TIMESTAMP", "timestamp", 26)],
    "CR2": [("ATM_ID", "string", 10), ("CARD_NUMBER", "string", 19), ("TXN_TYPE", "string", 12), ("TXN_AMOUNT", "decimal", 18), ("TXN_STATUS", "string", 10), ("TXN_TIMESTAMP", "timestamp", 26)],
    "OFSAA": [("ENTITY_ID", "integer", 15), ("RISK_METRIC_CODE", "string", 20), ("METRIC_VALUE", "decimal", 18), ("REPORTING_DATE", "date", 19), ("BASEL_CATEGORY", "string", 15)],
    "AS400": [("REC_KEY", "string", 20), ("FIELD_1", "string", 30), ("FIELD_2", "decimal", 18), ("LAST_UPDATE", "date", 19)],
    "IVR": [("CALL_ID", "integer", 15), ("CUSTOMER_ID", "integer", 15), ("AGENT_ID", "integer", 15), ("CALL_DURATION_SEC", "integer", 10), ("DISPOSITION_CODE", "string", 15), ("CALL_TIMESTAMP", "timestamp", 26)],
}

# Maps the CIB architecture diagram's Source Systems nodes to their DOMAIN_SOURCE_FIELDS
# entry, for the interactive Visual Architecture Builder.
VIZ_SOURCE_MAP = {"t24": "T24", "card400": "CARD400", "cr2": "CR2", "ivr": "IVR"}

def _target_fields_for_layer(layer, domain, source_fields):
    """
    Derives a reasonable target table name + column structure for a chosen
    architecture layer, given a source field list (dicts with name/datatype).
    Used by the Visual Architecture Builder to auto-populate target structure
    once the user picks a source + layer, without needing the LLM at all.
    """
    key = source_fields[0]
    others = source_fields[1:]

    def f(name, dtype, key_flag=False):
        return {"name": name, "datatype": dtype, "key": key_flag, "expression": ""}

    if layer == "stg":
        return f"STG_{domain}_RAW", [f(x["name"], x["datatype"], x.get("key", False)) for x in source_fields]
    if layer == "dv":
        return f"HUB_{domain}", [
            f(f"HK_{key['name']}", "string", True), f(key["name"], key["datatype"]),
            f("LOAD_DTS", "timestamp"), f("REC_SRC", "string"),
        ]
    if layer == "bv":
        return f"BV_{domain}_COMPUTED", (
            [f(f"HK_{key['name']}", "string", True), f(key["name"], key["datatype"])]
            + [f(x["name"], x["datatype"]) for x in others]
            + [f("LOAD_DTS", "timestamp")]
        )
    if layer == "imart":
        return f"FACT_{domain}", (
            [f(f"SK_{domain}", "integer", True), f(key["name"], key["datatype"])]
            + [f(x["name"], x["datatype"]) for x in others]
        )
    return f"TGT_{domain}", list(source_fields)


def _tx_datatype(neutral_type):
    return {"string": "string", "integer": "integer", "decimal": "decimal", "date": "date/time", "timestamp": "date/time"}.get((neutral_type or "string").lower(), "string")

def _build_domain_mapping_xml(domain, source_fields):
    if not source_fields: return "", "", "", "", []
    source_name = f"STG_{domain}_STAGING"
    target_name = f"HUB_{domain}"
    sq_name = f"SQ_{source_name}"
    exp_name = f"EXP_{domain}_GENERATE_KEYS"
    key_field, key_dtype, key_prec = source_fields[0]

    target_fields = [(f"HK_{key_field}", "string", 32), (key_field, key_dtype, key_prec), ("LOAD_DTS", "timestamp", 26), ("REC_SRC", "string", 50)]

    sourcefield_rows = []
    for n, d, p in source_fields:
        kt = "PRIMARY KEY" if n == key_field else "NOT A KEY"
        sourcefield_rows.append(f'            <SOURCEFIELD NAME="{n}" DATATYPE="{d}" PRECISION="{p}" KEYTYPE="{kt}" NULLABLE="NULL"/>')
    sourcefield_xml = "\n".join(sourcefield_rows)

    targetfield_rows = []
    for n, d, p in target_fields:
        kt = "PRIMARY KEY" if str(n).startswith("HK_") else "NOT A KEY"
        targetfield_rows.append(f'            <TARGETFIELD NAME="{n}" DATATYPE="{d}" PRECISION="{p}" KEYTYPE="{kt}" NULLABLE="NULL"/>')
    targetfield_xml = "\n".join(targetfield_rows)

    sq_ports = "\n".join(f'            <TRANSFORMFIELD DATATYPE="{_tx_datatype(d)}" EXPRESSION="{n}" NAME="{n}" PORTTYPE="INPUT/OUTPUT" PRECISION="{p}" SCALE="0"/>' for n, d, p in source_fields)
    
    exp_ports = [
        f'            <TRANSFORMFIELD DATATYPE="{_tx_datatype(key_dtype)}" EXPRESSION="{key_field}" NAME="{key_field}" PORTTYPE="INPUT/OUTPUT" PRECISION="{key_prec}" SCALE="0"/>',
        f'            <TRANSFORMFIELD DATATYPE="string" EXPRESSION="MD5(TO_CHAR({key_field}))" EXPRESSIONTYPE="GENERAL" NAME="HK_{key_field}" PORTTYPE="OUTPUT" PRECISION="32" SCALE="0"/>',
        f'            <TRANSFORMFIELD DATATYPE="date/time" EXPRESSION="SYSDATE" EXPRESSIONTYPE="GENERAL" NAME="LOAD_DTS" PORTTYPE="OUTPUT" PRECISION="29" SCALE="9"/>',
        f'            <TRANSFORMFIELD DATATYPE="string" EXPRESSION="\'{domain}_STAGING\'" EXPRESSIONTYPE="GENERAL" NAME="REC_SRC" PORTTYPE="OUTPUT" PRECISION="50" SCALE="0"/>',
    ]
    exp_ports_xml = "\n".join(exp_ports)

    connectors = (
        [f'        <CONNECTOR FROMFIELD="{n}" FROMINSTANCE="{source_name}" TOFIELD="{n}" TOINSTANCE="{sq_name}"/>' for n, _, _ in source_fields]
        + [f'        <CONNECTOR FROMFIELD="{key_field}" FROMINSTANCE="{sq_name}" TOFIELD="{key_field}" TOINSTANCE="{exp_name}"/>']
        + [f'        <CONNECTOR FROMFIELD="{n}" FROMINSTANCE="{exp_name}" TOFIELD="{n}" TOINSTANCE="{target_name}"/>' for n, _, _ in target_fields]
    )
    connectors_xml = "\n".join(connectors)

    source_xml = f'        <SOURCE DATABASETYPE="Oracle" DESCRIPTION="Staging source for {domain}" NAME="{source_name}" OWNERNAME="dbo">\n{sourcefield_xml}\n        </SOURCE>'
    target_xml = f'        <TARGET DATABASETYPE="Oracle" DESCRIPTION="Data Vault Hub for {domain}" NAME="{target_name}">\n{targetfield_xml}\n        </TARGET>'
    
    mapping_xml = f"""        <MAPPING DESCRIPTION="Loads {source_name} into {target_name} via hash-key generation" NAME="m_{domain}_LOAD_HUB">
            <TRANSFORMATION DESCRIPTION="Reads from {source_name}" NAME="{sq_name}" TYPE="Source Qualifier">
{sq_ports}
            </TRANSFORMATION>
            <TRANSFORMATION DESCRIPTION="Generates hub hash key, load timestamp, record source" NAME="{exp_name}" TYPE="Expression">
{exp_ports_xml}
            </TRANSFORMATION>
            <INSTANCE NAME="{source_name}" TRANSFORMATION_NAME="{source_name}" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
            <INSTANCE NAME="{sq_name}" TRANSFORMATION_NAME="{sq_name}" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
            <INSTANCE NAME="{exp_name}" TRANSFORMATION_NAME="{exp_name}" TRANSFORMATION_TYPE="Expression" TYPE="TRANSFORMATION"/>
            <INSTANCE NAME="{target_name}" TRANSFORMATION_NAME="{target_name}" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
{connectors_xml}
        </MAPPING>"""

    return source_xml, target_xml, mapping_xml, target_name, target_fields

def initialize_domain_workspaces():
    base_dir = "workflows"
    os.makedirs(base_dir, exist_ok=True)

    for domain in DOMAINS:
        domain_dir = os.path.join(base_dir, domain)
        os.makedirs(domain_dir, exist_ok=True)
        source_fields = DOMAIN_SOURCE_FIELDS.get(domain, [])
        source_xml, target_xml, mapping_xml, target_name, target_fields = _build_domain_mapping_xml(domain, source_fields)

        sample_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="09/23/2026" REPOSITORY_VERSION="189.98">
  <REPOSITORY CODEPAGE="UTF-8" DATABASETYPE="Oracle" NAME="REP_PROD" VERSION="189">
    <FOLDER DESCRIPTION="Enterprise {domain} pipelines" GROUP="" NAME="{domain}_DATA_VAULT" OWNER="Administrator" SHARED="NOTSHARED">
{source_xml}

{target_xml}

{mapping_xml}

        <WORKFLOW DESCRIPTION="Daily batch load for {domain} into Raw Data Vault" ISENABLED="YES" NAME="wf_{domain}_DAILY_BATCH" VERSIONNUMBER="1">
            <TASK DESCRIPTION="Initialize Variables" NAME="Start" REUSABLE="NO" TYPE="Start" VERSIONNUMBER="1"/>
            <SESSION DESCRIPTION="Extract from Source, generate hub key, load Hub" NAME="s_m_{domain}_LOAD_HUB" REUSABLE="NO" TYPE="Session" VERSIONNUMBER="1"/>
            <SESSION DESCRIPTION="Load Links and Sats" NAME="s_m_{domain}_LOAD_LINKS_SATS" REUSABLE="NO" TYPE="Session" VERSIONNUMBER="1"/>
            <WORKFLOWLINK CONDITION="" FROMTASK="Start" TOTASK="s_m_{domain}_LOAD_HUB"/>
            <WORKFLOWLINK CONDITION="$s_m_{domain}_LOAD_HUB.Status=SUCCEEDED" FROMTASK="s_m_{domain}_LOAD_HUB" TOTASK="s_m_{domain}_LOAD_LINKS_SATS"/>
        </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>"""
        with open(os.path.join(domain_dir, f"wf_{domain}_DAILY_BATCH.xml"), "w") as f:
            f.write(sample_xml)

initialize_domain_workspaces()

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def clean_ddl(ddl_text):
    return sqlparse.format(ddl_text, strip_comments=True, reindent=True, keyword_case='upper')

# ============================================================
# DETERMINISTIC DDL / FIELD PARSING
# ============================================================
# The local LLM (qwen3:4b) is unreliable at turning raw DDL into strict JSON
# structure, which is what was causing the DDL popup to loop forever even
# after the user supplied everything. These helpers parse DDL/field lists
# ourselves so source/target structure never depends on the model getting
# JSON formatting right — the LLM is only asked to design the transformation
# logic on top of structure we already know is correct.
_DATATYPE_NORMALIZE = {
    "VARCHAR": "string", "VARCHAR2": "string", "CHAR": "string", "NCHAR": "string",
    "NVARCHAR": "string", "TEXT": "string", "STRING": "string", "CLOB": "string",
    "INT": "integer", "INTEGER": "integer", "BIGINT": "integer", "SMALLINT": "integer", "TINYINT": "integer", "NUMBER": "integer",
    "DECIMAL": "decimal", "NUMERIC": "decimal", "FLOAT": "decimal", "DOUBLE": "decimal", "REAL": "decimal", "MONEY": "decimal",
    "DATE": "date",
    "TIMESTAMP": "timestamp", "DATETIME": "timestamp", "DATETIME2": "timestamp",
    "BOOLEAN": "boolean", "BOOL": "boolean", "BIT": "boolean",
}

def _normalize_datatype(raw):
    if not raw:
        return "string"
    key = re.split(r"[\s(]", raw.strip().upper())[0]
    return _DATATYPE_NORMALIZE.get(key, "string")

def parse_ddl_or_fields(text):
    """
    Parses either a full 'CREATE TABLE ... (...)' DDL statement or a plain
    list of field names/types (one per line — 'CUSTOMER_ID INT' or just
    'CUSTOMER_ID') into a structured field list.
    Returns (table_name_or_None, [{"name","datatype","key","expression"}]).
    """
    if not text or not text.strip():
        return None, []

    text = text.strip()
    m = re.search(r"CREATE\s+TABLE\s+([`\"\[]?[\w.]+[`\"\]]?)\s*\((.*)\)\s*;?\s*$", text, re.IGNORECASE | re.DOTALL)
    if m:
        table_name = m.group(1).strip('`"[]').split(".")[-1]
        body = m.group(2)
        parts, depth, buf = [], 0, ""
        for ch in body:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                parts.append(buf)
                buf = ""
            else:
                buf += ch
        if buf.strip():
            parts.append(buf)

        fields = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            upper = part.upper()
            if upper.startswith(("PRIMARY KEY", "FOREIGN KEY", "CONSTRAINT", "UNIQUE", "CHECK", "INDEX", "KEY ")):
                continue
            tokens = part.split(None, 2)
            if len(tokens) < 2:
                continue
            name = tokens[0].strip('`"[]')
            fields.append({
                "name": name,
                "datatype": _normalize_datatype(tokens[1]),
                "key": "PRIMARY KEY" in upper,
                "expression": "",
            })
        return table_name, fields

    # Fallback: plain field list -- newline-separated, and/or comma-separated
    # on one line (e.g. "customer_id int, first_name varchar, email varchar").
    segments = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "," in line:
            segments.extend(part for part in line.split(",") if part.strip())
        else:
            segments.append(line)

    fields = []
    for seg in segments:
        seg = seg.strip().rstrip(",")
        if not seg:
            continue
        tokens = seg.split(None, 1)
        name = tokens[0].strip('`"[]')
        dtype_raw = tokens[1] if len(tokens) > 1 else "string"
        fields.append({"name": name, "datatype": _normalize_datatype(dtype_raw), "key": False, "expression": ""})
    return None, fields

def _find_create_table_blocks(text):
    """Finds each 'CREATE TABLE ... (...)' statement in text, correctly
    tracking paren depth so a nested paren (e.g. VARCHAR(30)) doesn't
    truncate the match early."""
    blocks = []
    for m in re.finditer(r"CREATE\s+TABLE\s+[`\"\[]?[\w.]+[`\"\]]?\s*\(", text, re.IGNORECASE):
        start = m.start()
        i = m.end() - 1  # position of the opening '('
        depth = 0
        while i < len(text):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        end = i + 1
        if end < len(text) and text[end] == ";":
            end += 1
        blocks.append(text[start:end])
    return blocks

_DB_PLATFORM_WORDS = {
    "oracle", "teradata", "mysql", "postgres", "postgresql", "sqlserver", "sql server",
    "snowflake", "redshift", "bigquery", "db2", "sybase", "netezza", "hana", "mongodb",
    "as400", "db400", "generic/ansi", "generic", "ansi",
}

def split_source_target_blocks(text):
    """
    If a message contains two CREATE TABLE statements, or explicit
    SOURCE:/TARGET: markers with real column content, splits it into
    (source_text, target_text). Returns (None, None) if it can't confidently
    find both -- in particular, "Source: oracle" / "Target: teradata" (naming
    a DB platform, not a schema) must NOT be mistaken for column definitions.
    """
    creates = _find_create_table_blocks(text)
    if len(creates) >= 2:
        return creates[0], creates[1]

    # Prefer the more specific "Source columns:"/"Target columns:" markers --
    # much less ambiguous than bare "Source:"/"Target:", which often just
    # names a DB platform rather than introducing a schema.
    m_src_cols = re.search(r"Source\s+columns?\s*:?\s*(.*?)(?=\n\s*Target|\Z)", text, re.IGNORECASE | re.DOTALL)
    m_tgt_cols = re.search(r"Target\s+columns?\s*:?\s*(.*?)(?=\n\s*(?:Requirements?|Generate)\s*:|\Z)", text, re.IGNORECASE | re.DOTALL)
    if m_src_cols and m_tgt_cols and m_src_cols.group(1).strip() and m_tgt_cols.group(1).strip():
        return m_src_cols.group(1).strip(), m_tgt_cols.group(1).strip()

    m_src = re.search(r"(?<!\w)SOURCE\s*:?\s*(.*?)(?=TARGET\s*:|\Z)", text, re.IGNORECASE | re.DOTALL)
    m_tgt = re.search(r"(?<!\w)TARGET\s*:?\s*(.*)", text, re.IGNORECASE | re.DOTALL)
    if m_src and m_tgt and m_src.group(1).strip() and m_tgt.group(1).strip():
        src_text, tgt_text = m_src.group(1).strip(), m_tgt.group(1).strip()
        # Guard: if either block's first line is just a bare DB platform name
        # with nothing else useful, this is "Source: oracle" -- not a schema.
        src_first_line = src_text.splitlines()[0].strip().lower()
        tgt_first_line = tgt_text.splitlines()[0].strip().lower()
        if src_first_line in _DB_PLATFORM_WORDS or tgt_first_line in _DB_PLATFORM_WORDS:
            return None, None
        return src_text, tgt_text

    return None, None

def extract_source_only_fields(text):
    """
    Best-effort: finds a 'Source'/'Source columns' block in free text and
    parses it, even when no matching Target block exists (e.g. the target
    schema must be derived via a transformation like a concatenation, which
    genuinely needs the LLM to reason about -- but the source structure
    itself doesn't need to be re-derived by the model).
    """
    # Prefer the more specific "Source columns:" marker -- a bare "Source:"
    # often just names a DB platform ("Source: oracle"), not a schema.
    m = re.search(r"Source\s+columns?\s*:?\s*(.*?)(?=\n\s*(?:Target|Requirements?)\s*:|\Z)", text, re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"(?<!\w)Source\s*:?\s*(.*?)(?=\n\s*(?:Target|Requirements?)\s*:|\Z)", text, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    _, fields = parse_ddl_or_fields(m.group(1).strip())
    return fields

def build_deterministic_etl(pipeline_name, description, source_name, source_fields, target_name, target_fields):
    """
    Builds a valid, guaranteed-correct SOURCE -> Source Qualifier -> TARGET
    pipeline directly from parsed structure — no LLM involved. This is the
    baseline that gets displayed/exported immediately; the LLM can still be
    asked afterwards to layer extra transformations on top of it, but the
    user is never blocked waiting on the model to get structure right.
    """
    sq_id = f"sq_{re.sub(r'[^a-zA-Z0-9]', '_', source_name.lower())}"
    sq_fields = [{"name": f["name"], "datatype": f["datatype"], "expression": ""} for f in source_fields]

    components = [{
        "id": sq_id,
        "name": f"SQ_{source_name}",
        "type": "SOURCE_QUALIFIER",
        "description": "Auto-generated Source Qualifier",
        "fields": sq_fields,
        "condition": "",
    }]
    connections = [
        {"from": source_name, "to": sq_id, "label": ""},
        {"from": sq_id, "to": target_name, "label": ""},
    ]

    return {
        "needs_more_info": False,
        "clarifying_question": "",
        "pipeline_name": pipeline_name,
        "description": description,
        "source": {"id": source_name, "name": source_name, "type": "SOURCE", "fields": source_fields},
        "components": components,
        "connections": connections,
        "target": {"id": target_name, "name": target_name, "type": "TARGET", "fields": target_fields},
    }

def _md_cell(value):
    """Escape a value for use inside a Markdown table cell (pipes such as the
    || concatenation operator would otherwise split the row into extra columns)."""
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")

def generate_markdown_docs(etl):
    md = f"# ETL Documentation: {etl.get('pipeline_name', 'Pipeline')}\n\n"
    md += f"**Description**: {etl.get('description') or 'N/A'}\n\n"

    source = etl.get("source") or {}
    target = etl.get("target") or {}
    comps = etl.get("components") or []

    # ---- Source-to-target mapping: for each target column, the last
    # component that declares it (with an expression) or the source column.
    md += "---\n## Source-to-Target Mapping\n"
    md += "| Target Column | Data Type | Derived From | Logic/Expression |\n|---|---|---|---|\n"
    src_names = {(f.get("name") if isinstance(f, dict) else f) for f in source.get("fields", [])}
    for tf in target.get("fields", []):
        tname = tf.get("name", "") if isinstance(tf, dict) else tf
        ttype = tf.get("datatype", "") if isinstance(tf, dict) else ""
        origin, logic = "-", "passthrough"
        for comp in reversed(comps):
            hit = next((f for f in comp.get("fields", []) if isinstance(f, dict) and f.get("name") == tname), None)
            if hit and hit.get("expression") and hit.get("expression") != tname:
                origin, logic = comp.get("name", "component"), hit["expression"]
                break
        else:
            if tname in src_names:
                origin = source.get("name", "source")
        md += f"| {_md_cell(tname)} | {_md_cell(ttype)} | {_md_cell(origin)} | {_md_cell(logic)} |\n"
    md += "\n"

    md += "---\n## Components & Flow\n"
    for comp in [source] + comps + [target]:
        if not comp: continue
        name = comp.get("name", "Unnamed")
        ctype = comp.get("type", "Component")
        md += f"### {name} ({ctype})\n"
        if comp.get("description"): md += f"*{comp.get('description')}*\n\n"
        if comp.get("condition"): md += f"**Condition**: `{comp.get('condition')}`\n\n"
        md += "| Column | Data Type | Logic/Expression |\n|---|---|---|\n"
        for f in comp.get("fields", []):
            fname = f.get("name", "") if isinstance(f, dict) else f
            ftype = f.get("datatype", "") if isinstance(f, dict) else ""
            fexpr = (f.get("expression") if isinstance(f, dict) else "") or "passthrough"
            md += f"| {_md_cell(fname)} | {_md_cell(ftype)} | {_md_cell(fexpr)} |\n"
        md += "\n"
    return md

def render_simple_lineage(etl):
    """
    Field-level lineage preview of a generated ETL pipeline.

    Uses the SAME compute_field_flow() as the PowerCenter XML exporter, so the
    preview always shows exactly the connections that will be exported --
    including fields an Expression consumes but doesn't redeclare (e.g.
    first_name/second_name feeding full_name), which the old name-only
    matching left dangling. Orange edges = positional fallback wiring (used
    when no field names match between two connected nodes).
    """
    if not etl:
        return None

    flow = compute_field_flow(etl.get("source"), etl.get("target"), etl.get("components"), etl.get("connections"))
    source, target = flow["source"], flow["target"]
    component_map = flow["component_map"]
    outputs_by_id, inputs_by_id = flow["outputs_by_id"], flow["inputs_by_id"]

    graph = graphviz.Digraph(engine="dot")
    graph.attr(rankdir="LR", size="12,6", nodesep="0.4", ranksep="0.8")
    graph.attr('node', shape='plaintext', fontname='Helvetica', fontsize='10')
    graph.attr('edge', color='#777777', arrowsize='0.7')

    def esc(text):
        return html.escape(str(text if text is not None else ""), quote=True)

    def field_names(fields):
        return [n for f in (fields or []) if (n := (f.get("name") if isinstance(f, dict) else f))]

    # Ordered node list: source, components in topological order, target.
    nodes = []  # (id, display_name, type_label, header_color, [(field, expression_or_None)])
    src_fields = field_names(source.get("fields"))
    nodes.append((source["id"], source.get("name", "SRC"), "SOURCE", "#E3F2FD", [(n, None) for n in src_fields]))

    for cid in flow["ordered_ids"]:
        comp = component_map[cid]
        declared = {(f.get("name") if isinstance(f, dict) else f): f for f in comp.get("fields", []) if f}
        rows = []
        for name in outputs_by_id.get(cid, {}):
            d = declared.get(name)
            expr = d.get("expression") if isinstance(d, dict) else None
            rows.append((name, expr if expr and expr != name else None))
        color = "#FFF8E1" if str(comp.get("type", "")).upper() == "SOURCE_QUALIFIER" else "#F3E5F5"
        nodes.append((cid, comp.get("name", cid), str(comp.get("type", "COMP")).upper(), color, rows))

    nodes.append((target["id"], target.get("name", "TGT"), "TARGET", "#E8F5E9", [(n, None) for n in field_names(target.get("fields"))]))

    # Safe, generated ids for graph nodes and ports -- real names can contain
    # spaces/colons/quotes that would otherwise break Graphviz port syntax.
    node_key = {}
    port_key = {}
    for i, (nid, nname, ntype, color, rows) in enumerate(nodes):
        node_key[nid] = f"n{i}"
        port_key[nid] = {fname: f"p{j}" for j, (fname, _) in enumerate(rows)}
        label = (
            f'<<TABLE BORDER="0" CELLBORDER="1" CELLPADDING="4" CELLSPACING="0">'
            f'<TR><TD BGCOLOR="{color}"><B>{esc(nname)}</B><BR/><FONT POINT-SIZE="9">({esc(ntype)})</FONT></TD></TR>'
        )
        for j, (fname, expr) in enumerate(rows):
            text = esc(fname)
            if expr:
                short = expr if len(expr) <= 40 else expr[:37] + "..."
                text += f'<BR/><FONT POINT-SIZE="8" COLOR="#6A1B9A">= {esc(short)}</FONT>'
            label += f'<TR><TD ALIGN="LEFT" PORT="p{j}">{text}</TD></TR>'
        label += '</TABLE>>'
        graph.node(node_key[nid], label=label)

    # Edges: same rules as the XML exporter (name match, else positional).
    for conn in flow["connections"]:
        frm, to = conn.get("from"), conn.get("to")
        if frm not in node_key or to not in node_key:
            continue
        f_flds = list(outputs_by_id.get(frm, {}).keys())
        t_flds = inputs_by_id.get(to, [])
        t_set = set(t_flds)
        mapped = False
        for n in f_flds:
            if n in t_set and n in port_key[frm] and n in port_key[to]:
                graph.edge(f"{node_key[frm]}:{port_key[frm][n]}:e", f"{node_key[to]}:{port_key[to][n]}:w")
                mapped = True
        if not mapped and f_flds and t_flds:
            for i in range(min(len(f_flds), len(t_flds))):
                a, b = f_flds[i], t_flds[i]
                if a in port_key[frm] and b in port_key[to]:
                    graph.edge(f"{node_key[frm]}:{port_key[frm][a]}:e", f"{node_key[to]}:{port_key[to][b]}:w", color="#ff9800")
                    mapped = True
        if not mapped:
            graph.edge(node_key[frm], node_key[to])
    return graph

# ============================================================
# WORKFLOW VISUAL MAPPING (Explanation sector)
# ============================================================
WORKFLOW_TASK_STYLE = {
    "Start": {"shape": "circle", "fillcolor": "#4CAF50", "fontcolor": "white", "width": "0.6"},
    "Session": {"shape": "box", "style": "filled,rounded", "fillcolor": "#E3F2FD"},
    "Decision": {"shape": "diamond", "fillcolor": "#FFF3E0"},
    "Command": {"shape": "box", "style": "filled,rounded", "fillcolor": "#F3E5F5"},
    "Email": {"shape": "note", "fillcolor": "#FFFDE7"},
    "Timer": {"shape": "box", "style": "filled,rounded", "fillcolor": "#FBE9E7"},
    "Event-Wait": {"shape": "box", "style": "filled,rounded", "fillcolor": "#FBE9E7"},
}
WORKFLOW_TASK_DEFAULT_STYLE = {"shape": "box", "style": "filled,rounded", "fillcolor": "#ECEFF1"}

def render_workflow_diagram(root):
    workflows = root.findall(".//WORKFLOW")
    if not workflows: return None
    graph = graphviz.Digraph(engine="dot")
    graph.attr(rankdir="LR", nodesep="0.45", ranksep="0.85")
    graph.attr('node', fontname='Helvetica', fontsize='11', fontcolor='#1a1a1a')
    graph.attr('edge', color='#888888', fontname='Helvetica', fontsize='9', fontcolor='#555555', arrowsize='0.8')

    for i, wf in enumerate(workflows):
        wf_name = wf.get("NAME", f"WORKFLOW_{i}")
        with graph.subgraph(name=f"cluster_{i}") as c:
            c.attr(label=f"  {wf_name}  ", fontsize='13', fontname='Helvetica-Bold', color='#B0BEC5', style='rounded', bgcolor='#FAFAFA', labeljust='l')
            seen = set()
            for tag in ("TASK", "SESSION"):
                for elem in wf.findall(tag):
                    name = elem.get("NAME")
                    if not name or name in seen: continue
                    seen.add(name)
                    ttype = elem.get("TYPE", tag.title())
                    style = WORKFLOW_TASK_STYLE.get(ttype, WORKFLOW_TASK_DEFAULT_STYLE)
                    tooltip = elem.get("DESCRIPTION", "") or f"{ttype} task"
                    c.node(f"{i}_{name}", label=name, tooltip=tooltip, **style)

            for link in wf.findall("WORKFLOWLINK"):
                frm, to = link.get("FROMTASK"), link.get("TOTASK")
                if not frm or not to: continue
                cond = link.get("CONDITION", "")
                edge_label = "✓ on success" if "SUCCEEDED" in cond.upper() else (cond if len(cond) <= 28 else cond[:25] + "…") if cond else ""
                c.edge(f"{i}_{frm}", f"{i}_{to}", label=edge_label, labeltooltip=cond)
    return graph

MAPPING_INSTANCE_STYLE = {"Source Definition": "#E3F2FD", "Target Definition": "#E8F5E9", "Source Qualifier": "#FFF8E1", "Expression": "#F3E5F5"}
MAPPING_INSTANCE_DEFAULT_STYLE = "#ECEFF1"

def render_mapping_diagram(root):
    mapping = root.find(".//MAPPING")
    if mapping is None: return None
    graph = graphviz.Digraph(engine="dot")
    graph.attr(rankdir="LR", nodesep="0.4", ranksep="0.9")
    graph.attr('node', shape='plaintext', fontname='Helvetica', fontsize='10')
    graph.attr('edge', color='#777777', arrowsize='0.7', fontsize='8', fontcolor='#666666')

    fields_by_instance = {}
    for src in root.findall(".//SOURCE"): fields_by_instance[src.get("NAME")] = [f.get("NAME") for f in src.findall("SOURCEFIELD")]
    for tgt in root.findall(".//TARGET"): fields_by_instance[tgt.get("NAME")] = [f.get("NAME") for f in tgt.findall("TARGETFIELD")]
    for tx in mapping.findall("TRANSFORMATION"): fields_by_instance[tx.get("NAME")] = [f.get("NAME") for f in tx.findall("TRANSFORMFIELD")]

    for inst in mapping.findall("INSTANCE"):
        name = inst.get("NAME")
        itype = inst.get("TRANSFORMATION_TYPE", "")
        color = MAPPING_INSTANCE_STYLE.get(itype, MAPPING_INSTANCE_DEFAULT_STYLE)
        flds = fields_by_instance.get(name, [])
        rows = "".join(f'<TR><TD ALIGN="LEFT" PORT="{f}">{f}</TD></TR>' for f in flds)
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLPADDING="4" CELLSPACING="0"><TR><TD BGCOLOR="{color}"><B>{name}</B><BR/><FONT POINT-SIZE="9">({itype})</FONT></TD></TR>{rows}</TABLE>>'
        graph.node(name, label=label)

    for conn in mapping.findall("CONNECTOR"):
        f_inst, f_fld = conn.get("FROMINSTANCE"), conn.get("FROMFIELD")
        t_inst, t_fld = conn.get("TOINSTANCE"), conn.get("TOFIELD")
        if not f_inst or not t_inst: continue
        f_port = f"{f_inst}:{f_fld}:e" if f_fld else f_inst
        t_port = f"{t_inst}:{t_fld}:w" if t_fld else t_inst
        graph.edge(f_port, t_port)
    return graph

def render_cib_architecture_diagram(highlight_source=None, highlight_layer=None):
    """
    Layered CIB Logical Data Architecture: Source Systems -> Staging -> Data
    Vault (Hub/Link/Satellite). From Data Vault, Business Vault and
    Information Mart are PARALLEL siblings (both consume directly from the
    Data Vault — iMart does not require passing through Business Vault
    first), and both feed Business Objects/Delivery.

    highlight_source: one of "t24"/"card400"/"cr2"/"ivr" to bold that source
    node + its edge into Staging.
    highlight_layer: one of "dv"/"bv"/"imart" to bold that layer's edges
    from Data Vault, showing the active selection in the interactive builder.
    """
    graph = graphviz.Digraph(engine="dot")
    graph.attr(rankdir="LR", nodesep="0.3", ranksep="0.65", compound="true")
    graph.attr('node', shape='box', style='filled,rounded', fontname='Helvetica', fontsize='10')
    graph.attr('edge', color='#888888', arrowsize='0.7', penwidth='1.1')

    HIGHLIGHT_COLOR = "#1E88E5"

    with graph.subgraph(name="cluster_src") as c:
        c.attr(label="Source Systems", fontsize='12', fontname='Helvetica-Bold', style='rounded', color='#90A4AE', bgcolor='#FAFAFA')
        for nid, label in [("t24", "T24\n(Core Banking)"), ("card400", "CARD400\n(Card Systems)"),
                            ("cr2", "CR2\n(ATM Switches)"), ("ivr", "IVR\n(Call Center)")]:
            is_hl = nid == highlight_source
            c.node(nid, label=label, fillcolor="#1E88E5" if is_hl else "#E3F2FD",
                   fontcolor="white" if is_hl else "black", penwidth="2.5" if is_hl else "1")

    graph.node("stg", label="Staging (STG)\n1:1 raw tables\nNo business logic", fillcolor="#FFF3E0")

    with graph.subgraph(name="cluster_dv") as c:
        c.attr(label="Data Vault (DV)", fontsize='12', fontname='Helvetica-Bold', style='rounded', color='#90A4AE', bgcolor='#FAFAFA')
        dv_fill = "#1E88E5" if highlight_layer == "dv" else "#E8F5E9"
        dv_font = "white" if highlight_layer == "dv" else "black"
        for nid, label in [("hub", "Hub"), ("link", "Link"), ("sat", "Satellite")]:
            c.node(nid, label=label, fillcolor=dv_fill, fontcolor=dv_font, penwidth="2.5" if highlight_layer == "dv" else "1")

    # Business Vault and Information Mart are drawn as PARALLEL siblings: both
    # are placed on the same rank so they sit side-by-side (not one after the
    # other), and both connect directly back to the Data Vault cluster.
    bv_fill = "#1E88E5" if highlight_layer == "bv" else "#F3E5F5"
    graph.node("bv", label="Business Vault (BV)\nComputations\nSoft business rules",
                fillcolor=bv_fill, fontcolor="white" if highlight_layer == "bv" else "black",
                penwidth="2.5" if highlight_layer == "bv" else "1")

    with graph.subgraph(name="cluster_imart") as c:
        c.attr(label="Information Mart (iMART)", fontsize='12', fontname='Helvetica-Bold', style='rounded', color='#90A4AE', bgcolor='#FAFAFA')
        im_fill = "#1E88E5" if highlight_layer == "imart" else "#FCE4EC"
        im_font = "white" if highlight_layer == "imart" else "black"
        for nid, label in [("fact", "Fact"), ("dim", "Dimension")]:
            c.node(nid, label=label, fillcolor=im_fill, fontcolor=im_font, penwidth="2.5" if highlight_layer == "imart" else "1")

    # BV and the iMart cluster naturally land on the same rank (dot's default
    # longest-path layering) since both are exactly one hop from the Data
    # Vault cluster and one hop to Business Objects — i.e. parallel siblings.

    with graph.subgraph(name="cluster_bo") as c:
        c.attr(label="Business Objects / Delivery", fontsize='12', fontname='Helvetica-Bold', style='rounded', color='#90A4AE', bgcolor='#FAFAFA')
        for nid, label in [("tableau", "Tableau"), ("ofsaa", "OFSAA\n(Risk/Finance)")]:
            c.node(nid, label=label, fillcolor="#ECEFF1")

    # Source Systems -> Staging (single edge from cluster boundary)
    src_edge_color = HIGHLIGHT_COLOR if highlight_source else "#888888"
    src_tail = highlight_source or "t24"
    graph.edge(src_tail, "stg", ltail="cluster_src", color=src_edge_color, penwidth="2.2" if highlight_source else "1.1")

    # Staging -> Data Vault
    graph.edge("stg", "hub", lhead="cluster_dv",
               color=HIGHLIGHT_COLOR if highlight_layer == "dv" else "#888888",
               penwidth="2.2" if highlight_layer == "dv" else "1.1")

    # Data Vault -> Business Vault (parallel branch 1)
    graph.edge("hub", "bv", ltail="cluster_dv",
               color=HIGHLIGHT_COLOR if highlight_layer == "bv" else "#888888",
               penwidth="2.2" if highlight_layer == "bv" else "1.1")

    # Data Vault -> Information Mart (parallel branch 2 — does NOT require Business Vault)
    graph.edge("hub", "fact", ltail="cluster_dv", lhead="cluster_imart",
               color=HIGHLIGHT_COLOR if highlight_layer == "imart" else "#888888",
               penwidth="2.2" if highlight_layer == "imart" else "1.1")

    # Business Vault -> Business Objects
    graph.edge("bv", "tableau", lhead="cluster_bo")
    # Information Mart -> Business Objects
    graph.edge("fact", "tableau", ltail="cluster_imart", lhead="cluster_bo")

    return graph

# ============================================================
# LOCAL PMREP DEPLOYMENT
# ============================================================
def deploy_local_pmrep(xml_string, pc_domain, pc_repo, pc_user, pc_pwd, target_folder):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode='w', encoding='utf-8') as tmp_map:
            tmp_map.write(xml_string)
            map_path = tmp_map.name
            
        control_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE IMPORTPARAMS SYSTEM "impcntl.dtd">
        <IMPORTPARAMS CHECKRESOLUTION="YES">
            <FOLDERMAP SOURCEFOLDERNAME="{target_folder}" SOURCEREPOSITORYNAME="{pc_repo}" TARGETFOLDERNAME="{target_folder}" TARGETREPOSITORYNAME="{pc_repo}"/>
            <RESOLVECONFLICT><TYPEOBJECT OBJECTTYPENAME="ALL" RESOLUTION="REPLACE"/></RESOLVECONFLICT>
        </IMPORTPARAMS>"""

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode='w', encoding='utf-8') as tmp_ctl:
            tmp_ctl.write(control_xml)
            ctl_path = tmp_ctl.name

        connect_cmd = ["pmrep", "connect", "-r", pc_repo, "-d", pc_domain, "-n", pc_user, "-x", pc_pwd]
        import_cmd = ["pmrep", "objectimport", "-i", map_path, "-c", ctl_path, "-f", target_folder]
        
        conn_res = subprocess.run(connect_cmd, capture_output=True, text=True)
        if conn_res.returncode != 0:
            os.remove(map_path)
            os.remove(ctl_path)
            return False, f"Connection Failed:\n{conn_res.stderr}\n{conn_res.stdout}"
            
        imp_res = subprocess.run(import_cmd, capture_output=True, text=True)
        
        os.remove(map_path)
        os.remove(ctl_path)
        
        if imp_res.returncode != 0 or "Failed to execute" in imp_res.stdout:
            return False, f"Import Failed:\n{imp_res.stderr}\n{imp_res.stdout}"
            
        return True, imp_res.stdout
    except Exception as e:
        return False, str(e)

# ============================================================
# SIDEBAR  (kept minimal: two pages + one collapsed settings box)
# ============================================================
PAGE_EXPLAIN = "🔍 Explain a workflow"
PAGE_BUILD = "🏗️ Build an ETL"

with st.sidebar:
    st.markdown("### 🤖 PowerCenter Architect")
    page = st.radio(
        "Go to", [PAGE_EXPLAIN, PAGE_BUILD],
        index=0 if st.session_state.page == "Workflow Intelligence" else 1,
        label_visibility="collapsed",
    )
    st.session_state.page = "Workflow Intelligence" if page == PAGE_EXPLAIN else "ETL Generation"

    st.divider()
    with st.expander("⚙️ Settings", expanded=False):
        st.session_state.domain_sector = st.selectbox("Business domain", DOMAINS)
        st.session_state.database_type = st.selectbox("Target database", ["Teradata", "Oracle", "Microsoft SQL Server", "Generic/ANSI"])
    st.caption(f"{st.session_state.domain_sector} · {st.session_state.database_type}")

# ============================================================
# PAGE 1: EXPLAIN A WORKFLOW
#   1. choose a file  ->  2. read the picture  ->  3. ask questions
# ============================================================
if st.session_state.page == "Workflow Intelligence":
    st.markdown('<div class="main-title">🔍 Explain a workflow</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Choose a file, read the picture, ask questions.</div>', unsafe_allow_html=True)

    # ---- Step 1: choose a file (a library file is pre-selected; upload is optional)
    domain_folder = os.path.join("workflows", st.session_state.domain_sector)
    file_names = sorted(os.path.basename(f) for f in glob.glob(f"{domain_folder}/*.xml"))

    st.markdown(f"**1 · Choose a workflow** &nbsp;·&nbsp; {st.session_state.domain_sector} library")
    lib_selection = st.selectbox("Workflow", file_names, label_visibility="collapsed") if file_names else None
    with st.expander("Have your own XML? Upload it instead"):
        uploaded_file = st.file_uploader("PowerCenter XML", type=["xml"], label_visibility="collapsed")

    if uploaded_file is not None:
        active_file_name = uploaded_file.name
        file_key = f"upload::{active_file_name}::{uploaded_file.size}"
        xml_bytes = uploaded_file.getvalue()
        st.caption(f"Using your uploaded file: **{active_file_name}**")
    elif lib_selection:
        active_file_name = lib_selection
        file_key = f"library::{st.session_state.domain_sector}::{active_file_name}"
        try:
            with open(os.path.join(domain_folder, active_file_name), "rb") as f:
                xml_bytes = f.read()
        except Exception as e:
            st.error(f"Could not read {active_file_name}: {e}")
            st.stop()
    else:
        st.info("No workflows in this library yet — upload an XML above to get started.")
        st.stop()

    # ---- Parse
    root = None
    tasks, sessions, links = [], [], []
    source_fields, source_name = [], None
    target_fields, target_name = [], None
    transformation_names = []
    try:
        root = ET.fromstring(xml_bytes)
        tasks = [elem.get("NAME") for elem in root.findall(".//TASK")]
        sessions = [elem.get("NAME") for elem in root.findall(".//SESSION")]
        links = [f'{elem.get("FROMTASK")} ➔ {elem.get("TOTASK")}' for elem in root.findall(".//WORKFLOWLINK")]

        source_elem = root.find(".//SOURCE")
        source_name = source_elem.get("NAME") if source_elem is not None else None
        source_fields = [{"name": f.get("NAME"), "datatype": f.get("DATATYPE"), "precision": f.get("PRECISION")} for f in source_elem.findall("SOURCEFIELD")] if source_elem is not None else []

        target_elem = root.find(".//TARGET")
        target_name = target_elem.get("NAME") if target_elem is not None else None
        target_fields = [{"name": f.get("NAME"), "datatype": f.get("DATATYPE"), "precision": f.get("PRECISION")} for f in target_elem.findall("TARGETFIELD")] if target_elem is not None else []

        transformation_names = [f'{tx.get("NAME")} ({tx.get("TYPE")})' for tx in root.findall(".//MAPPING/TRANSFORMATION")]
    except Exception as e:
        st.error(f"This file isn't valid XML: {e}")
        st.stop()

    # ---- Step 2: read the picture (one summary line instead of a wall of metrics)
    st.markdown("**2 · See what it does**")
    st.caption(f"{len(sessions)} sessions · {len(links)} dependencies · {len(transformation_names)} transformations")

    tab_flow, tab_lineage, tab_details, tab_ask = st.tabs(["🗺️ Flow", "🔀 Data lineage", "📋 Details", "💬 Ask AI"])

    with tab_flow:
        try:
            wf_diagram = render_workflow_diagram(root)
        except Exception as e:
            wf_diagram = None
            st.error(f"Couldn't draw the flow: {e}")
        if wf_diagram:
            st.graphviz_chart(wf_diagram, use_container_width=True)
            st.caption("Green = start · Blue = session · Arrows run only when the previous step succeeds.")
        else:
            st.info("This file has no workflow steps. Try the Data lineage tab.")

    with tab_lineage:
        try:
            mapping_diagram = render_mapping_diagram(root)
        except Exception as e:
            mapping_diagram = None
            st.error(f"Couldn't draw the lineage: {e}")
        if mapping_diagram:
            st.graphviz_chart(mapping_diagram, use_container_width=True)
            st.caption("Blue = source · Yellow = source qualifier · Purple = expression · Green = target. Each arrow is one column.")
        else:
            st.info("This file has no mapping, so there is no column lineage to show.")

    with tab_details:
        col_s, col_t = st.columns(2)
        with col_s:
            st.markdown(f"**Source** · `{source_name or '—'}`")
            if source_fields:
                st.table([{"Column": f["name"], "Type": f["datatype"]} for f in source_fields])
            else:
                st.caption("No source columns in this file.")
        with col_t:
            st.markdown(f"**Target** · `{target_name or '—'}`")
            if target_fields:
                st.table([{"Column": f["name"], "Type": f["datatype"]} for f in target_fields])
            else:
                st.caption("No target columns in this file.")
        if transformation_names:
            st.markdown("**Transformations**")
            st.markdown("\n".join(f"- {t}" for t in transformation_names))
        if links:
            st.markdown("**Run order**")
            st.markdown("\n".join(f"- {l}" for l in links))

    with tab_ask:
        if st.session_state.get("last_analyzed_file") != file_key:
            st.session_state.workflow_messages = []
            st.session_state.last_analyzed_file = file_key

        def _cols(fields):
            return ", ".join(f'{f["name"]} ({f["datatype"]})' for f in fields) or "none defined in this file"

        starters = ["What does this workflow do?", "What are the source columns?", "What transformations run?"]
        st.caption("Not sure what to ask? Tap one:")
        starter_cols = st.columns(len(starters))
        clicked = None
        for i, (col, q) in enumerate(zip(starter_cols, starters)):
            if col.button(q, use_container_width=True, key=f"starter_{i}"):
                clicked = q

        question = clicked or st.chat_input(f"Ask about {active_file_name}…")
        if question:
            st.session_state.workflow_messages.append({"role": "user", "content": question})
            with st.spinner("Thinking…"):
                try:
                    answer = workflow_agent.chat(f"""
You are an expert Informatica PowerCenter architecture partner.
File: {active_file_name} (business domain: {st.session_state.domain_sector}).
Sessions: {sessions}
Dependencies: {links}
Source {source_name}: {_cols(source_fields)}
Target {target_name}: {_cols(target_fields)}
Transformations: {transformation_names}
Answer using only the details above. If something isn't there, say so plainly instead of guessing.

Question: {question}
""")
                except Exception as e:
                    answer = f"Sorry, I couldn't reach the AI ({type(e).__name__}). Check that Ollama is running."
            st.session_state.workflow_messages.append({"role": "assistant", "content": answer})

        for message in st.session_state.workflow_messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])


# ============================================================
# PAGE 2: BUILD AN ETL
#   1. choose how to start  ->  2. give the details  ->  3. review & download
# ============================================================
elif st.session_state.page == "ETL Generation":
    st.markdown('<div class="main-title">🏗️ Build an ETL</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="subtitle">Loading into <b>{st.session_state.database_type}</b> for the <b>{st.session_state.domain_sector}</b> domain. '
        f'Change this in ⚙️ Settings.</div>', unsafe_allow_html=True)

    MODE_PICK = "🖱️ Pick from the architecture"
    MODE_DESCRIBE = "💬 Describe it in words"
    MODE_FORM = "📝 Type the columns"

    st.markdown("**1 · How do you want to start?**")
    mode = st.radio("Start", [MODE_PICK, MODE_DESCRIBE, MODE_FORM], horizontal=True, label_visibility="collapsed")
    st.session_state.etl_generation_mode = mode
    st.divider()

    # ========================================================
    # TYPE THE COLUMNS
    # ========================================================
    if mode == MODE_FORM:
        st.markdown("**2 · Tell me the source and target**")
        col1, col2 = st.columns(2)
        with col1:
            source_name = st.text_input("Source table", placeholder=f"e.g. STG_{st.session_state.domain_sector}_ACCOUNT")
            source_fields_text = st.text_area("Source columns", placeholder="ACCOUNT_ID int\nBALANCE decimal\nSTATUS varchar", height=150,
                                              help="One per line, or a full CREATE TABLE. The type is optional.")
        with col2:
            target_name = st.text_input("Target table", placeholder="e.g. HUB_ACCOUNT")
            target_fields_text = st.text_area("Target columns", placeholder="HK_ACCOUNT_ID varchar\nACCOUNT_ID int\nLOAD_DTS timestamp", height=150)

        additional_requirements = st.text_area(
            "Business rules (optional)", height=90,
            placeholder="e.g. Calculate an MD5 hash for the hub key. Filter out STATUS = 'CLOSED'.",
            help="Leave empty for an instant one-to-one mapping. Add rules and the AI designs the transformations.")
        with st.expander("Advanced: architecture layer & transformations"):
            architecture = st.multiselect("Target layer", ["Staging (STG)", "Raw Data Vault (Hub/Link/Sat)", "Business Vault (BV)", "Information Mart (iMART)"])
            transformations = st.multiselect("Transformations", PC_TRANSFORMATIONS)

        if st.button("🚀 Build pipeline", type="primary", use_container_width=True):
            domain = st.session_state.domain_sector
            _, s_parsed = parse_ddl_or_fields(source_fields_text)
            _, t_parsed = parse_ddl_or_fields(target_fields_text)
            src_nm = source_name.strip() or f"SRC_{domain}"
            tgt_nm = target_name.strip() or f"TGT_{domain}"

            if not s_parsed or not t_parsed:
                st.warning("Please enter at least one source column and one target column.")
            elif not additional_requirements.strip() and not transformations:
                # Nothing to design -> build instantly and reliably, no AI needed.
                etl = build_deterministic_etl(f"ETL_{src_nm}_TO_{tgt_nm}".upper(), "One-to-one mapping built from the columns provided.", src_nm, s_parsed, tgt_nm, t_parsed)
                etl_generator.current_etl = etl
                st.session_state.current_etl = etl
                st.session_state.etl_messages = []
                st.session_state.awaiting_ddl = False
                st.success("Pipeline built. Review it below.")
            else:
                requirement = f"""
                Create a logical ETL pipeline.
                Domain/Sector: {domain}
                Source Name: {src_nm}
                Source Fields (fixed, do not change): {[f"{f['name']} ({f['datatype']})" for f in s_parsed]}
                Architecture Layer: {", ".join(architecture)}
                Transformations to Include: {", ".join(transformations)}
                Target Name: {tgt_nm}
                Target Fields (fixed, do not change): {[f"{f['name']} ({f['datatype']})" for f in t_parsed]}
                Business Rules: {additional_requirements}
                """
                with st.spinner("The AI is designing your pipeline…"):
                    try:
                        etl = etl_generator.generate_from_requirement(requirement, st.session_state.database_type, domain)
                        if etl.get("needs_more_info"):
                            st.warning(etl.get("clarifying_question") or "I need a little more detail to build this.")
                        else:
                            # Source/target structure is what the user typed -- never let the model alter it.
                            etl["source"]["fields"], etl["target"]["fields"] = s_parsed, t_parsed
                            st.session_state.current_etl = etl
                            st.session_state.etl_messages = []
                            st.session_state.awaiting_ddl = False
                            st.success("Pipeline built. Review it below.")
                    except Exception as e:
                        st.error("The AI couldn't structure that. Try again, or clear the business rules to get an instant one-to-one mapping "
                                 f"({type(e).__name__}).")

    # ========================================================
    # DESCRIBE IT IN WORDS
    # ========================================================
    elif mode == MODE_DESCRIBE:
        st.markdown("**2 · Describe what you need**")
        st.caption("Example: paste two CREATE TABLE statements and I'll build it instantly, or describe a rule like \"join customers to accounts\".")
        
        for message in st.session_state.etl_messages:
            with st.chat_message(message["role"]): st.markdown(message["content"])

        if st.session_state.awaiting_ddl:
            st.warning("I need the columns to build this. Add them below and I'll do the rest.")
            with st.form("ddl_popup_form"):
                st.markdown("**Add the missing columns**")
                col_src, col_tgt = st.columns(2)
                with col_src:
                    src_info = st.text_area("Source Fields or DDL", height=150, placeholder="e.g. CUSTOMER_ID INT\nFIRST_NAME VARCHAR")
                with col_tgt:
                    tgt_info = st.text_area("Target Fields or DDL", height=150, placeholder="e.g. HK_CUSTOMER VARCHAR\nCUSTOMER_ID INT")
                
                if st.form_submit_button("Build pipeline", type="primary", use_container_width=True):
                    # Parsed deterministically -- no LLM round-trip here, so this
                    # can never loop back into asking for DDL again.
                    src_name, src_fields = parse_ddl_or_fields(src_info)
                    tgt_name, tgt_fields = parse_ddl_or_fields(tgt_info)

                    if src_fields and tgt_fields:
                        domain = st.session_state.domain_sector
                        src_name = src_name or f"SRC_{domain}"
                        tgt_name = tgt_name or f"TGT_{domain}"
                        pipeline_name = f"ETL_{src_name}_TO_{tgt_name}".upper()
                        etl = build_deterministic_etl(pipeline_name, "Pipeline built from user-provided DDL", src_name, src_fields, tgt_name, tgt_fields)
                        etl_generator.current_etl = etl

                        st.session_state.etl_messages.append({"role": "user", "content": f"SOURCE:\n{src_info}\n\nTARGET:\n{tgt_info}"})
                        response = f"Built the pipeline directly from what you provided — **{src_name}** ({len(src_fields)} columns) → **{tgt_name}** ({len(tgt_fields)} columns). Ask me to add transformations, or export it below."
                        st.session_state.etl_messages.append({"role": "assistant", "content": response})

                        st.session_state.current_etl = etl
                        st.session_state.awaiting_ddl = False
                        st.rerun()
                    else:
                        missing = []
                        if not src_fields: missing.append("Source")
                        if not tgt_fields: missing.append("Target")
                        st.error(f"Couldn't find any fields in: {', '.join(missing)}. Use one field per line (e.g. `CUSTOMER_ID INT`) or a full `CREATE TABLE ...` statement.")
        
        else:
            if etl_question := st.chat_input(f"E.g., 'Create a {st.session_state.domain_sector} transaction satellite mapping...'"):
                st.session_state.etl_messages.append({"role": "user", "content": etl_question})
                with st.chat_message("user"): st.markdown(etl_question)
                
                with st.chat_message("assistant"):
                    with st.spinner("Architecting..."):
                        try:
                            db_type = st.session_state.database_type
                            domain = st.session_state.domain_sector

                            # Deterministic fast-path: if this single message already
                            # contains BOTH a source and target definition (two CREATE
                            # TABLE statements, or SOURCE:/TARGET: blocks), build the
                            # pipeline directly and skip the LLM + DDL popup entirely.
                            src_block, tgt_block = split_source_target_blocks(etl_question)
                            src_name_parsed = tgt_name_parsed = None
                            src_fields = tgt_fields = []
                            if src_block and tgt_block:
                                src_name_parsed, src_fields = parse_ddl_or_fields(src_block)
                                tgt_name_parsed, tgt_fields = parse_ddl_or_fields(tgt_block)

                            if src_fields and tgt_fields:
                                src_name = src_name_parsed or f"SRC_{domain}"
                                tgt_name = tgt_name_parsed or f"TGT_{domain}"
                                pipeline_name = f"ETL_{src_name}_TO_{tgt_name}".upper()
                                etl = build_deterministic_etl(pipeline_name, etl_question.strip()[:200], src_name, src_fields, tgt_name, tgt_fields)
                                etl_generator.current_etl = etl
                                st.session_state.current_etl = etl
                                st.session_state.awaiting_ddl = False
                                response = f"Built the pipeline directly from your source/target definitions — **{src_name}** ({len(src_fields)} columns) → **{tgt_name}** ({len(tgt_fields)} columns). Ask me to add transformations, or export it below."
                            else:
                                question_for_llm = etl_question
                                if "CREATE TABLE" in etl_question.upper():
                                    question_for_llm = "Here is my DDL:\n" + clean_ddl(etl_question)
                                else:
                                    # Target couldn't be built deterministically (it likely
                                    # requires derivation, e.g. a concatenation/computed
                                    # column) -- but if we CAN pre-parse the source, hand
                                    # it to the model as fixed, guaranteed-correct
                                    # structure instead of making it re-derive names/types
                                    # it might get wrong.
                                    pre_parsed_source = extract_source_only_fields(etl_question)
                                    if pre_parsed_source:
                                        field_lines = "\n".join(f"- {f['name']} ({f['datatype']})" for f in pre_parsed_source)
                                        question_for_llm = (
                                            etl_question
                                            + "\n\n[PRE-PARSED SOURCE STRUCTURE -- use these EXACT field names "
                                            + "and datatypes for source.fields. Do not rename or invent new "
                                            + "source fields; only design target.fields and the transformation "
                                            + "components needed to satisfy the stated requirements.]\n"
                                            + field_lines
                                        )

                                if st.session_state.current_etl is None:
                                    etl = etl_generator.generate_from_requirement(question_for_llm, db_type, domain)
                                else:
                                    etl = etl_generator.modify_etl(question_for_llm, db_type, domain)

                                if etl.get("needs_more_info") or etl.get("clarifying_question"):
                                    response = etl.get("clarifying_question") or "Please provide your DDL and Source fields."
                                    st.session_state.awaiting_ddl = True
                                else:
                                    response = "I've architected the initial ETL design based on our standards."
                                    st.session_state.current_etl = etl
                                
                        except Exception as e:
                            response = (
                                "The AI had trouble structuring that request into a pipeline "
                                f"(`{type(e).__name__}`). This can happen with complex requirements "
                                "the local model struggles to format correctly.\n\n"
                                "You can either rephrase, or use the form below to give me the exact "
                                "Source and Target fields directly — that path never depends on the model "
                                "getting JSON formatting right."
                            )
                            st.session_state.awaiting_ddl = True
                    st.markdown(response)
                st.session_state.etl_messages.append({"role": "assistant", "content": response})
                
                if st.session_state.awaiting_ddl:
                    st.rerun()

    # ========================================================
    # PICK FROM THE ARCHITECTURE
    # ========================================================
    elif mode == MODE_PICK:
        SOURCE_BUTTONS = [("t24", "T24", "Core Banking"), ("card400", "CARD400", "Cards"), ("cr2", "CR2", "ATMs"), ("ivr", "IVR", "Call Center")]
        LAYER_BUTTONS = [("dv", "🗄️ Data Vault", "Hub"), ("bv", "🧮 Business Vault", "Business rules"), ("imart", "📊 Info Mart", "Fact table")]
        LAYER_LABEL = {"dv": "Data Vault", "bv": "Business Vault", "imart": "Information Mart"}

        if "viz_source" not in st.session_state: st.session_state.viz_source = None
        if "viz_layer" not in st.session_state: st.session_state.viz_layer = None

        st.markdown("**2 · Where does the data come from, and where should it go?**")
        left, right = st.columns(2)
        with left:
            st.caption("FROM (source system)")
            src_cols = st.columns(len(SOURCE_BUTTONS))
            for col, (nid, name, hint) in zip(src_cols, SOURCE_BUTTONS):
                with col:
                    if st.button(f"{name}\n{hint}", use_container_width=True,
                                 type="primary" if st.session_state.viz_source == nid else "secondary", key=f"viz_src_{nid}"):
                        st.session_state.viz_source = nid
                        st.rerun()
        with right:
            st.caption("TO (target layer)")
            layer_cols = st.columns(len(LAYER_BUTTONS))
            for col, (nid, name, hint) in zip(layer_cols, LAYER_BUTTONS):
                with col:
                    if st.button(f"{name}\n{hint}", use_container_width=True, disabled=st.session_state.viz_source is None,
                                 type="primary" if st.session_state.viz_layer == nid else "secondary", key=f"viz_layer_{nid}"):
                        st.session_state.viz_layer = nid
                        st.rerun()

        st.graphviz_chart(
            render_cib_architecture_diagram(highlight_source=st.session_state.viz_source, highlight_layer=st.session_state.viz_layer),
            use_container_width=True,
        )

        ready = bool(st.session_state.viz_source and st.session_state.viz_layer)
        if ready:
            st.success(f"**{VIZ_SOURCE_MAP[st.session_state.viz_source]}  →  {LAYER_LABEL[st.session_state.viz_layer]}**")
        else:
            st.caption("Pick a source, then a target layer. The highlighted path shows your choice.")

        default_tx_by_layer = {
            "dv": ["Source Qualifier", "Expression"],
            "bv": ["Source Qualifier", "Expression", "Aggregator"],
            "imart": ["Source Qualifier", "Joiner", "Aggregator", "Router"],
        }
        viz_transformations = []
        if ready:
            with st.expander("Advanced: change the transformations (optional)"):
                viz_transformations = st.multiselect(
                    "Transformations", PC_TRANSFORMATIONS,
                    default=default_tx_by_layer.get(st.session_state.viz_layer, []),
                    key=f"viz_tx_select_{st.session_state.viz_layer}",
                )
            if not viz_transformations:
                viz_transformations = default_tx_by_layer.get(st.session_state.viz_layer, [])

        st.markdown("**3 · Build it**")
        if st.button("🚀 Build pipeline", type="primary", use_container_width=True, disabled=not ready, key="viz_generate_btn"):
            domain = VIZ_SOURCE_MAP[st.session_state.viz_source]
            layer = st.session_state.viz_layer
            source_fields = [{"name": n, "datatype": d, "key": False, "expression": ""} for n, d, _p in DOMAIN_SOURCE_FIELDS.get(domain, [])]
            source_name = f"STG_{domain}_STAGING"
            target_name, target_fields = _target_fields_for_layer(layer, domain, source_fields)

            etl = build_deterministic_etl(
                f"ETL_{domain}_TO_{layer.upper()}",
                f"{domain} → {LAYER_LABEL[layer]} pipeline, built via the architecture picker. Requested transformations: {', '.join(viz_transformations)}.",
                source_name, source_fields, target_name, target_fields,
            )
            etl_generator.current_etl = etl
            st.session_state.current_etl = etl
            st.session_state.etl_messages = []
            st.session_state.awaiting_ddl = False
            st.success(f"Built {source_name} → {target_name}. Review it below.")

        with st.expander("What do these layers mean?"):
            st.markdown(
                "- **Source systems** — where data starts: T24 (core banking), CARD400 (cards), CR2 (ATMs), IVR (call center).\n"
                "- **Staging** — raw 1:1 copies, no business logic.\n"
                "- **Data Vault** — the historical system of record (hubs, links, satellites).\n"
                "- **Business Vault** — derived values and business rules. Runs *beside* the Info Mart, not before it.\n"
                "- **Information Mart** — star schemas ready for reporting.\n"
                "- **Business Objects** — Tableau, OFSAA and other consumers.")

    # ========================================================
    # STEP 3: REVIEW, DOWNLOAD, DEPLOY (only shown once a pipeline exists)
    # ========================================================
    current_etl = st.session_state.current_etl

    if current_etl:
        st.divider()
        head_col, reset_col = st.columns([4, 1])
        with head_col:
            st.markdown(f"### ✅ Your pipeline: `{current_etl.get('pipeline_name', 'ETL')}`")
        with reset_col:
            if st.button("🗑️ Start over", use_container_width=True):
                st.session_state.current_etl = None
                st.session_state.etl_messages = []
                st.session_state.awaiting_ddl = False
                etl_generator.reset()
                st.rerun()

        safe_name = current_etl.get("pipeline_name", "etl").replace(" ", "_")
        try:
            xml_data = generate_powercenter_xml(current_etl, database_type=st.session_state.database_type)
        except Exception as e:
            xml_data = None
            st.error(f"Couldn't create the PowerCenter XML: {e}")

        tab_preview, tab_download, tab_deploy = st.tabs(["👀 Preview", "⬇️ Download", "🚀 Deploy"])

        with tab_preview:
            diagram = render_simple_lineage(current_etl)
            if diagram:
                st.graphviz_chart(diagram, use_container_width=True)
                st.caption("Each arrow is one column. Orange arrows were matched by position because the names differ.")

        with tab_download:
            if xml_data:
                st.download_button("⬇️ Download PowerCenter XML", data=xml_data, file_name=f"{safe_name}.xml", mime="application/xml", type="primary", use_container_width=True)
                st.caption("Import this file in PowerCenter Designer.")
            with st.expander("Other formats"):
                st.download_button("Architecture docs (.md)", data=generate_markdown_docs(current_etl), file_name=f"{safe_name}_docs.md", mime="text/markdown", use_container_width=True)
                st.download_button("Raw structure (.json)", data=json.dumps(current_etl, indent=2), file_name=f"{safe_name}.json", mime="application/json", use_container_width=True)

        with tab_deploy:
            st.caption("Sends the XML straight into a PowerCenter repository. Requires `pmrep` on this machine.")
            with st.form("deploy_form"):
                col_a, col_b = st.columns(2)
                with col_a:
                    pc_domain = st.text_input("PowerCenter domain", placeholder="Domain_dev")
                    pc_repo = st.text_input("Repository", placeholder="REP_DEV")
                    target_folder = st.text_input("Folder", placeholder="my_folder")
                with col_b:
                    pc_user = st.text_input("User", placeholder="Administrator")
                    pc_pwd = st.text_input("Password", type="password")
                deploy_btn = st.form_submit_button("Deploy", use_container_width=True)

            if deploy_btn:
                if not xml_data:
                    st.error("Can't deploy: the XML couldn't be created.")
                elif not all([pc_domain, pc_repo, pc_user, pc_pwd, target_folder]):
                    st.warning("Please fill in every field.")
                else:
                    with st.spinner(f"Deploying to {pc_repo}…"):
                        success, log = deploy_local_pmrep(xml_data, pc_domain, pc_repo, pc_user, pc_pwd, target_folder)
                    if success:
                        st.success(f"Deployed to {pc_repo}/{target_folder}.")
                    else:
                        st.error("Deployment failed.")
                    st.code(log, language="bash")
