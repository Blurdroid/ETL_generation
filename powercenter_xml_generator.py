"""
Converts an ETL Generator pipeline into an Informatica PowerCenter-importable 
XML mapping (POWERMART format).
"""

from datetime import datetime
from xml.sax.saxutils import quoteattr

# ============================================================
# DATATYPE MAPPING
# ============================================================
DATATYPE_MAP = {
    "Oracle": {"string": "varchar2", "integer": "number", "decimal": "number", "boolean": "number", "date": "date", "timestamp": "timestamp"},
    "Microsoft SQL Server": {"string": "varchar", "integer": "int", "decimal": "decimal", "boolean": "bit", "date": "date", "timestamp": "datetime2"},
    "Generic/ANSI": {"string": "varchar", "integer": "integer", "decimal": "decimal", "boolean": "boolean", "date": "date", "timestamp": "timestamp"},
}

DEFAULT_PRECISION = {"string": (100, 0), "integer": (15, 0), "decimal": (18, 2), "boolean": (1, 0), "date": (19, 0), "timestamp": (26, 6)}

TRANSFORMATION_TYPE_MAP = {
    "SOURCE_QUALIFIER": "Source Qualifier", "UPDATE_STRATEGY": "Update Strategy", "SEQUENCE_GENERATOR": "Sequence Generator",
    "DATA_VAULT": "Expression", "DATA_VAULT_HUB": "Expression", "DATA_VAULT_LINK": "Expression", "DATA_VAULT_SATELLITE": "Expression", "BUSINESS_VAULT": "Expression"
}

CONDITION_ATTRIBUTE_NAME = {
    "FILTER": "Filter Condition", "JOINER": "Join Condition", "ROUTER": "Group Filter Condition", "LOOKUP": "Lookup Condition", "UPDATE_STRATEGY": "Update Strategy Expression"
}

def _esc(value): return quoteattr(str(value if value is not None else ""))[1:-1]
def _pc_type(comp_type): return TRANSFORMATION_TYPE_MAP.get((comp_type or "").strip().upper(), (comp_type or "").strip().upper().replace("_", " ").title())
def _field_name(f): return f.get("name") if isinstance(f, dict) else f
def _field_datatype(f): return f.get("datatype", "string") if isinstance(f, dict) else "string"

def _precision_scale(f_info, def_dtype="string"):
    if not isinstance(f_info, dict): f_info = {}
    dtype = (f_info.get("datatype") or def_dtype).strip().lower()
    p, s = DEFAULT_PRECISION.get(dtype, (100, 0))
    return int(f_info.get("precision", f_info.get("length", p))), int(f_info.get("scale", s))

def _tx_dtype(nt, p, s):
    nt = (nt or "string").strip().lower()
    if nt == "integer": return "integer", 10, 0
    if nt == "decimal": return "decimal", int(p), int(s)
    if nt in ("date", "timestamp"): return "date/time", 29, 9
    return "string", max(1, int(p)), 0

def _get_phys_length(db, pt, log_p):
    db, pt = (db or "").lower(), (pt or "").lower()
    if "oracle" in db and pt == "number": return 22
    if "oracle" in db and pt in ("date", "timestamp"): return 19
    if "sql server" in db and pt == "int": return 4
    if "sql server" in db and pt == "decimal": return 17
    return log_p

def _physical_datatype(neutral_type, database_type):
    neutral_type = (neutral_type or "string").strip().lower()
    return DATATYPE_MAP.get(database_type, DATATYPE_MAP["Generic/ANSI"]).get(neutral_type, "varchar")

def compute_component_order(components, connections):
    cmap = {c["id"]: c for c in components if c.get("id")}
    inc, outg = {cid: 0 for cid in cmap}, {cid: [] for cid in cmap}
    for conn in connections:
        if conn.get("from") in cmap and conn.get("to") in cmap:
            inc[conn["to"]] += 1
            outg[conn["from"]].append(conn["to"])
    queue = [cid for cid, count in inc.items() if count == 0]
    ordered, visited = [], set()
    while queue:
        curr = queue.pop(0)
        if curr in visited: continue
        visited.add(curr)
        ordered.append(curr)
        for nxt in outg.get(curr, []):
            inc[nxt] -= 1
            if inc[nxt] == 0: queue.append(nxt)
    return ordered + [cid for cid in cmap if cid not in ordered], cmap

def generate_powercenter_xml(etl, database_type="Oracle", repository_name="ETL_REPOSITORY", folder_owner="Administrator"):
    pipeline_name = etl.get("pipeline_name", "GENERATED_ETL")
    description = etl.get("description", "")
    source = etl.get("source", {})
    target = etl.get("target", {})
    all_components = etl.get("components", [])
    raw_connections = etl.get("connections", [])

    # 1. Normalize IDs and Names to prevent disconnects
    source["id"] = source.get("id", source.get("name", "SRC"))
    target["id"] = target.get("id", target.get("name", "TGT"))
    for c in all_components:
        c["id"] = c.get("id", c.get("name"))
        
    all_nodes = [source] + all_components + [target]

    def resolve_ref(ref):
        for n in all_nodes:
            if n.get("id") == ref or n.get("name") == ref:
                return n["id"]
        return ref

    connections = []
    for conn in raw_connections:
        connections.append({"from": resolve_ref(conn.get("from")), "to": resolve_ref(conn.get("to"))})

    ordered_ids, component_map = compute_component_order(all_components, connections)

    source_instance = source.get("name", "SRC")
    target_instance = target.get("name", "TGT")
    s_fields = source.get("fields", [])
    t_fields = target.get("fields", [])

    def def_fields_xml(tag, fields):
        rows = []
        for i, f in enumerate(fields, start=1):
            name = _field_name(f)
            if not name: continue
            dtype = _field_datatype(f)
            is_key = bool(f.get("key")) if isinstance(f, dict) else False
            p, s = _precision_scale(f, dtype)
            pt = _physical_datatype(dtype, database_type)
            pl = _get_phys_length(database_type, pt, p)
            ka = ' KEYTYPE="PRIMARY KEY"' if is_key else ' KEYTYPE="NOT A KEY"'
            na = ' NULLABLE="NOTNULL"' if is_key else ' NULLABLE="NULL"'

            if tag == "SOURCEFIELD":
                rows.append(f'        <{tag} BUSINESSNAME="" DATATYPE="{pt}" DESCRIPTION="" FIELDNUMBER="{i}" FIELDPROPERTY="0" FIELDTYPE="ELEMITEM" HIDDEN="NO"{ka} LENGTH="{p}" LEVEL="0" NAME="{_esc(name)}"{na} OCCURS="0" OFFSET="0" PHYSICALLENGTH="{pl}" PHYSICALOFFSET="0" PICTURETEXT="" PRECISION="{p}" SCALE="{s}" USAGE_FLAGS=""/>')
            else:
                rows.append(f'        <{tag} BUSINESSNAME="" DATATYPE="{pt}" DESCRIPTION="" FIELDNUMBER="{i}"{ka} NAME="{_esc(name)}"{na} PICTURETEXT="" PRECISION="{p}" SCALE="{s}"/>')
        return "\n".join(rows)

    src_owner = source.get("owner", "dbo")
    src_desc = source.get("description", description)
    tgt_desc = target.get("description", "")

    src_xml = (
        f'      <SOURCE BUSINESSNAME="" DATABASETYPE="{_esc(database_type)}" DBDNAME="default" '
        f'DESCRIPTION="{_esc(src_desc)}" NAME="{_esc(source_instance)}" OBJECTVERSION="1" '
        f'OWNERNAME="{_esc(src_owner)}" VERSIONNUMBER="1">\n'
        f'{def_fields_xml("SOURCEFIELD", s_fields)}\n'
        f'      </SOURCE>'
    )

    tgt_xml = (
        f'      <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="{_esc(database_type)}" '
        f'DESCRIPTION="{_esc(tgt_desc)}" NAME="{_esc(target_instance)}" OBJECTVERSION="1" '
        f'TABLEOPTIONS="" VERSIONNUMBER="1">\n'
        f'{def_fields_xml("TARGETFIELD", t_fields)}\n'
        f'      </TARGET>'
    )

    outputs_by_id = {}
    inputs_by_id = {}
    inst_name_by_id = {}
    inst_type_by_id = {}

    inst_name_by_id[source["id"]] = source_instance
    inst_type_by_id[source["id"]] = "Source Definition"
    outputs_by_id[source["id"]] = {}
    for f in s_fields:
        if name := _field_name(f):
            outputs_by_id[source["id"]][name] = {"datatype": _field_datatype(f), "precision": _precision_scale(f)[0], "scale": _precision_scale(f)[1]}
            
    inst_name_by_id[target["id"]] = target_instance
    inst_type_by_id[target["id"]] = "Target Definition"
    inputs_by_id[target["id"]] = [n for f in t_fields if (n := _field_name(f))]

    tx_blocks = []
    
    # CRITICAL FIX: DBDNAME="default" added to the Source INSTANCE
    inst_blocks = [f'        <INSTANCE DBDNAME="default" DESCRIPTION="" NAME="{_esc(source_instance)}" TRANSFORMATION_NAME="{_esc(source_instance)}" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>']
    
    def upstream_ids_for(cid):
        return [c.get("from") for c in connections if c.get("to") == cid and c.get("from") in outputs_by_id]

    for cid in ordered_ids:
        comp = component_map[cid]
        cname = (comp.get("name") or cid).replace(" ", "_")
        pct = _pc_type(comp.get("type"))
        
        inst_name_by_id[cid] = cname
        inst_type_by_id[cid] = pct
        
        decl = {_field_name(f): f for f in comp.get("fields", []) if isinstance(f, dict) and _field_name(f)}
        
        inc_names, seen, inc_types = [], set(), {}
        for up in upstream_ids_for(cid):
            for n in outputs_by_id.get(up, {}).keys():
                if n not in seen:
                    seen.add(n)
                    inc_names.append(n)
            inc_types.update(outputs_by_id.get(up, {}))
            
        all_names = list(inc_names)
        for n in [n for f in comp.get("fields", []) if (n := _field_name(f))]:
            if n not in seen:
                all_names.append(n)
                seen.add(n)

        port_rows = []
        outputs_by_id[cid] = {}
        inputs_by_id[cid] = inc_names

        for name in all_names:
            info = decl.get(name, {})
            expr = info.get("expression")
            dtype = info.get("datatype", inc_types.get(name, {}).get("datatype", "string"))
            
            p, s = _precision_scale(info, dtype)
            if not ("precision" in info or "length" in info): p = inc_types.get(name, {}).get("precision", p)
            if "scale" not in info: s = inc_types.get(name, {}).get("scale", s)
            
            tdt, tp, ts = _tx_dtype(dtype, p, s)
            outputs_by_id[cid][name] = {"datatype": dtype, "precision": p, "scale": s}
            
            ptype = "INPUT/OUTPUT" if name in inc_names else "OUTPUT"
            ref_attr = f' REF_FIELD="{_esc(name)}"' if pct in ("Source Qualifier", "Router") else ''
            expr_attr = ' EXPRESSIONTYPE="GENERAL"' if expr else ''
            expr_val = expr if expr else name
            
            port_rows.append(f'          <TRANSFORMFIELD DATATYPE="{tdt}" DEFAULTVALUE="" DESCRIPTION="" EXPRESSION="{_esc(expr_val)}"{expr_attr} NAME="{_esc(name)}" PICTURETEXT="" PORTTYPE="{ptype}" PRECISION="{tp}" SCALE="{ts}"{ref_attr}/>')

        attr_rows = []
        cond = comp.get("condition")
        c_type = comp.get("type", "").upper()
        if cond and c_type in CONDITION_ATTRIBUTE_NAME:
            attr_rows.append(f'          <TABLEATTRIBUTE NAME="{CONDITION_ATTRIBUTE_NAME[c_type]}" VALUE="{_esc(cond)}"/>')

        tx_desc = comp.get("description", "")
        tx_blocks.append(
            f'        <TRANSFORMATION DESCRIPTION="{_esc(tx_desc)}" NAME="{_esc(cname)}" OBJECTVERSION="1" REUSABLE="NO" TYPE="{_esc(pct)}" VERSIONNUMBER="1">\n'
            + "\n".join(port_rows) + ("\n" + "\n".join(attr_rows) if attr_rows else "") +
            f'\n        </TRANSFORMATION>'
        )

        # CRITICAL FIX: Ensure Source Qualifier instances strictly include the ASSOCIATED_SOURCE_INSTANCE tag
        if pct == "Source Qualifier":
            inst_blocks.append(
                f'        <INSTANCE DESCRIPTION="" NAME="{_esc(cname)}" REUSABLE="NO" TRANSFORMATION_NAME="{_esc(cname)}" TRANSFORMATION_TYPE="{_esc(pct)}" TYPE="TRANSFORMATION">\n'
                f'            <ASSOCIATED_SOURCE_INSTANCE NAME="{_esc(source_instance)}"/>\n'
                f'        </INSTANCE>'
            )
        else:
            inst_blocks.append(f'        <INSTANCE DESCRIPTION="" NAME="{_esc(cname)}" REUSABLE="NO" TRANSFORMATION_NAME="{_esc(cname)}" TRANSFORMATION_TYPE="{_esc(pct)}" TYPE="TRANSFORMATION"/>')

    inst_blocks.append(f'        <INSTANCE DESCRIPTION="" NAME="{_esc(target_instance)}" TRANSFORMATION_NAME="{_esc(target_instance)}" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>')

    conn_blocks = []
    for conn in connections:
        frm = conn.get("from")
        to = conn.get("to")
        
        if frm in inst_name_by_id and to in inst_name_by_id:
            f_inst = inst_name_by_id[frm]
            f_type = inst_type_by_id[frm]
            f_flds = list(outputs_by_id[frm].keys())
            
            t_inst = inst_name_by_id[to]
            t_type = inst_type_by_id[to]
            t_flds = inputs_by_id[to]
            
            t_set = set(t_flds)
            for n in f_flds:
                if n in t_set:
                    conn_blocks.append(f'        <CONNECTOR FROMFIELD="{_esc(n)}" FROMINSTANCE="{_esc(f_inst)}" FROMINSTANCETYPE="{_esc(f_type)}" TOFIELD="{_esc(n)}" TOINSTANCE="{_esc(t_inst)}" TOINSTANCETYPE="{_esc(t_type)}"/>')

    mapping_body = "\n\n".join(tx_blocks)
    if tx_blocks:
        mapping_body += "\n\n"
    mapping_body += "\n".join(inst_blocks)
    mapping_body += "\n\n"
    mapping_body += "\n".join(conn_blocks)
    
    safe_pipe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in pipeline_name).upper()
    creation_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{creation_date}" REPOSITORY_VERSION="189.98">
  <REPOSITORY CODEPAGE="UTF-8" DATABASETYPE="{_esc(database_type)}" NAME="{_esc(repository_name)}" VERSION="189">
    <FOLDER DESCRIPTION="{_esc(description)}" GROUP="" NAME="{_esc(safe_pipe)}" OWNER="{_esc(folder_owner)}" PERMISSIONS="rwx---r--" SHARED="NOTSHARED">
{src_xml}

{tgt_xml}

      <MAPPING DESCRIPTION="{_esc(description)}" ISVALID="YES" NAME="m_{safe_pipe}" OBJECTVERSION="1" VERSIONNUMBER="1">
{mapping_body}
        <TARGETLOADORDER ORDER="1" TARGETINSTANCE="{_esc(target_instance)}"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""
