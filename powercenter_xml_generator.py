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
    connections = etl.get("connections", [])

    # ============================================================
    # FIX: AUTO-INJECT SOURCE QUALIFIER IF MISSING
    # PowerCenter requires a Source Qualifier. If the AI skipped it, build it here.
    # ============================================================
    source_name = source.get("name", "SRC")
    has_sq = any((c.get("type") or "").upper() == "SOURCE_QUALIFIER" for c in all_components)
    
    if not has_sq and source.get("fields"):
        sq_id = f"auto_sq_{source_name}"
        sq_comp = {
            "id": sq_id,
            "name": f"SQ_{source_name}",
            "type": "SOURCE_QUALIFIER",
            "description": "Auto-injected Source Qualifier",
            "fields": [{"name": _field_name(f), "datatype": _field_datatype(f), "expression": ""} for f in source.get("fields", [])]
        }
        all_components.insert(0, sq_comp)
        
        # Rewire connections: anything coming from SOURCE now comes from the SQ
        for conn in connections:
            if conn.get("from") == source_name:
                conn["from"] = sq_id
                
        # Connect SOURCE to the new SQ
        connections.insert(0, {"from": source_name, "to": sq_id, "label": ""})
    # ============================================================

    ordered_ids, component_map = compute_component_order(all_components, connections)

    sid = next((c for c in ordered_ids if (component_map[c].get("type") or "").upper() == "SOURCE"), None)
    tid = next((c for c in ordered_ids if (component_map[c].get("type") or "").upper() == "TARGET"), None)

    s_inst = component_map[sid].get("name") if sid else source.get("name", "SRC")
    t_inst = component_map[tid].get("name") if tid else target.get("name", "TGT")
    s_fields = component_map[sid].get("fields") if sid and component_map[sid].get("fields") else source.get("fields", [])
    t_fields = component_map[tid].get("fields") if tid and component_map[tid].get("fields") else target.get("fields", [])

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
        f'DESCRIPTION="{_esc(src_desc)}" NAME="{_esc(s_inst)}" OBJECTVERSION="1" '
        f'OWNERNAME="{_esc(src_owner)}" VERSIONNUMBER="1">\n'
        f'{def_fields_xml("SOURCEFIELD", s_fields)}\n'
        f'      </SOURCE>'
    )

    tgt_xml = (
        f'      <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="{_esc(database_type)}" '
        f'DESCRIPTION="{_esc(tgt_desc)}" NAME="{_esc(t_inst)}" OBJECTVERSION="1" '
        f'TABLEOPTIONS="" VERSIONNUMBER="1">\n'
        f'{def_fields_xml("TARGETFIELD", t_fields)}\n'
        f'      </TARGET>'
    )

    outputs_by_id, inputs_by_id, inst_name_by_id, inst_type_by_id = {}, {}, {}, {}

    outputs_by_id["__source__"] = {}
    for f in s_fields:
        if name := _field_name(f):
            outputs_by_id["__source__"][name] = {"datatype": _field_datatype(f), "precision": _precision_scale(f)[0], "scale": _precision_scale(f)[1]}
            
    s_order = [n for f in s_fields if (n := _field_name(f))]

    tx_blocks = []
    inst_blocks = [f'        <INSTANCE DESCRIPTION="" NAME="{_esc(s_inst)}" TRANSFORMATION_NAME="{_esc(s_inst)}" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>']
    conn_blocks = []

    def upstream_ids_for(cid):
        ups = [c.get("from") for c in connections if c.get("to") == cid]
        return [u for u in ups if u in outputs_by_id or u in inst_name_by_id]

    prev_id = "__source__"
    tx_ids = [c for c in ordered_ids if c not in (sid, tid)]

    for cid in tx_ids:
        comp = component_map[cid]
        cname = (comp.get("name") or cid).replace(" ", "_")
        pct = _pc_type(comp.get("type"))
        
        inst_name_by_id[cid] = cname
        inst_type_by_id[cid] = pct
        
        decl = {_field_name(f): f for f in comp.get("fields", []) if isinstance(f, dict) and _field_name(f)}
        
        inc_names, seen, inc_types = [], set(), {}
        for up in upstream_ids_for(cid) or [prev_id]:
            for n in out_by_id.get(up, {}).keys() if 'out_by_id' in locals() else outputs_by_id.get(up, {}).keys():
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
        if cond and comp.get("type", "").upper() in CONDITION_ATTRIBUTE_NAME:
            attr_rows.append(f'          <TABLEATTRIBUTE NAME="{CONDITION_ATTRIBUTE_NAME[comp.get("type").upper()]}" VALUE="{_esc(cond)}"/>')

        tx_desc = comp.get("description", "")
        tx_blocks.append(
            f'        <TRANSFORMATION DESCRIPTION="{_esc(tx_desc)}" NAME="{_esc(cname)}" OBJECTVERSION="1" REUSABLE="NO" TYPE="{_esc(pct)}" VERSIONNUMBER="1">\n'
            + "\n".join(port_rows) + ("\n" + "\n".join(attr_rows) if attr_rows else "") +
            f'\n        </TRANSFORMATION>'
        )

        inst_blocks.append(f'        <INSTANCE DESCRIPTION="" NAME="{_esc(cname)}" REUSABLE="NO" TRANSFORMATION_NAME="{_esc(cname)}" TRANSFORMATION_TYPE="{_esc(pct)}" TYPE="TRANSFORMATION"/>')
        prev_id = cid

    inst_blocks.append(f'        <INSTANCE DESCRIPTION="" NAME="{_esc(t_inst)}" TRANSFORMATION_NAME="{_esc(t_inst)}" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>')

    def connect(f_inst, f_type, f_flds, t_inst, t_type, t_flds):
        t_set = set(t_flds)
        for n in f_flds:
            if n in t_set:
                conn_blocks.append(f'        <CONNECTOR FROMFIELD="{_esc(n)}" FROMINSTANCE="{_esc(f_inst)}" FROMINSTANCETYPE="{_esc(f_type)}" TOFIELD="{_esc(n)}" TOINSTANCE="{_esc(t_inst)}" TOINSTANCETYPE="{_esc(t_type)}"/>')

    if tx_ids:
        for cid in tx_ids:
            ups = upstream_ids_for(cid) or (["__source__"] if cid == tx_ids[0] else [tx_ids[tx_ids.index(cid) - 1]])
            for up in ups:
                f_i = s_inst if up == "__source__" else inst_name_by_id[up]
                f_t = "Source Definition" if up == "__source__" else inst_type_by_id[up]
                f_f = s_order if up == "__source__" else list(outputs_by_id[up].keys())
                connect(f_i, f_t, f_f, inst_name_by_id[cid], inst_type_by_id[cid], inputs_by_id[cid])
                
        last_id = tx_ids[-1]
        connect(inst_name_by_id[last_id], inst_type_by_id[last_id], list(outputs_by_id[last_id].keys()), t_inst, "Target Definition", [n for f in t_fields if (n := _field_name(f))])
    else:
        connect(s_inst, "Source Definition", s_order, t_inst, "Target Definition", [n for f in t_fields if (n := _field_name(f))])

    mapping_body = "\n\n".join(tx_blocks) + ("\n\n" if tx_blocks else "\n") + "\n".join(inst_blocks) + "\n\n" + "\n".join(conn_blocks)
    safe_pipe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in pipeline_name).upper()
    creation_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{creation_date}" REPOSITORY_VERSION="189.98">
  <REPOSITORY NAME="{_esc(repository_name)}" VERSION="189" CODEPAGE="UTF-8" DATABASETYPE="{_esc(database_type)}">
    <FOLDER NAME="{_esc(safe_pipe)}" GROUP="" OWNER="{_esc(folder_owner)}" SHARED="NOTSHARED" DESCRIPTION="{_esc(description)}" PERMISSIONS="rwx---r--">
{src_xml}

{tgt_xml}

      <MAPPING DESCRIPTION="{_esc(description)}" ISVALID="YES" NAME="m_{safe_pipe}" OBJECTVERSION="1" VERSIONNUMBER="1">
{mapping_body}
        <TARGETLOADORDER ORDER="1" TARGETINSTANCE="{_esc(t_inst)}"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
"""