"""
gap_analysis.py — RSD Gap Analysis Engine

Reads:
  - column_lineage.csv / table_lineage.csv (PowerCenter extracted lineage)
  - An RSD (Requirements Specification Document) — Excel or free text

Produces:
  - gap_result dict: existing items (blue) + missing items (red)
  - DDL for missing tables/columns
  - HTML lineage path strings for existing items
"""

import csv
import io
import re
from collections import defaultdict
from functools import lru_cache

# ─────────────────────────────────────────────────────────────────────────────
# CSV column indices (0-based, after stripping BOM from header)
# column_lineage: folder,workflow,worklet,session,mapping,src_kind,src_table,
#   src_canon,src_col,src_expr,tgt_table,tgt_canon,tgt_col,rel_type,
#   sql_derived,confidence,n_paths,target_instance,path_text,expressions,xml_file
# table_lineage:  folder,workflow,worklet,session,mapping,src_table,src_canon,
#   tgt_table,tgt_canon,rel_types,confidence,n_columns,sql_derived,xml_file
# ─────────────────────────────────────────────────────────────────────────────

COL_LIN_HEADER = [
    "folder","workflow","worklet","session","mapping","src_kind",
    "src_table","src_canon","src_col","src_expr","tgt_table","tgt_canon",
    "tgt_col","rel_type","sql_derived","confidence","n_paths",
    "target_instance","path_text","expressions","xml_file"
]
TBL_LIN_HEADER = [
    "folder","workflow","worklet","session","mapping","src_table","src_canon",
    "tgt_table","tgt_canon","rel_types","confidence","n_columns","sql_derived","xml_file"
]


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def _normalize_table(name: str) -> str:
    """Upper-case, strip schema prefix for matching."""
    if not name:
        return ""
    return name.strip().upper()


def _normalize_col(name: str) -> str:
    return name.strip().upper()


# ─────────────────────────────────────────────────────────────────────────────
# LINEAGE LOADING
# ─────────────────────────────────────────────────────────────────────────────

class LineageIndex:
    """
    Builds fast-lookup indexes from the two CSVs.

    Indexes built:
      table_targets   : {canon_src_table → list of (canon_tgt_table, mapping, folder)}
      table_sources   : {canon_tgt_table → list of (canon_src_table, mapping, folder)}
      col_targets     : {(canon_tgt_table, canon_col) → list of (canon_src_table, src_col, mapping)}
      col_sources     : {(canon_src_table, canon_col) → list of (canon_tgt_table, tgt_col, mapping)}
      all_tables      : set of all known canon table names
      all_cols        : {canon_table → set of canon column names}
      table_domain    : {canon_table → folder}
      table_mappings  : {canon_table → set of mapping names}
    """

    def __init__(self):
        self.table_targets   = defaultdict(list)
        self.table_sources   = defaultdict(list)
        self.col_targets     = defaultdict(list)
        self.col_sources     = defaultdict(list)
        self.all_tables      = set()
        self.all_cols        = defaultdict(set)
        self.table_domain    = {}
        self.table_mappings  = defaultdict(set)
        self._loaded_tbl     = False
        self._loaded_col     = False

    # ------------------------------------------------------------------
    def load_table_lineage(self, path: str):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            raw_hdr = [_strip_bom(h) for h in next(reader)]
            idx = {h: i for i, h in enumerate(raw_hdr)}
            i_src  = idx.get("src_canon", idx.get("src_table", 5))
            i_tgt  = idx.get("tgt_canon", idx.get("tgt_table", 7))
            i_fold = idx.get("folder", 0)
            i_map  = idx.get("mapping", 4)

            for row in reader:
                if len(row) <= max(i_src, i_tgt):
                    continue
                src = _normalize_table(row[i_src])
                tgt = _normalize_table(row[i_tgt])
                fld = row[i_fold].strip() if i_fold < len(row) else ""
                mp  = row[i_map].strip()  if i_map  < len(row) else ""
                if src:
                    self.all_tables.add(src)
                    self.table_domain.setdefault(src, fld)
                    self.table_mappings[src].add(mp)
                if tgt:
                    self.all_tables.add(tgt)
                    self.table_domain.setdefault(tgt, fld)
                    self.table_mappings[tgt].add(mp)
                if src and tgt:
                    self.table_targets[src].append((tgt, mp, fld))
                    self.table_sources[tgt].append((src, mp, fld))
        self._loaded_tbl = True

    def load_column_lineage(self, path: str):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            raw_hdr = [_strip_bom(h) for h in next(reader)]
            idx = {h: i for i, h in enumerate(raw_hdr)}
            i_stbl = idx.get("src_canon", idx.get("src_table", 7))
            i_scol = idx.get("src_col", 8)
            i_ttbl = idx.get("tgt_canon", idx.get("tgt_table", 10))
            i_tcol = idx.get("tgt_col", 12)
            i_map  = idx.get("mapping", 4)
            i_fold = idx.get("folder", 0)

            for row in reader:
                if len(row) <= max(i_stbl, i_ttbl, i_scol, i_tcol):
                    continue
                src_t = _normalize_table(row[i_stbl])
                src_c = _normalize_col(row[i_scol])
                tgt_t = _normalize_table(row[i_ttbl])
                tgt_c = _normalize_col(row[i_tcol])
                mp    = row[i_map].strip() if i_map < len(row) else ""
                fld   = row[i_fold].strip() if i_fold < len(row) else ""

                if src_t:
                    self.all_tables.add(src_t)
                if tgt_t:
                    self.all_tables.add(tgt_t)
                if src_t and src_c:
                    self.all_cols[src_t].add(src_c)
                if tgt_t and tgt_c:
                    self.all_cols[tgt_t].add(tgt_c)
                if src_t and tgt_t and src_c and tgt_c:
                    self.col_targets[(src_t, src_c)].append((tgt_t, tgt_c, mp))
                    self.col_sources[(tgt_t, tgt_c)].append((src_t, src_c, mp))
        self._loaded_col = True

    # ------------------------------------------------------------------
    def find_table(self, name: str):
        """Exact match on normalized name, then partial match."""
        n = _normalize_table(name)
        if n in self.all_tables:
            return n
        # Try matching just the table name part (strip schema prefix)
        bare = n.split(".")[-1]
        for t in self.all_tables:
            if t.split(".")[-1] == bare:
                return t
        return None

    def find_col(self, table_norm: str, col: str):
        col_n = _normalize_col(col)
        if col_n in self.all_cols.get(table_norm, set()):
            return col_n
        return None

    def lineage_path_for_table(self, table_norm: str, max_hops=2) -> str:
        """Return a short lineage summary string: src → src → table → tgt."""
        parts = []
        # upstream (up to max_hops)
        up_chain = []
        cur = table_norm
        for _ in range(max_hops):
            srcs = self.table_sources.get(cur, [])
            if not srcs:
                break
            up_chain.append(srcs[0][0])
            cur = srcs[0][0]
        up_chain.reverse()
        parts = up_chain + [table_norm]
        # downstream (1 hop)
        dsts = self.table_targets.get(table_norm, [])
        if dsts:
            parts.append(dsts[0][0])
        return " → ".join(parts) if parts else table_norm

    def lineage_path_for_col(self, table_norm: str, col_norm: str) -> str:
        srcs = self.col_sources.get((table_norm, col_norm), [])
        if srcs:
            return f"{srcs[0][0]}.{srcs[0][1]} → {table_norm}.{col_norm}"
        return f"{table_norm}.{col_norm}"


# ─────────────────────────────────────────────────────────────────────────────
# RSD PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_rsd_excel(file_bytes: bytes) -> list[dict]:
    """
    Parse an Excel RSD. Looks for columns named:
      Target_Table / table / table_name
      Column_Name  / column / field
      DataType     / data_type / type
      Description  / desc
      Business_Rule / rule
    Returns list of {table, column, datatype, description, business_rule}
    """
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    results = []
    for ws in wb.worksheets:
        rows = list(ws.values)
        if not rows:
            continue
        hdr_idx = 0
        for i, row in enumerate(rows):
            if any(cell is not None for cell in row):
                hdr_idx = i
                break
        hdr = [str(c).strip().lower().replace(" ", "_") if c else "" for c in rows[hdr_idx]]

        def _col(aliases):
            for a in aliases:
                if a in hdr:
                    return hdr.index(a)
            return None

        i_tbl  = _col(["target_table","table","table_name","tbl","target"])
        i_col  = _col(["column_name","column","field","col","attribute"])
        i_dt   = _col(["datatype","data_type","type","dtype"])
        i_desc = _col(["description","desc","definition","business_definition"])
        i_rule = _col(["business_rule","rule","transformation","transform","logic"])

        if i_tbl is None and i_col is None:
            continue

        for row in rows[hdr_idx + 1:]:
            def _v(i):
                if i is None or i >= len(row): return ""
                v = row[i]
                return str(v).strip() if v is not None else ""
            tbl  = _v(i_tbl)
            col  = _v(i_col)
            dt   = _v(i_dt) or "varchar"
            desc = _v(i_desc)
            rule = _v(i_rule)
            if not tbl and not col: continue
            results.append({"table": tbl.upper() if tbl else "",
                             "column": col.upper() if col else "",
                             "datatype": dt, "description": desc, "business_rule": rule})
    return results


def parse_rsd_docx(file_bytes: bytes) -> list[dict]:
    """
    Parse a Word (.docx) RSD document.

    Strategy (in priority order):
    1. Find Word tables where headers contain table/column keywords.
    2. Scan paragraph text for inline table definitions like:
         TABLE_NAME (col1 type, col2 type)  or  TABLE_NAME.COL type
    3. Detect heading-based sections: a heading names a table,
       following bullet points list columns.
    """
    from docx import Document

    doc = Document(io.BytesIO(file_bytes))
    results = []

    # ── Strategy 1: Word tables ──────────────────────────────────────────────
    for tbl in doc.tables:
        if not tbl.rows:
            continue
        # Normalise headers
        hdr_cells = [c.text.strip().lower().replace(" ", "_") for c in tbl.rows[0].cells]

        def _ci(aliases):
            for a in aliases:
                if a in hdr_cells: return hdr_cells.index(a)
            return None

        i_tbl  = _ci(["target_table","table","table_name","tbl","target_table_name"])
        i_col  = _ci(["column_name","column","field","col","attribute","column/field"])
        i_dt   = _ci(["datatype","data_type","type","dtype","data_type/format"])
        i_desc = _ci(["description","desc","definition","business_definition","business_description"])
        i_rule = _ci(["business_rule","rule","transformation","logic","business_rule/logic"])

        if i_tbl is None and i_col is None:
            continue  # Not a column requirements table

        for row in tbl.rows[1:]:
            cells = [c.text.strip() for c in row.cells]
            def _cv(i):
                if i is None or i >= len(cells): return ""
                return cells[i].strip()
            tbl_v = _cv(i_tbl)
            col_v = _cv(i_col)
            dt_v  = _cv(i_dt) or "varchar"
            desc_v= _cv(i_desc)
            rule_v= _cv(i_rule)
            if not tbl_v and not col_v: continue
            results.append({"table": tbl_v.upper() if tbl_v else "",
                             "column": col_v.upper() if col_v else "",
                             "datatype": dt_v, "description": desc_v, "business_rule": rule_v})

    if results:
        return results

    # ── Strategy 2: Full text extraction → same as parse_rsd_text ───────────
    full_text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    # Also add text from tables not already matched
    for tbl in doc.tables:
        for row in tbl.rows:
            full_text += "\n" + " ".join(c.text.strip() for c in row.cells)

    return parse_rsd_text(full_text)


def parse_rsd_text(text: str) -> list[dict]:
    """
    Parse free-text RSD. Supports formats like:
      TABLE_NAME (col1 type, col2 type, ...)
      or one col per line: TABLE.COL datatype
    """
    results = []
    current_table = ""

    # Pattern 1: TABLE (col type, col type)
    for m in re.finditer(
        r"(?P<tbl>[A-Z_][A-Z0-9_\.]{2,})\s*\((?P<cols>[^)]+)\)",
        text, re.IGNORECASE
    ):
        tbl = m.group("tbl").upper()
        for part in m.group("cols").split(","):
            tokens = part.strip().split()
            if len(tokens) >= 2:
                results.append({"table": tbl, "column": tokens[0].upper(),
                                 "datatype": tokens[1], "description": "", "business_rule": ""})
            elif len(tokens) == 1:
                results.append({"table": tbl, "column": tokens[0].upper(),
                                 "datatype": "varchar", "description": "", "business_rule": ""})
    if results:
        return results

    # Pattern 2: TABLE.COLUMN datatype  (one per line)
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if re.match(r"[A-Z_][A-Z0-9_]{2,}\s*$", line, re.IGNORECASE):
            current_table = line.upper()
            continue
        m = re.match(r"([A-Z_][A-Z0-9_\.]+)\.([A-Z_][A-Z0-9_]+)\s+(\w+)", line, re.IGNORECASE)
        if m:
            results.append({"table": m.group(1).upper(), "column": m.group(2).upper(),
                             "datatype": m.group(3), "description": "", "business_rule": ""})
            continue
        m2 = re.match(r"([A-Z_][A-Z0-9_]+)\s+(\w+)", line, re.IGNORECASE)
        if m2 and current_table:
            results.append({"table": current_table, "column": m2.group(1).upper(),
                             "datatype": m2.group(2), "description": "", "business_rule": ""})
    return results


# ─────────────────────────────────────────────────────────────────────────────
# GAP ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def run_gap_analysis(rsd_items: list[dict], index: LineageIndex) -> dict:
    """
    Compare RSD requirements against the lineage index.

    Returns:
    {
        "tables": {
            "TABLE_NAME": {
                "status": "exists" | "missing",
                "lineage_path": "...",
                "domain": "...",
                "mappings": [...],
                "columns": {
                    "COL_NAME": {
                        "status": "exists" | "missing",
                        "datatype": "...",
                        "description": "...",
                        "business_rule": "...",
                        "lineage_path": "...",
                    }
                }
            }
        },
        "summary": {"total_tables": N, "existing_tables": N, "missing_tables": N,
                    "total_cols": N, "existing_cols": N, "missing_cols": N}
    }
    """
    result = {}

    for item in rsd_items:
        tbl  = item["table"]
        col  = item["column"]
        dt   = item["datatype"]
        desc = item["description"]
        rule = item["business_rule"]

        if not tbl:
            continue

        if tbl not in result:
            found_tbl = index.find_table(tbl)
            if found_tbl:
                result[tbl] = {
                    "status": "exists",
                    "matched_name": found_tbl,
                    "lineage_path": index.lineage_path_for_table(found_tbl),
                    "domain": index.table_domain.get(found_tbl, ""),
                    "mappings": list(index.table_mappings.get(found_tbl, set()))[:5],
                    "columns": {}
                }
            else:
                result[tbl] = {
                    "status": "missing",
                    "matched_name": None,
                    "lineage_path": "",
                    "domain": "",
                    "mappings": [],
                    "columns": {}
                }

        if not col:
            continue

        tbl_info = result[tbl]
        matched  = tbl_info.get("matched_name")

        if matched:
            found_col = index.find_col(matched, col)
            if found_col:
                tbl_info["columns"][col] = {
                    "status": "exists",
                    "datatype": dt,
                    "description": desc,
                    "business_rule": rule,
                    "lineage_path": index.lineage_path_for_col(matched, found_col),
                }
            else:
                tbl_info["columns"][col] = {
                    "status": "missing",
                    "datatype": dt,
                    "description": desc,
                    "business_rule": rule,
                    "lineage_path": "",
                }
        else:
            tbl_info["columns"][col] = {
                "status": "missing",
                "datatype": dt,
                "description": desc,
                "business_rule": rule,
                "lineage_path": "",
            }

    # Summary
    t_ex  = sum(1 for v in result.values() if v["status"] == "exists")
    t_mis = sum(1 for v in result.values() if v["status"] == "missing")
    c_ex  = sum(1 for v in result.values() for cv in v["columns"].values() if cv["status"] == "exists")
    c_mis = sum(1 for v in result.values() for cv in v["columns"].values() if cv["status"] == "missing")
    t_col = c_ex + c_mis

    return {
        "tables": result,
        "summary": {
            "total_tables": len(result),
            "existing_tables": t_ex,
            "missing_tables": t_mis,
            "total_cols": t_col,
            "existing_cols": c_ex,
            "missing_cols": c_mis,
        }
    }






# ─────────────────────────────────────────────────────────────────────────────
# CIB DATA VAULT DDL GOVERNANCE ENGINE v2
# Physical Ingestion Lifecycle Sequence:
#   Stage 1: _REP  (STGT24)      - Replication / CDC capture
#   Stage 2: _INC  (DATAVLT_PRC) - Incremental batch loads, LD_DT CONSTRAINT PK
#   Stage 3: _MRR  (DATAVLT_PRC) - Mirror / current state, same template as _INC
#   Stage 4: _ARCH (QC_STG)      - Archive (explicitly defined in RSD only)
#   Stage 5: TARGET (DATAVLT)    - Enterprise Raw Data Vault table
# ─────────────────────────────────────────────────────────────────────────────

SEQ_ORDER = {'rep': 1, 'inc': 2, 'mrr': 3, 'arch': 4, 'target': 5}

_DEFAULT_TARGET_PARTITION = (
    "PARTITION BY (\n"
    "    CASE_N(LD_ENDDT > TIMESTAMP '2999-12-31 00:00:00+00:00'\n"
    "        NOT (LD_ENDDT > TIMESTAMP '2999-12-31 00:00:00+00:00'), NO CASE, UNKNOWN),\n"
    "    RANGE_N(DR_TMSTP BETWEEN TIMESTAMP '2015-01-01 00:00:00+00:00'\n"
    "        AND TIMESTAMP '2040-12-31 23:59:59+00:00'\n"
    "        EACH INTERVAL '1' DAY, NO RANGE, UNKNOWN),\n"
    "    RANGE_N(LD_DT BETWEEN TIMESTAMP '2015-01-01 00:00:00+00:00'\n"
    "        AND TIMESTAMP '2040-12-31 23:59:59+00:00'\n"
    "        EACH INTERVAL '1' DAY, NO RANGE, UNKNOWN)\n"
    ")"
)

_DEFAULT_INC_MRR_PARTITION = "PARTITION BY RANGE_N(SRC_FLAG BETWEEN 'STG' AND 'VAULT')"

# ── Public governance config (editable via UI Governance Portal) ──────────────
DEFAULT_DV_GOVERNANCE = {
    "fallback":          False,
    # Journal / Checksum / Map — all OFF per CIB policy update
    "no_before_journal": False,
    "no_after_journal":  False,
    "checksum_default":  False,
    "mergeblockedratio": False,
    "map_td_map1":       False,
    # Datatype enforcement
    "pk_datatype":       "VARCHAR(50) NOT NULL",
    "date_to_timestamp": True,
    "float_to_decimal":  True,
    # Audit columns
    "auto_audit_cols":   True,
    # Primary Index patterns per layer:
    # Uses tokens: '{rsd_pk}' for RSD key, '{bare}' for table name, or explicit columns
    "rep_pi_pattern":    "PRIMARY INDEX (DateTime, TimeZone_, skillTargetID)",
    "inc_pi_pattern":    "PRIMARY INDEX PI_{bare} ({rsd_pk})",
    "mrr_pi_pattern":    "PRIMARY INDEX PI_{bare} ({rsd_pk})",
    "arch_pi_pattern":   "PRIMARY INDEX PI_{bare} ({rsd_pk})",
    "target_pi_pattern": "PRIMARY INDEX PI_{bare} ({rsd_pk})",
    # Legacy alias kept for backwards compatibility
    "rep_primary_index": "PRIMARY INDEX (DateTime, TimeZone_, skillTargetID)",
    # Primary Key patterns per layer:
    # Uses tokens: '{rsd_pk}' for RSD key, 'LD_DT' for audit date, or 'NONE'
    "rep_pk_pattern":    "NONE",
    "inc_pk_pattern":    "LD_DT, {rsd_pk}",
    "mrr_pk_pattern":    "LD_DT, {rsd_pk}",
    "arch_pk_pattern":   "{rsd_pk}",
    "target_pk_pattern": "{rsd_pk}",
    # Partition clauses
    "inc_mrr_partition": _DEFAULT_INC_MRR_PARTITION,
    "target_partition":  _DEFAULT_TARGET_PARTITION,
}



# Column suffixes that identify a Primary / Hash Key
_PK_SUFFIXES = ("_HK", "_KEY", "_PK", "_ID", "_SK", "_NK")

# CIB Teradata datatype map
_CIB_DT_MAP = {
    "int": "INTEGER", "integer": "INTEGER", "bigint": "BIGINT", "smallint": "SMALLINT",
    "varchar": "VARCHAR(255)", "char": "CHAR(1)", "string": "VARCHAR(255)",
    "text": "VARCHAR(4000)", "clob": "VARCHAR(4000)",
    "decimal": "DECIMAL(18,2)", "numeric": "DECIMAL(18,2)", "number": "DECIMAL(18,2)",
    "float":  "DECIMAL(18,4)",
    "double": "DECIMAL(18,4)",
    "date":      "TIMESTAMP(6)",
    "datetime":  "TIMESTAMP(6)",
    "timestamp": "TIMESTAMP(6)",
    "boolean": "BYTEINT", "bool": "BYTEINT", "bit": "BYTEINT",
}


def _cib_datatype(col_name, raw_dt, is_pk=False, cfg=None):
    cfg = cfg or DEFAULT_DV_GOVERNANCE
    col_up = col_name.upper()
    pk_dt  = cfg.get("pk_datatype", "VARCHAR(50) NOT NULL")
    # Rule 1: PK/HK cols -> configurable PK datatype
    if is_pk or any(col_up.endswith(s) for s in _PK_SUFFIXES):
        return pk_dt
    base = raw_dt.lower().split("(")[0].strip()
    # Rule 2: Date -> Timestamp (if configured)
    if cfg.get("date_to_timestamp", True) and base in ("date", "datetime"):
        return "TIMESTAMP(6)"
    # Rule 3: Float on monetary cols -> DECIMAL (if configured)
    if cfg.get("float_to_decimal", True) and base in ("float", "double", "real"):
        if any(kw in col_up for kw in ("AMT", "BAL", "AMOUNT", "BALANCE", "RATE", "PRICE", "FEE")):
            return "DECIMAL(18,2)"
        return "DECIMAL(18,4)"
    return _CIB_DT_MAP.get(base, raw_dt.upper())


def _table_short(full_name):
    return full_name.split(".")[-1].upper()


def _detect_table_type(table_name):
    bare = _table_short(table_name).lower()
    if bare.endswith("_rep"):  return "rep"
    if bare.endswith("_inc"):  return "inc"
    if bare.endswith("_mrr"):  return "mrr"
    if bare.endswith("_arch"): return "arch"
    return "target"


def _governance_schema(table_name, table_type):
    if table_type == "rep":            return "STGT24"
    if table_type in ("inc", "mrr"):   return "DATAVLT_PRC"
    if table_type == "arch":           return "QC_STG"
    return "DATAVLT"


# Mandatory audit columns per table type: (NAME, DATATYPE, is_constraint_pk)
_AUDIT_INC_MRR = [
    ("LD_DT",    "TIMESTAMP(6)",  True),    # Always CONSTRAINT PK for INC/MRR
    ("RCRD_SRC", "VARCHAR(20)",   False),
    ("TBL_NAME", "VARCHAR(50)",   False),
    ("DR_TMSTP", "TIMESTAMP(6)",  False),
    ("DR_FLG",   "VARCHAR(5)",    False),
    ("SRC_FLAG", "VARCHAR(50)",   False),
    ("LD_ENDDT", "TIMESTAMP(6)",  False),
]
_AUDIT_REP = [
    ("RPL_TRX_TIME", "TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)", False),
    ("RPL_TRX_TYPE", "VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'L'", False),
    ("RPL_CAPXINFO", "VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC", False),
    ("DV_FLG",       "VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT 'N'", False),
]
_AUDIT_TARGET = [
    ("LD_DT",    "TIMESTAMP(6)",  False),
    ("RCRD_SRC", "VARCHAR(20)",   False),
    ("TBL_NAME", "VARCHAR(50)",   False),
    ("DR_TMSTP", "TIMESTAMP(6)",  False),
    ("DR_FLG",   "VARCHAR(5)",    False),
    ("LD_ENDDT", "TIMESTAMP(6)",  False),
]
_AUDIT_ARCH = []

_AUDIT_MAP = {
    "rep":    _AUDIT_REP,
    "inc":    _AUDIT_INC_MRR,
    "mrr":    _AUDIT_INC_MRR,
    "arch":   _AUDIT_ARCH,
    "target": _AUDIT_TARGET,
}


def _build_ddl_header(schema, bare, cfg):
    """Build CREATE MULTISET TABLE header with configurable optional clauses."""
    fallback = " ,FALLBACK" if cfg.get("fallback", True) else ""
    clauses  = [f"CREATE MULTISET TABLE {schema}.{bare}{fallback}"]
    if cfg.get("no_before_journal", False): clauses.append("NO BEFORE JOURNAL")
    if cfg.get("no_after_journal",  False): clauses.append("NO AFTER JOURNAL")
    if cfg.get("checksum_default",  False): clauses.append("CHECKSUM = DEFAULT")
    if cfg.get("mergeblockedratio", False): clauses.append("DEFAULT MERGEBLOCKEDRATIO")
    if cfg.get("map_td_map1",       False): clauses.append("MAP = TD_MAP1")
    return ",\n".join(clauses) if len(clauses) > 1 else clauses[0]


def _resolve_pk_col(tbl_type, biz_pk, cfg):
    """
    Resolve the PRIMARY KEY constraint expression for a table stage based on governance pattern.
    Patterns can be:
      - 'LD_DT, {rsd_pk}' -> e.g. 'LD_DT, CALL_ID'
      - '{rsd_pk}' -> e.g. 'CALL_ID' or 'CALL_HK'
      - '{rsd_pk}, LD_DT' -> e.g. 'CALL_ID, LD_DT'
      - 'LD_DT' -> 'LD_DT'
      - 'NONE' or '' -> None (no PK constraint)
    """
    pattern_key = f"{tbl_type}_pk_pattern"
    pattern = cfg.get(pattern_key)
    if pattern is None:
        if tbl_type in ("inc", "mrr"):
            pattern = "LD_DT, {rsd_pk}"
        elif tbl_type == "rep":
            pattern = "NONE"
        else:
            pattern = "{rsd_pk}"

    pattern = str(pattern).strip()
    if pattern.upper() in ("NONE", "NO", "FALSE", "", "NULL"):
        return None

    actual_biz = biz_pk or "ID"
    if "{rsd_pk}" in pattern:
        if actual_biz.upper() == "LD_DT":
            resolved = "LD_DT"
        else:
            resolved = pattern.replace("{rsd_pk}", actual_biz)
    else:
        resolved = pattern

    parts = [p.strip() for p in resolved.split(",") if p.strip()]
    seen = set()
    dedup = []
    for p in parts:
        if p.upper() not in seen:
            seen.add(p.upper())
            dedup.append(p)
    return ", ".join(dedup) if dedup else None


def _resolve_pi_clause(tbl_type, bare, biz_pk, cfg):
    """
    Resolve the PRIMARY INDEX clause for a table stage based on governance pattern.
    Patterns can be:
      - 'PI_{bare} ({rsd_pk})' or 'PRIMARY INDEX PI_{bare} ({rsd_pk})'
      - '({rsd_pk})' or 'PRIMARY INDEX ({rsd_pk})'
      - '(DateTime, TimeZone_, skillTargetID)' or 'PRIMARY INDEX (DateTime, TimeZone_, skillTargetID)'
      - 'NO PRIMARY INDEX'
      - 'NONE' or ''
    """
    pattern_key = f"{tbl_type}_pi_pattern"
    if tbl_type == "rep":
        pattern = cfg.get("rep_pi_pattern") or cfg.get("rep_primary_index") or "PRIMARY INDEX (DateTime, TimeZone_, skillTargetID)"
    else:
        pattern = cfg.get(pattern_key) or f"PRIMARY INDEX PI_{{bare}} ({{rsd_pk}})"

    pattern = str(pattern).strip()
    if pattern.upper() in ("NONE", "NO", "FALSE", "", "NULL"):
        return ""

    actual_biz = biz_pk or "ID"
    resolved = pattern.replace("{rsd_pk}", actual_biz).replace("{bare}", bare)

    if not (resolved.upper().startswith("PRIMARY INDEX") or resolved.upper().startswith("NO PRIMARY INDEX")):
        resolved = f"PRIMARY INDEX {resolved}"

    return resolved


def _build_column_block(rsd_columns, audit_cols, gov_log, tbl, tbl_type="target", cfg=None):
    """
    Build the column definition list.
    Returns: (col_defs, pk_col, biz_pk)
      pk_col  = CONSTRAINT PRIMARY KEY columns (resolved per governance pattern)
      biz_pk  = PRIMARY INDEX column (business/natural key from RSD)
    """
    cfg      = cfg or DEFAULT_DV_GOVERNANCE
    col_defs = []
    existing = {c.upper() for c in rsd_columns}
    biz_pk   = None   # Natural business key from RSD -> becomes PRIMARY INDEX

    for col_name, col_info in rsd_columns.items():
        raw_dt = col_info.get("datatype", "varchar")
        is_pk  = col_info.get("key", False)
        if not is_pk and biz_pk is None:
            is_pk = any(col_name.upper().endswith(s) for s in _PK_SUFFIXES)
        gov_dt = _cib_datatype(col_name, raw_dt, is_pk, cfg)
        if gov_dt.replace(" NOT NULL", "") != raw_dt.upper():
            gov_log.append(
                f"--     {col_name} ({raw_dt.upper()}) -> {gov_dt} "
                f"({'PK rule' if is_pk else 'DT rule'})"
            )
        desc    = col_info.get("description", "")
        comment = f"  -- {desc}" if desc else ""
        col_defs.append(f"    {col_name:<35} {gov_dt}{comment}")
        if is_pk and biz_pk is None:
            biz_pk = col_name

    if cfg.get("auto_audit_cols", True):
        for a_name, a_dt, a_pk in audit_cols:
            if a_name.upper() in existing:
                continue
            col_defs.append(f"    {a_name:<35} {a_dt}  -- CIB Audit Column")
            gov_log.append(f"--     Auto-injected: {a_name} {a_dt}")
            if a_pk and biz_pk is None:
                biz_pk = a_name

    if biz_pk is None:
        biz_pk = "LD_DT"

    # Resolve PK constraint columns based on governance policy pattern for this stage
    pk_col = _resolve_pk_col(tbl_type, biz_pk, cfg)
    if pk_col:
        gov_log.append(f"--     PK Constraint: PRIMARY KEY ({pk_col})")
    else:
        gov_log.append(f"--     PK Constraint: (none configured)")

    # Log resolved Primary Index
    pi_clause = _resolve_pi_clause(tbl_type, tbl, biz_pk, cfg)
    if pi_clause:
        gov_log.append(f"--     Primary Index: {pi_clause}")
    else:
        gov_log.append(f"--     Primary Index: (none configured)")

    return col_defs, pk_col, biz_pk


# ── DDL Template Functions ────────────────────────────────────────────────────

def _ddl_rep(schema, bare, col_defs, pk_col, biz_pk, cfg):
    """Stage 1: _REP in STGT24 — multiset with configurable Primary Index and optional PK."""
    cols      = ",\n".join(col_defs)
    rep_pi    = _resolve_pi_clause("rep", bare, biz_pk, cfg)
    pk_clause = f",\n    CONSTRAINT CSTR_{bare}_PK PRIMARY KEY ({pk_col})" if pk_col else ""
    pi_suffix = f"\n{rep_pi};" if rep_pi else ";"
    return (
        f"CREATE MULTISET TABLE {schema}.{bare}\n"
        f"(\n{cols}{pk_clause}\n){pi_suffix}\n"
    )


def _ddl_inc(schema, bare, col_defs, pk_col, biz_pk, cfg):
    """Stage 2: _INC in DATAVLT_PRC — configurable PK & configurable Primary Index."""
    cols       = ",\n".join(col_defs)
    header     = _build_ddl_header(schema, bare, cfg)
    partition  = cfg.get("inc_mrr_partition", _DEFAULT_INC_MRR_PARTITION).strip()
    pi_clause  = _resolve_pi_clause("inc", bare, biz_pk, cfg)
    pk_clause  = f",\n    CONSTRAINT CSTR_{bare}_PK PRIMARY KEY ({pk_col})" if pk_col else ""
    tail_parts = [p for p in [pi_clause, partition] if p]
    tail_str   = ("\n" + "\n".join(tail_parts)) if tail_parts else ""
    return (
        f"{header}\n"
        f"(\n{cols}{pk_clause}\n){tail_str};\n"
    )


def _ddl_mrr(schema, bare, col_defs, pk_col, biz_pk, cfg):
    """Stage 3: _MRR in DATAVLT_PRC — same template as _INC with MRR PI pattern."""
    cols       = ",\n".join(col_defs)
    header     = _build_ddl_header(schema, bare, cfg)
    partition  = cfg.get("inc_mrr_partition", _DEFAULT_INC_MRR_PARTITION).strip()
    pi_clause  = _resolve_pi_clause("mrr", bare, biz_pk, cfg)
    pk_clause  = f",\n    CONSTRAINT CSTR_{bare}_PK PRIMARY KEY ({pk_col})" if pk_col else ""
    tail_parts = [p for p in [pi_clause, partition] if p]
    tail_str   = ("\n" + "\n".join(tail_parts)) if tail_parts else ""
    return (
        f"{header}\n"
        f"(\n{cols}{pk_clause}\n){tail_str};\n"
    )


def _ddl_arch(schema, bare, col_defs, pk_col, biz_pk, cfg):
    """Stage 4: _ARCH in QC_STG — configurable PK & Primary Index."""
    cols      = ",\n".join(col_defs)
    header    = _build_ddl_header(schema, bare, cfg)
    pi_clause = _resolve_pi_clause("arch", bare, biz_pk, cfg)
    pk_clause = f",\n    CONSTRAINT CSTR_{bare}_PK PRIMARY KEY ({pk_col})" if pk_col else ""
    tail_str  = f"\n{pi_clause}" if pi_clause else ""
    return (
        f"{header}\n"
        f"(\n{cols}{pk_clause}\n){tail_str};\n"
    )


def _ddl_target(schema, bare, col_defs, pk_col, biz_pk, cfg):
    """Stage 5: TARGET in DATAVLT — configurable PK & Primary Index with multi-part PARTITION BY."""
    cols       = ",\n".join(col_defs)
    header     = _build_ddl_header(schema, bare, cfg)
    partition  = cfg.get("target_partition", _DEFAULT_TARGET_PARTITION).strip()
    pi_clause  = _resolve_pi_clause("target", bare, biz_pk, cfg)
    pk_clause  = f",\n    CONSTRAINT CSTR_{bare}_PK PRIMARY KEY ({pk_col})" if pk_col else ""
    tail_parts = [p for p in [pi_clause, partition] if p]
    tail_str   = ("\n" + "\n".join(tail_parts)) if tail_parts else ""
    return (
        f"{header}\n"
        f"(\n{cols}{pk_clause}\n){tail_str};\n"
    )


_TEMPLATE_MAP = {
    "rep":    _ddl_rep,
    "inc":    _ddl_inc,
    "mrr":    _ddl_mrr,
    "arch":   _ddl_arch,
    "target": _ddl_target,
}


def build_pipeline_graphviz(table_base_name=None, plan_items=None):
    """
    Generate a Graphviz Digraph visualizing the CIB Data Vault Physical Ingestion Lifecycle:
    Stage 1: _REP (stgt24) -> Stage 2: _INC (datavlt_prc) -> Stage 3: _MRR (datavlt_prc) -> Stage 4: TARGET (datavlt)
    Optionally shows archive (_ARCH in qc_stg) if present.
    """
    import graphviz
    dot = graphviz.Digraph(comment="CIB Data Vault Ingestion Lifecycle")
    dot.attr(rankdir="LR", bgcolor="transparent")
    dot.attr("node", shape="box", style="filled,rounded", fontname="Segoe UI, Arial, sans-serif", fontsize="11", penwidth="1.8", margin="0.22,0.15")
    dot.attr("edge", color="#1976D2", penwidth="2.0", arrowsize="0.9", fontname="Segoe UI, Arial, sans-serif", fontsize="9")

    base = table_base_name or "ENTITY"
    rep_name = f"{base}_REP" if table_base_name else "Stage 1: _REP"
    dot.node("rep", f"Stage 1: Replication (_REP)\nTable: {rep_name}\nSchema: STGT24\nCDC Capture Staging\nPI: Configurable",
             fillcolor="#E3F2FD", color="#1565C0", fontcolor="#0D47A1")

    inc_name = f"{base}_INC" if table_base_name else "Stage 2: _INC"
    dot.node("inc", f"Stage 2: Incremental (_INC)\nTable: {inc_name}\nSchema: DATAVLT_PRC\nDelta Batch Load · LD_DT + RSD PK\nPartition: RANGE_N(SRC_FLAG)",
             fillcolor="#F3E5F5", color="#7B1FA2", fontcolor="#4A148C")

    mrr_name = f"{base}_MRR" if table_base_name else "Stage 3: _MRR"
    dot.node("mrr", f"Stage 3: Mirror (_MRR)\nTable: {mrr_name}\nSchema: DATAVLT_PRC\nCurrent State · LD_DT + RSD PK\nPartition: RANGE_N(SRC_FLAG)",
             fillcolor="#EDE7F6", color="#512DA8", fontcolor="#311B92")

    tgt_name = f"{base}" if table_base_name else "Stage 4: TARGET"
    dot.node("target", f"Stage 4: Target Vault\nTable: {tgt_name}\nSchema: DATAVLT\nEnterprise Raw Data Vault\nMulti-temperature Partitioning",
             fillcolor="#E8F5E9", color="#2E7D32", fontcolor="#1B5E20")

    dot.edge("rep", "inc", label=" CDC Stream")
    dot.edge("inc", "mrr", label=" Upsert Merge")
    dot.edge("mrr", "target", label=" Vault Load")

    has_arch = False
    if plan_items:
        has_arch = any(p[2] == "arch" for p in plan_items)
    if has_arch:
        arch_name = f"{base}_ARCH" if table_base_name else "Stage 5: _ARCH"
        dot.node("arch", f"Stage 5: Archive (_ARCH)\nTable: {arch_name}\nSchema: QC_STG\nCold Historical Staging",
                 fillcolor="#FFF3E0", color="#E65100", fontcolor="#BF360C")
        dot.edge("inc", "arch", label=" Cold Archive", style="dashed", color="#FB8C00")

    return dot


def build_column_level_graphviz(table_base_name=None, columns_dict=None, cfg=None):
    """
    Generate a Graphviz Digraph displaying column-level physical lineage across stages:
    STGT24._REP -> DATAVLT_PRC._INC -> DATAVLT_PRC._MRR -> DATAVLT.TARGET
    Renders HTML-like table record nodes with column ports and arrows between mapped attributes.
    """
    import graphviz
    dot = graphviz.Digraph(comment="Column Level Data Vault Lineage")
    dot.attr(rankdir="LR", bgcolor="transparent")
    dot.attr("node", shape="plain", fontname="Segoe UI, Arial, sans-serif", fontsize="10")
    dot.attr("edge", color="#1976D2", penwidth="1.6", arrowsize="0.7")

    base = table_base_name or "ENTITY"
    cfg = cfg or DEFAULT_DV_GOVERNANCE
    columns = columns_dict or {"ID": {"datatype": "VARCHAR(50)", "key": True}}

    stages = [
        ("rep", f"{base}_REP", "STGT24", "#1565C0", "#E3F2FD"),
        ("inc", f"{base}_INC", "DATAVLT_PRC", "#7B1FA2", "#F3E5F5"),
        ("mrr", f"{base}_MRR", "DATAVLT_PRC", "#512DA8", "#EDE7F6"),
        ("tgt", f"{base}", "DATAVLT", "#2E7D32", "#E8F5E9"),
    ]

    # Stage-specific audit columns
    stage_audit = {
        "rep": [
            ("RPL_TRX_TIME", "TIMESTAMP(6)"),
            ("RPL_TRX_TYPE", "VARCHAR(1)"),
            ("RPL_CAPXINFO", "VARCHAR(250)"),
            ("DV_FLG", "VARCHAR(1)"),
        ],
        "inc": [
            ("LD_DT", "TIMESTAMP(6)"),
            ("RCRD_SRC", "VARCHAR(20)"),
            ("TBL_NAME", "VARCHAR(50)"),
            ("DR_TMSTP", "TIMESTAMP(6)"),
            ("SRC_FLAG", "VARCHAR(50)"),
            ("LD_ENDDT", "TIMESTAMP(6)"),
        ],
        "mrr": [
            ("LD_DT", "TIMESTAMP(6)"),
            ("RCRD_SRC", "VARCHAR(20)"),
            ("TBL_NAME", "VARCHAR(50)"),
            ("DR_TMSTP", "TIMESTAMP(6)"),
            ("SRC_FLAG", "VARCHAR(50)"),
            ("LD_ENDDT", "TIMESTAMP(6)"),
        ],
        "tgt": [
            ("LD_DT", "TIMESTAMP(6)"),
            ("RCRD_SRC", "VARCHAR(20)"),
            ("TBL_NAME", "VARCHAR(50)"),
            ("DR_TMSTP", "TIMESTAMP(6)"),
            ("DR_FLG", "VARCHAR(5)"),
            ("LD_ENDDT", "TIMESTAMP(6)"),
        ],
    }

    col_items = list(columns.items())
    max_display = 12
    display_cols = col_items[:max_display]

    for stg_id, tbl_name, schema, border_color, fill_color in stages:
        rows = [
            f'<TR><TD BGCOLOR="{border_color}" COLSPAN="2"><FONT COLOR="white"><B>{schema}.{tbl_name}</B></FONT></TD></TR>'
        ]
        for cname, cinfo in display_cols:
            raw_dt = cinfo.get("datatype", "varchar") if isinstance(cinfo, dict) else "varchar"
            is_pk = cinfo.get("key", False) if isinstance(cinfo, dict) else False
            gov_dt = _cib_datatype(cname, raw_dt, is_pk=is_pk, cfg=cfg)
            gov_dt_clean = gov_dt.replace(" NOT NULL", "")
            icon = "🔑 " if is_pk else ""
            port_name = f"c_{cname.lower()}"
            rows.append(
                f'<TR><TD PORT="{port_name}" ALIGN="LEFT" BGCOLOR="{fill_color}">{icon}{cname}</TD>'
                f'<TD ALIGN="RIGHT" BGCOLOR="{fill_color}"><FONT COLOR="#555">{gov_dt_clean}</FONT></TD></TR>'
            )

        if len(col_items) > max_display:
            rows.append(
                f'<TR><TD COLSPAN="2" ALIGN="CENTER" BGCOLOR="{fill_color}"><FONT COLOR="#777"><I>+ {len(col_items) - max_display} more columns</I></FONT></TD></TR>'
            )

        for aname, adt in stage_audit.get(stg_id, []):
            aport_name = f"a_{aname.lower()}"
            rows.append(
                f'<TR><TD PORT="{aport_name}" ALIGN="LEFT"><FONT COLOR="#777"><I>{aname}</I></FONT></TD>'
                f'<TD ALIGN="RIGHT"><FONT COLOR="#888">{adt}</FONT></TD></TR>'
            )

        table_html = f'<<TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" COLOR="{border_color}" STYLE="ROUNDED">{"".join(rows)}</TABLE>>'
        dot.node(stg_id, table_html)

    stage_pairs = [("rep", "inc"), ("inc", "mrr"), ("mrr", "tgt")]
    for src_stg, dst_stg in stage_pairs:
        for cname, _ in display_cols:
            cid = f"c_{cname.lower()}"
            dot.edge(f"{src_stg}:{cid}:e", f"{dst_stg}:{cid}:w")

    for aname in ["ld_dt", "rcrd_src", "tbl_name", "dr_tmstp", "ld_enddt"]:
        aport = f"a_{aname}"
        dot.edge(f"inc:{aport}:e", f"mrr:{aport}:w", style="dashed", color="#9C27B0")
        dot.edge(f"mrr:{aport}:e", f"tgt:{aport}:w", style="dashed", color="#4CAF50")

    return dot


def generate_ddl(gap_result, target_db="Teradata", governance_config=None):
    """
    CIB Data Vault DDL Governance Engine v2.

    Physical Lifecycle: _REP (stgt24) -> _INC (datavlt_prc) -> _MRR (datavlt_prc)
                        -> _ARCH (qc_stg) -> TARGET (datavlt)

    governance_config: optional dict to override DEFAULT_DV_GOVERNANCE settings.
    All settings are editable through the app Governance Portal.
    """
    cfg      = {**DEFAULT_DV_GOVERNANCE, **(governance_config or {})}
    sections    = []
    gov_summary = []

    header = (
        "-- ================================================================\n"
        "-- CIB DATA VAULT DDL  --  Governance Engine v2\n"
        "-- Teradata / CIB DWH / Data Vault Naming Convention\n"
        "-- Physical Lifecycle:\n"
        "--   Stage 1: _REP  (STGT24)      - Replication staging\n"
        "--   Stage 2: _INC  (DATAVLT_PRC) - Incremental, LD_DT CONSTRAINT PK\n"
        "--   Stage 3: _MRR  (DATAVLT_PRC) - Mirror / current state\n"
        "--   Stage 4: _ARCH (QC_STG)      - Archive (explicit RSD only)\n"
        "--   Stage 5: TARGET (DATAVLT)    - Enterprise Data Vault\n"
        "-- Rules enforced:\n"
        f"--   PK/_HK/_KEY  -> {cfg['pk_datatype']}\n"
        f"--   DATE         -> TIMESTAMP(6)  [{'ON' if cfg['date_to_timestamp'] else 'OFF'}]\n"
        f"--   FLOAT        -> DECIMAL       [{'ON' if cfg['float_to_decimal'] else 'OFF'}]\n"
        f"--   Audit cols   -> {'ON' if cfg['auto_audit_cols'] else 'OFF'}\n"
        "-- ================================================================\n\n"
    )

    missing_tables = {tbl: info for tbl, info in gap_result["tables"].items() if info["status"] == "missing"}
    existing_with_missing_cols = {
        tbl: info for tbl, info in gap_result["tables"].items()
        if info["status"] == "exists" and any(v["status"] == "missing" for v in info["columns"].values())
    }

    # Detect if RSD explicitly models pipeline stages already
    has_explicit_pipeline = any(
        tbl.split('.')[-1].upper().endswith(('_REP', '_INC', '_MRR'))
        for tbl in missing_tables
    )

    plan = []
    for tbl, info in missing_tables.items():
        bare  = tbl.split('.')[-1].upper()
        lower = bare.lower()
        if lower.endswith('_rep'):
            plan.append(('STGT24', bare, 'rep', info['columns']))
        elif lower.endswith('_inc'):
            plan.append(('DATAVLT_PRC', bare, 'inc', info['columns']))
        elif lower.endswith('_mrr'):
            plan.append(('DATAVLT_PRC', bare, 'mrr', info['columns']))
        elif lower.endswith('_arch'):
            plan.append(('QC_STG', bare, 'arch', info['columns']))
        else:
            if has_explicit_pipeline:
                plan.append(('DATAVLT', bare, 'target', info['columns']))
            else:
                # Auto-expand to CIB 4-tier lifecycle: _REP -> _INC -> _MRR -> TARGET
                base = re.sub(r'_(stg|inc|mrr|rep|arch)$', '', bare, flags=re.IGNORECASE)
                plan.append(('STGT24',      f'{base}_REP', 'rep',    info['columns']))
                plan.append(('DATAVLT_PRC', f'{base}_INC', 'inc',    info['columns']))
                plan.append(('DATAVLT_PRC', f'{base}_MRR', 'mrr',    info['columns']))
                plan.append(('DATAVLT',     bare,          'target', info['columns']))

    # Sort strictly by CIB lifecycle sequence
    plan.sort(key=lambda x: (SEQ_ORDER.get(x[2], 99), x[1]))

    for schema, bare, tbl_type, columns in plan:
        audit_cols = _AUDIT_MAP[tbl_type]
        template   = _TEMPLATE_MAP[tbl_type]
        gov_log    = [f"-- TABLE: {schema}.{bare}  [type: {tbl_type.upper()}]",
                      "-- Governance applied:"]
        col_defs, pk_col, biz_pk = _build_column_block(columns, audit_cols, gov_log, bare, tbl_type, cfg)
        if not col_defs:
            col_defs = ["    -- No columns defined in RSD"]
        ddl = template(schema, bare, col_defs, pk_col, biz_pk, cfg)
        sections.append("\n".join(gov_log) + "\n" + ddl)
        gov_summary.append(f"--   NEW {tbl_type.upper():<8} {schema}.{bare}")

    for tbl, info in existing_with_missing_cols.items():
        missing_cols = {c: v for c, v in info["columns"].items() if v["status"] == "missing"}
        matched = info.get("matched_name", tbl)
        lines   = [
            f"-- EXISTING TABLE: {matched}  [add {len(missing_cols)} missing col(s)]",
            "-- Governance applied:",
        ]
        for col_name, cv in missing_cols.items():
            gov_dt = _cib_datatype(col_name, cv.get("datatype", "varchar"), cfg=cfg)
            if gov_dt != cv.get("datatype", "varchar").upper():
                lines.append(f"--   {col_name} ({cv.get('datatype','?').upper()}) -> {gov_dt}")
            lines.append(f"ALTER TABLE {matched} ADD {col_name:<35} {gov_dt};")
        sections.append("\n".join(lines))
        gov_summary.append(f"--   ALTER TABLE  {matched}  ({len(missing_cols)} cols)")

    if not sections:
        return "-- No missing elements found. All RSD requirements already exist in lineage."

    summary_block = (
        "\n\n-- ================================================================\n"
        "-- GOVERNANCE SUMMARY\n"
        "-- ================================================================\n"
        + "\n".join(gov_summary) + "\n"
        "-- ================================================================\n"
    )
    return header + "\n\n".join(sections) + summary_block
