import json
import requests
import streamlit as st
import graphviz
import sqlparse
import xml.etree.ElementTree as ET
import paramiko
import tempfile
import os

from powercenter_agent import PowerCenterAgent
from etl_generator import ETLGenerator
from powercenter_xml_generator import generate_powercenter_xml

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
if "page" not in st.session_state: st.session_state.page = "ETL Generation"
if "workflow_messages" not in st.session_state: st.session_state.workflow_messages = []
if "etl_messages" not in st.session_state: st.session_state.etl_messages = []
if "current_etl" not in st.session_state: st.session_state.current_etl = None
if "etl_generation_mode" not in st.session_state: st.session_state.etl_generation_mode = "Form Builder"

st.markdown("""
    <style>
    .main-title { font-size: 38px; font-weight: 700; margin-bottom: 5px; }
    .subtitle { color: #777; font-size: 16px; margin-bottom: 25px; }
    .section-title { font-size: 25px; font-weight: 650; margin-top: 10px; margin-bottom: 15px; }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def clean_ddl(ddl_text):
    return sqlparse.format(ddl_text, strip_comments=True, reindent=True, keyword_case='upper')

def generate_markdown_docs(etl):
    md = f"# ETL Documentation: {etl.get('pipeline_name', 'Pipeline')}\n\n"
    md += f"**Description**: {etl.get('description', 'N/A')}\n\n"
    md += "---\n## Components & Flow\n"
    components = [etl.get("source", {})] + etl.get("components", []) + [etl.get("target", {})]
    for comp in components:
        if not comp: continue
        name = comp.get("name", "Unnamed")
        ctype = comp.get("type", "Component")
        md += f"### {name} ({ctype})\n"
        if comp.get("description"): md += f"*{comp.get('description')}*\n"
        if comp.get("condition"): md += f"**Condition**: `{comp.get('condition')}`\n"
        md += "| Column | Data Type | Logic/Expression |\n|---|---|---|\n"
        for f in comp.get("fields", []):
            fname = f.get("name", "") if isinstance(f, dict) else f
            ftype = f.get("datatype", "") if isinstance(f, dict) else ""
            fexpr = f.get("expression", "passthrough") if isinstance(f, dict) else ""
            md += f"| {fname} | {ftype} | {fexpr} |\n"
        md += "\n"
    return md

def render_simple_lineage(etl):
    if not etl: return None
    graph = graphviz.Digraph(engine="dot")
    graph.attr(rankdir="LR", size="12,6", nodesep="0.4", ranksep="0.8")
    graph.attr('node', shape='plaintext', fontname='Helvetica', fontsize='10')
    graph.attr('edge', color='#777777', arrowsize='0.7')
    
    src = etl.get("source", {})
    tgt = etl.get("target", {})
    comps = etl.get("components", [])
    
    if src: src["id"] = src.get("id", src.get("name", "SRC"))
    if tgt: tgt["id"] = tgt.get("id", tgt.get("name", "TGT"))
    for c in comps: c["id"] = c.get("id", c.get("name"))
    
    all_nodes = ([src] if src else []) + comps + ([tgt] if tgt else [])
    
    for node in all_nodes:
        if not node: continue
        nid = node["id"]
        nname = node.get("name", "Unnamed")
        ntype = node.get("type", "COMP").upper()
        label = f'''<<TABLE BORDER="0" CELLBORDER="1" CELLPADDING="4" CELLSPACING="0">
        <TR><TD BGCOLOR="#E8F0FE"><B>{nname}</B><BR/><FONT POINT-SIZE="9">({ntype})</FONT></TD></TR>
        '''
        for f in node.get("fields", []):
            fname = f.get("name", "") if isinstance(f, dict) else f
            label += f'<TR><TD ALIGN="LEFT" PORT="{fname}">{fname}</TD></TR>'
        label += '</TABLE>>'
        graph.node(nid, label=label)

    def resolve_ref(ref):
        for n in all_nodes:
            if n.get("id") == ref or n.get("name") == ref:
                return n["id"]
        return ref

    for conn in etl.get("connections", []):
        src_nid = resolve_ref(conn.get("from"))
        dst_nid = resolve_ref(conn.get("to"))
        
        if src_nid and dst_nid:
            src_node = next((n for n in all_nodes if n.get("id") == src_nid), None)
            dst_node = next((n for n in all_nodes if n.get("id") == dst_nid), None)
            
            if src_node and dst_node:
                src_f = [f.get("name") if isinstance(f, dict) else f for f in src_node.get("fields", [])]
                dst_f = [f.get("name") if isinstance(f, dict) else f for f in dst_node.get("fields", [])]
                mapped = False
                for sf in src_f:
                    if sf in dst_f:
                        graph.edge(f"{src_nid}:{sf}:e", f"{dst_nid}:{sf}:w")
                        mapped = True
                if not mapped: 
                    graph.edge(src_nid, dst_nid)
    return graph

def render_cib_architecture_graph():
    """Generates an interactive Graphviz representation of the CIB Architecture."""
    dot = graphviz.Digraph(engine='dot')
    dot.attr(rankdir='LR', size='14,7', compound='true', nodesep='0.4', ranksep='0.6')
    dot.attr('node', shape='box', style='filled', fontname='Helvetica', margin='0.2')

    # 1. Source Systems
    with dot.subgraph(name='cluster_source') as c:
        c.attr(label='Source Systems', style='dashed', color='#555555')
        c.node('SRC_DB2', 'IBM DB2\n(CARD400 / CR2)', tooltip='Source Systems: Core Banking, Cards, ATM Switches.', fillcolor='#e3f2fd')
        c.node('SRC_ORA', 'Oracle DB\n(T24 / OFSAA)', tooltip='Source Systems: Oracle-backed systems like T24 Core Banking.', fillcolor='#e3f2fd')
        c.node('SRC_SQL', 'SQL Server\n(IVR / Logs)', tooltip='Source Systems: Call center, CRM, and system logs.', fillcolor='#e3f2fd')

    # 2. Ingestion Layer
    with dot.subgraph(name='cluster_ingestion') as c:
        c.attr(label='Ingestion', style='dashed', color='#ff9800')
        c.node('ING_GG', 'Oracle GoldenGate', tooltip='Real-time CDC and replication from Oracle sources.', fillcolor='#fff3e0')
        c.node('ING_PE', 'PowerExchange', tooltip='Mainframe and DB2 Change Data Capture integration.', fillcolor='#fff3e0')

    # 3. Teradata Platform (Enterprise Data Warehouse)
    with dot.subgraph(name='cluster_td') as c:
        c.attr(label='Teradata Platform (EDW)', style='solid', color='#e53935', penwidth='2')
        c.node('TD_STG', 'Staging\n(STG)', tooltip='Staging (STG): 1:1 raw tables. No business logic. Immutable landing zone.', fillcolor='#ffebee')
        c.node('TD_DV', 'Data Vault\n(DV)', tooltip='Data Vault (DV): Historical system of record. Hubs, Links, Satellites.', fillcolor='#ffebee')
        c.node('TD_BV', 'Business Vault\n(BV)', tooltip='Business Vault (BV): Computations, soft business rules, derived attributes.', fillcolor='#ffebee')
        c.node('TD_IMART', 'Information Mart\n(iMART)', tooltip='Information Mart (iMART): Denormalized delivery layer. Star schemas.', fillcolor='#ffebee')

        c.edge('TD_STG', 'TD_DV')
        c.edge('TD_DV', 'TD_BV')
        c.edge('TD_BV', 'TD_IMART')
        c.edge('TD_DV', 'TD_IMART', style='dashed', label=' Fast Track', fontname='Helvetica', fontsize='10', color='#888888')

    # 4. Delivery / Business Objects
    with dot.subgraph(name='cluster_delivery') as c:
        c.attr(label='Business Objects / Delivery', style='dashed', color='#43a047')
        c.node('DEL_BI', 'Tableau / BI', tooltip='Tableau / BI: Executive Dashboards and Visualizations.', fillcolor='#e8f5e9')
        c.node('DEL_APP', 'OFSAA / IFRS9', tooltip='OFSAA / IFRS9: Risk, Compliance, and Finance modeling engines.', fillcolor='#e8f5e9')
        c.node('DEL_EXT', 'Extracts\n(CSV / SAAS)', tooltip='Extracts: Downstream system feeds and CSV exports.', fillcolor='#e8f5e9')

    # Edges between clusters
    dot.edge('SRC_DB2', 'ING_PE')
    dot.edge('SRC_ORA', 'ING_GG')
    dot.edge('SRC_SQL', 'ING_GG')

    dot.edge('ING_PE', 'TD_STG')
    dot.edge('ING_GG', 'TD_STG')

    dot.edge('TD_IMART', 'DEL_BI')
    dot.edge('TD_IMART', 'DEL_APP')
    dot.edge('TD_IMART', 'DEL_EXT')
    
    # Fast track from BV
    dot.edge('TD_BV', 'DEL_APP', style='dashed', color='#888888')

    return dot

def deploy_via_pmrep(xml_string, ssh_host, ssh_user, ssh_pwd, pc_domain, pc_repo, pc_user, pc_pwd, target_folder):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ssh_host, username=ssh_user, password=ssh_pwd, timeout=10)
        
        sftp = ssh.open_sftp()
        remote_xml_path = "/tmp/ai_generated_mapping.xml"
        remote_ctl_path = "/tmp/ai_import_control.xml"
        
        control_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE IMPORTPARAMS SYSTEM "impcntl.dtd">
        <IMPORTPARAMS CHECKRESOLUTION="YES">
            <FOLDERMAP SOURCEFOLDERNAME="{target_folder}" SOURCEREPOSITORYNAME="{pc_repo}" TARGETFOLDERNAME="{target_folder}" TARGETREPOSITORYNAME="{pc_repo}"/>
            <RESOLVECONFLICT>
                <TYPEOBJECT OBJECTTYPENAME="ALL" RESOLUTION="REPLACE"/>
            </RESOLVECONFLICT>
        </IMPORTPARAMS>"""

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_map:
            tmp_map.write(xml_string.encode('utf-8'))
            tmp_map_name = tmp_map.name
            
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_ctl:
            tmp_ctl.write(control_xml.encode('utf-8'))
            tmp_ctl_name = tmp_ctl.name

        sftp.put(tmp_map_name, remote_xml_path)
        sftp.put(tmp_ctl_name, remote_ctl_path)
        sftp.close()
        
        os.remove(tmp_map_name)
        os.remove(tmp_ctl_name)

        connect_cmd = f"pmrep connect -r {pc_repo} -d {pc_domain} -n {pc_user} -x {pc_pwd}"
        import_cmd = f"pmrep objectimport -i {remote_xml_path} -c {remote_ctl_path} -f {target_folder}"
        
        stdin, stdout, stderr = ssh.exec_command(f"{connect_cmd} && {import_cmd}")
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        
        if "Failed to execute" in output or error:
            return False, error or output
        return True, output
    except Exception as e:
        return False, str(e)

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("## 🤖 AI PowerCenter Architect")
    page = st.radio("Navigation", ["🔍 Workflow Explanation", "🏗️ ETL Generation"], index=1 if st.session_state.page == "ETL Generation" else 0)
    st.session_state.page = "ETL Generation" if page == "🏗️ ETL Generation" else "Workflow Explanation"
    st.divider()
    
    st.markdown("### Architecture Context")
    st.session_state.domain_sector = st.selectbox("Business Domain / Sector", ["General", "CARD400 (Cards)", "CR2 (ATM/Switch)", "T24 (Core Banking)", "OFSAA (Finance/Risk)", "IVR (Call Center)"])
    st.session_state.database_type = st.selectbox("Target Database", ["Teradata", "Oracle", "Microsoft SQL Server", "Generic/ANSI"])

# ============================================================
# PAGE 1: WORKFLOW EXPLANATION
# ============================================================
if st.session_state.page == "Workflow Explanation":
    st.markdown('<div class="main-title">🔍 Workflow Explanation</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Upload an Informatica PowerCenter XML file or select an existing workflow to investigate it using AI.</div>', unsafe_allow_html=True)

    uploaded_xml = st.file_uploader("Upload PowerCenter XML Export", type=["xml"])
    workflow_names = [w.get("name") for w in workflow_agent.metadata.get("workflows", []) if w.get("name")]
    
    if uploaded_xml:
        try:
            tree = ET.parse(uploaded_xml)
            root = tree.getroot()
            dynamic_workflow = {
                "name": uploaded_xml.name.replace(".xml", ""),
                "tasks": [elem.get("NAME") for elem in root.findall(".//TASK")],
                "sessions": [elem.get("NAME") for elem in root.findall(".//SESSION")],
                "links": [elem.get("FROMTASK") + " -> " + elem.get("TOTASK") for elem in root.findall(".//WORKFLOWLINK")]
            }
            if "workflows" not in workflow_agent.metadata:
                workflow_agent.metadata["workflows"] = []
            
            if dynamic_workflow["name"] not in workflow_names:
                workflow_agent.metadata["workflows"].append(dynamic_workflow)
                workflow_names.insert(0, dynamic_workflow["name"])
            st.success(f"Successfully loaded and parsed {uploaded_xml.name}")
        except Exception as e:
            st.error(f"Failed to parse XML: {e}")

    if not workflow_names:
        st.warning("No workflows found. Upload an XML file.")
        st.stop()

    selected_workflow = st.selectbox("Select Workflow to Analyze", workflow_names)
    workflow_agent.current_workflow = selected_workflow
    st.divider()

    workflow = workflow_agent.get_workflow(selected_workflow)
    if workflow:
        c1, c2, c3 = st.columns(3)
        c1.metric("Tasks", len(workflow.get("tasks", [])))
        c2.metric("Sessions", len(workflow.get("sessions", [])))
        c3.metric("Links", len(workflow.get("links", [])))

    st.divider()
    st.markdown('<div class="section-title">💬 Workflow Copilot</div>', unsafe_allow_html=True)
    for message in st.session_state.workflow_messages:
        with st.chat_message(message["role"]): st.markdown(message["content"])

    if question := st.chat_input("Ask about the selected workflow..."):
        st.session_state.workflow_messages.append({"role": "user", "content": question})
        with st.chat_message("user"): st.markdown(question)
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                try: answer = workflow_agent.chat(question)
                except Exception as e: answer = f"Error: `{e}`"
            st.markdown(answer)
        st.session_state.workflow_messages.append({"role": "assistant", "content": answer})

# ============================================================
# PAGE 2: ETL GENERATION
# ============================================================
elif st.session_state.page == "ETL Generation":
    st.markdown('<div class="main-title">🏗️ Enterprise ETL Generation</div>', unsafe_allow_html=True)
    st.caption(f"Domain: **{st.session_state.domain_sector}** | Target: **{st.session_state.database_type}**")
    
    # --- INTERACTIVE CIB ARCHITECTURE DIAGRAM ---
    with st.expander("🏛️ View CIB Logical Data Architecture Knowledge Base", expanded=False):
        st.info("💡 **Interactive Diagram:** Hover your mouse over any block in the diagram below to see its specific architectural role and definitions.")
        
        # Render the custom interactive Graphviz Diagram
        st.graphviz_chart(render_cib_architecture_graph(), use_container_width=True)
        
        st.markdown("### Interactive Layer Definitions")
        arch_tabs = st.tabs(["Source Systems", "Staging (STG)", "Data Vault (DV)", "Business Vault (BV)", "Information Mart (iMART)", "Business Objects"])
        
        with arch_tabs[0]:
            st.markdown("**Source Systems:** The origin of data. Includes Core Banking (T24), Card Systems (CARD400), ATM Switches (CR2), and Call Center logs (IVR). Handled via DB2, Oracle, or Flat Files.")
        with arch_tabs[1]:
            st.markdown("**Staging (STG):** The ingestion layer. Data is pulled via PowerExchange or Oracle GoldenGate and landed as 1:1 raw tables. No business logic is applied here; it acts as an immutable landing zone.")
        with arch_tabs[2]:
            st.markdown("**Data Vault (DV):** The historical system of record. Highly normalized into Hubs (business keys), Links (transactions/relationships), and Satellites (context/attributes). Designed for massive scalability and parallel loading.")
        with arch_tabs[3]:
            st.markdown("**Business Vault (BV):** The interpretation layer. Computations, soft business rules, and derived attributes are calculated here. Acts as a bridge between the raw Data Vault and analytical needs.")
        with arch_tabs[4]:
            st.markdown("**Information Mart (iMART):** The denormalized delivery layer. Star schemas and dimensional models are built here for specific business use cases (e.g., ad-hoc Fast Track reporting).")
        with arch_tabs[5]:
            st.markdown("**Business Objects / Delivery:** The consumption layer. Data is fed directly to Tableau, SAP BO, IFRS9 engines, or OFSAA scorecards for executive decision-making.")
    st.divider()
    # ------------------------------------------

    mode = st.radio("How do you want to build the ETL?", ["🧩 Form Builder", "💬 Conversational AI"], horizontal=True)
    st.session_state.etl_generation_mode = mode
    st.divider()

    # ========================================================
    # FORM BUILDER
    # ========================================================
    if mode == "🧩 Form Builder":
        st.markdown('<div class="section-title">🧩 Smart ETL Design</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        with col1:
            source_name = st.text_input("Source Name", placeholder="e.g., CARD400_ACCOUNT_STG")
            source_fields_text = st.text_area("Source Fields (or DDL)", placeholder="ACCOUNT_ID\nBALANCE\nSTATUS", height=140)
            target_name = st.text_input("Target Name", placeholder="e.g., HUB_ACCOUNT")
            target_fields_text = st.text_area("Target Fields", placeholder="HK_ACCOUNT_ID\nACCOUNT_ID\nLOAD_DTS", height=140)

        with col2:
            architecture = st.multiselect("Select Target Architecture Layer", ["Staging (STG)", "Raw Data Vault (Hub/Link/Sat)", "Business Vault (BV)", "Information Mart (iMART)"])
            transformations = st.multiselect("Select Transformations", ["Source Qualifier", "Expression", "Filter", "Joiner", "Lookup", "Sorter", "Aggregator", "Router", "Update Strategy", "Sequence Generator"])
            additional_requirements = st.text_area("Additional Business Rules", placeholder="Example:\nCalculate MD5 Hash for Hub Key.\nFilter out STATUS='CLOSED'.", height=160)

        st.divider()

        if st.button("🚀 Architect ETL Pipeline", type="primary", use_container_width=True):
            s_fields = [x.strip() for x in source_fields_text.splitlines() if x.strip()]
            t_fields = [x.strip() for x in target_fields_text.splitlines() if x.strip()]
            
            requirement = f"""
            Create a logical ETL pipeline.
            Domain/Sector: {st.session_state.domain_sector}
            Pipeline Name: {source_name or "ETL_PIPELINE"}
            Source Name: {source_name or "SOURCE"}
            Source Fields: {s_fields}
            Architecture Layer: {", ".join(architecture)}
            Transformations to Include: {", ".join(transformations)}
            Target Name: {target_name or "TARGET"}
            Target Fields: {t_fields}
            Business Rules: {additional_requirements}
            """
            
            with st.spinner("Data Architect AI is designing the ETL..."):
                try:
                    etl = etl_generator.generate_from_requirement(requirement, st.session_state.database_type, st.session_state.domain_sector)
                    st.session_state.current_etl = etl
                    st.session_state.etl_messages = [] 
                    st.success("ETL generated successfully.")
                except Exception as e:
                    st.error(f"ETL generation failed: {e}")

    # ========================================================
    # CONVERSATIONAL BUILDER
    # ========================================================
    else:
        st.markdown('<div class="section-title">💬 Conversational Architect</div>', unsafe_allow_html=True)
        st.info("Describe your business requirement. I will apply Enterprise Architecture standards to generate the pipeline.")
        
        for message in st.session_state.etl_messages:
            with st.chat_message(message["role"]): st.markdown(message["content"])

        if etl_question := st.chat_input("E.g., 'Create a T24 transaction satellite mapping. Here is my DDL...'"):
            st.session_state.etl_messages.append({"role": "user", "content": etl_question})
            with st.chat_message("user"): st.markdown(etl_question)
            
            with st.chat_message("assistant"):
                with st.spinner("Architecting..."):
                    try:
                        db_type = st.session_state.database_type
                        domain = st.session_state.domain_sector
                        if st.session_state.current_etl is None:
                            etl = etl_generator.generate_from_requirement(etl_question, db_type, domain)
                            response = etl.get("clarifying_question") or "I've architected the initial ETL design based on our standards."
                            if not etl.get("needs_more_info"): st.session_state.current_etl = etl
                        else:
                            etl = etl_generator.modify_etl(etl_question, db_type, domain)
                            response = etl.get("clarifying_question") or "I've updated the pipeline."
                            if not etl.get("needs_more_info"): st.session_state.current_etl = etl
                    except Exception as e:
                        response = f"I could not process the request.\n\nError: `{e}`"
                st.markdown(response)
            st.session_state.etl_messages.append({"role": "assistant", "content": response})

    # ========================================================
    # PREVIEW & EXPORT & DEPLOYMENT
    # ========================================================
    current_etl = st.session_state.current_etl
    
    if current_etl:
        st.divider()
        st.markdown(f"### 👀 Lineage Preview: `{current_etl.get('pipeline_name', 'ETL')}`")
        
        diagram = render_simple_lineage(current_etl)
        if diagram:
            st.graphviz_chart(diagram)
        
        st.divider()
        st.markdown("### 📦 Review & Export Downloads")
        safe_name = current_etl.get("pipeline_name", "etl").replace(" ", "_")
        
        try: 
            xml_data = generate_powercenter_xml(current_etl, database_type=st.session_state.database_type)
        except Exception as e: 
            xml_data = None
            st.error(f"XML Generation Error: {e}")

        c1, c2, c3 = st.columns(3)
        with c1:
            if xml_data: st.download_button("⬇️ PowerCenter XML (.xml)", data=xml_data, file_name=f"{safe_name}.xml", mime="application/xml", type="primary", use_container_width=True)
        with c2:
            st.download_button("⬇️ Architecture Docs (.md)", data=generate_markdown_docs(current_etl), file_name=f"{safe_name}_docs.md", mime="text/markdown", use_container_width=True)
        with c3:
            st.download_button("⬇️ Raw Structure (.json)", data=json.dumps(current_etl, indent=2), file_name=f"{safe_name}.json", mime="application/json", use_container_width=True)

        st.divider()
        st.markdown("### 🚀 Direct PowerCenter Deployment (pmrep)")
        
        with st.form("deploy_form"):
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                ssh_host = st.text_input("SSH Hostname", placeholder="10.x.x.x")
                ssh_user = st.text_input("SSH Username", placeholder="infa_admin")
                ssh_pwd = st.text_input("SSH Password", type="password")
            with col_b:
                pc_domain = st.text_input("PC Domain", placeholder="Domain_dev")
                pc_repo = st.text_input("Repository Name", placeholder="REP_DEV")
                target_folder = st.text_input("Target Folder", placeholder="ziad")
            with col_c:
                pc_user = st.text_input("PMREP User", placeholder="Administrator")
                pc_pwd = st.text_input("PMREP Password", type="password")
                deploy_btn = st.form_submit_button("Deploy to PowerCenter")
                
            if deploy_btn:
                if not xml_data:
                    st.error("Cannot deploy: XML Generation failed.")
                elif not all([ssh_host, ssh_user, ssh_pwd, pc_domain, pc_repo, pc_user, pc_pwd, target_folder]):
                    st.warning("Please fill in all connection details.")
                else:
                    with st.spinner(f"Connecting to {ssh_host} and deploying via pmrep..."):
                        success, log = deploy_via_pmrep(xml_data, ssh_host, ssh_user, ssh_pwd, pc_domain, pc_repo, pc_user, pc_pwd, target_folder)
                        if success:
                            st.success(f"Deployed successfully to {pc_repo}/{target_folder}!")
                            st.code(log, language="bash")
                        else:
                            st.error("Deployment failed.")
                            st.code(log, language="bash")

        if st.button("🗑️ Start New ETL", use_container_width=True):
            st.session_state.current_etl = None
            st.session_state.etl_messages = []
            etl_generator.reset()
            st.rerun()
