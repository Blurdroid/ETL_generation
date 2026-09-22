import json
import requests
import streamlit as st
import graphviz
import sqlparse
import xml.etree.ElementTree as ET

from powercenter_agent import PowerCenterAgent
from etl_generator import ETLGenerator
from powercenter_xml_generator import generate_powercenter_xml

# ============================================================
# CONFIGURATION & LOAD AGENTS
# ============================================================
OLLAMA_URL = "http://127.0.0.1:11434"
MODEL = "qwen3:4b"
METADATA_FILE = "powercenter_metadata.json"

st.set_page_config(page_title="AI PowerCenter", page_icon="🤖", layout="wide", initial_sidebar_state="expanded")

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
if "page" not in st.session_state: st.session_state.page = "Workflow Explanation"
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
    components = ([etl.get("source")] if etl.get("source") else []) + etl.get("components", []) + ([etl.get("target")] if etl.get("target") else [])
    
    for node in components:
        if not node: continue
        nid, nname, ntype = node.get("id", node.get("name")), node.get("name", "Unnamed"), node.get("type", "COMP").upper()
        label = f'''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
        <TR><TD BGCOLOR="#E8F0FE"><B>{nname}</B><BR/><FONT POINT-SIZE="9">({ntype})</FONT></TD></TR>
        '''
        for f in node.get("fields", []):
            fname = f.get("name", "") if isinstance(f, dict) else f
            label += f'<TR><TD PORT="{fname}" ALIGN="LEFT">{fname}</TD></TR>'
        label += '</TABLE>>'
        graph.node(nid, label=label)

    for conn in etl.get("connections", []):
        src, dst = conn.get("from"), conn.get("to")
        src_node = next((n for n in components if n and n.get("id", n.get("name")) == src), None)
        dst_node = next((n for n in components if n and n.get("id", n.get("name")) == dst), None)
        if src_node and dst_node:
            src_f = [f.get("name") if isinstance(f, dict) else f for f in src_node.get("fields", [])]
            dst_f = [f.get("name") if isinstance(f, dict) else f for f in dst_node.get("fields", [])]
            mapped = False
            for sf in src_f:
                if sf in dst_f:
                    graph.edge(f"{src}:{sf}:e", f"{dst}:{sf}:w")
                    mapped = True
            if not mapped: 
                graph.edge(src, dst)
    return graph

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("## 🤖 AI PowerCenter")
    page = st.radio("Navigation", ["🔍 Workflow Explanation", "🏗️ ETL Generation"], index=0 if st.session_state.page == "Workflow Explanation" else 1)
    st.session_state.page = "Workflow Explanation" if page == "🔍 Workflow Explanation" else "ETL Generation"
    st.divider()
    
    st.markdown("### Target Architecture")
    selected_db = st.selectbox("Target Database", ["Oracle", "Microsoft SQL Server", "Generic/ANSI"])
    st.session_state.database_type = selected_db

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
    st.markdown('<div class="main-title">🏗️ ETL Generation</div>', unsafe_allow_html=True)
    st.caption(f"Currently targeting: **{st.session_state.database_type}** (Change in sidebar)")
    
    # --- CIB Architecture Image Integration ---
    with st.expander("🏛️ View CIB Logical Data Architecture", expanded=False):
        try:
            # Displays the image provided by the user directly in Streamlit
            st.image("WhatsApp Image 2026-09-22 at 12.29.35 PM.jpeg", caption="CIB Architecture Reference", use_container_width=True)
        except Exception:
            st.error("Image 'WhatsApp Image 2026-09-22 at 12.29.35 PM.jpeg' not found in the root directory. Please ensure it is saved in the same folder as app.py.")
    st.divider()
    # ------------------------------------------

    mode = st.radio("How do you want to build the ETL?", ["🧩 Form Builder", "💬 Conversational AI"], horizontal=True)
    st.session_state.etl_generation_mode = mode
    st.divider()

    # ========================================================
    # FORM BUILDER
    # ========================================================
    if mode == "🧩 Form Builder":
        st.markdown('<div class="section-title">🧩 ETL Design</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        with col1:
            source_name = st.text_input("Source Name", placeholder="CUSTOMER")
            source_fields_text = st.text_area("Source Fields (or DDL)", placeholder="CUSTOMER_ID\nFIRST_NAME\nLAST_NAME\nSTATUS", height=140)
            target_name = st.text_input("Target Name", placeholder="CUSTOMER_DWH")
            target_fields_text = st.text_area("Target Fields", placeholder="CUSTOMER_ID\nFULL_NAME\nSTATUS", height=140)

        with col2:
            st.markdown("**Architecture Layers**")
            architecture = st.multiselect("Select architecture components", ["Data Vault", "Business Vault"])
            st.markdown("**Transformations**")
            transformations = st.multiselect("Select transformations", ["Source Qualifier", "Expression", "Filter", "Joiner", "Lookup", "Sorter", "Aggregator", "Router", "Update Strategy", "Sequence Generator"])
            additional_requirements = st.text_area("Additional Requirements", placeholder="Example:\nKeep active customers only.\nCreate FULL_NAME from first and last name.", height=160)

        st.divider()

        if st.button("🚀 Generate ETL", type="primary", use_container_width=True):
            s_fields = [x.strip() for x in source_fields_text.splitlines() if x.strip()]
            t_fields = [x.strip() for x in target_fields_text.splitlines() if x.strip()]
            arch_text = ", ".join(architecture) if architecture else "No Data Vault layer requested"
            tx_text = ", ".join(transformations) if transformations else "No additional transformations requested"

            requirement = f"""
            Create a logical ETL pipeline.
            Pipeline Name: {source_name or "ETL_PIPELINE"}
            Source Name: {source_name or "SOURCE"}
            Source Fields: {s_fields}
            Architecture: {arch_text}
            Transformations to Include: {tx_text}
            Target Name: {target_name or "TARGET"}
            Target Fields: {t_fields}
            Additional Business Logic: {additional_requirements}
            """
            
            with st.spinner("Qwen is designing the ETL..."):
                try:
                    etl = etl_generator.generate_from_requirement(requirement, st.session_state.database_type)
                    st.session_state.current_etl = etl
                    st.session_state.etl_messages = [] 
                    st.success("ETL generated successfully.")
                except Exception as e:
                    st.error("ETL generation failed.")
                    st.exception(e)

    # ========================================================
    # CONVERSATIONAL BUILDER
    # ========================================================
    else:
        st.markdown('<div class="section-title">💬 Dynamic ETL Generator</div>', unsafe_allow_html=True)
        st.info("You can start from scratch. Tell the AI what you want to build and provide your fields or DDL.")
        
        for message in st.session_state.etl_messages:
            with st.chat_message(message["role"]): st.markdown(message["content"])

        if etl_question := st.chat_input("Describe the ETL or paste your DDL..."):
            if "CREATE TABLE" in etl_question.upper():
                etl_question = "Here is my DDL:\n" + clean_ddl(etl_question)

            st.session_state.etl_messages.append({"role": "user", "content": etl_question})
            with st.chat_message("user"): st.markdown(etl_question)
            
            with st.chat_message("assistant"):
                with st.spinner("Qwen is designing the ETL..."):
                    try:
                        db_type = st.session_state.database_type
                        if st.session_state.current_etl is None:
                            etl = etl_generator.generate_from_requirement(etl_question, db_type)
                            if etl.get("needs_more_info") or etl.get("clarifying_question"):
                                response = etl.get("clarifying_question") or "Please provide your DDL."
                            else:
                                st.session_state.current_etl = etl
                                response = "I've created the initial ETL design."
                        else:
                            etl = etl_generator.modify_etl(etl_question, db_type)
                            if etl.get("needs_more_info") or etl.get("clarifying_question"):
                                response = etl.get("clarifying_question") or "Please provide more details."
                            else:
                                st.session_state.current_etl = etl
                                response = "I've updated the ETL according to your request."
                    except Exception as e:
                        response = f"I could not process the ETL request.\n\nError: `{e}`"
                st.markdown(response)
            st.session_state.etl_messages.append({"role": "assistant", "content": response})

    # ========================================================
    # PREVIEW & EXPORT (DYNAMIC ETL PREVIEW)
    # ========================================================
    current_etl = st.session_state.current_etl
    
    if current_etl:
        st.divider()
        st.markdown(f"### 👀 Dynamic Preview: `{current_etl.get('pipeline_name', 'ETL')}`")
        
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
            if xml_data: 
                st.download_button("⬇️ PowerCenter XML (.xml)", data=xml_data, file_name=f"{safe_name}.xml", mime="application/xml", type="primary", use_container_width=True)
        with c2:
            st.download_button("⬇️ Documentation (.md)", data=generate_markdown_docs(current_etl), file_name=f"{safe_name}_docs.md", mime="text/markdown", use_container_width=True)
        with c3:
            st.download_button("⬇️ Raw Structure (.json)", data=json.dumps(current_etl, indent=2), file_name=f"{safe_name}.json", mime="application/json", use_container_width=True)

        if st.button("🗑️ Start New ETL", use_container_width=True):
            st.session_state.current_etl = None
            st.session_state.etl_messages = []
            etl_generator.reset()
            st.rerun()