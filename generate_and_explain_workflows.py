import os
import xml.etree.ElementTree as ET
from powercenter_xml_generator import generate_powercenter_xml

def get_complex_pipelines():
    return [
        {
            "domain": "OFSAA",
            "pipeline_name": "OFSAA_RISK_METRICS_ETL",
            "description": "Extracts Risk Metrics from Data Vault to OFSAA Staging.",
            "source": {
                "name": "DV_HUB_RISK",
                "type": "SOURCE",
                "fields": [
                    {"name": "HK_RISK_ID", "datatype": "string", "key": True},
                    {"name": "RISK_METRIC", "datatype": "decimal"}
                ]
            },
            "components": [
                {
                    "id": "sq_1",
                    "name": "SQ_DV_HUB_RISK",
                    "type": "SOURCE_QUALIFIER",
                    "fields": [
                        {"name": "HK_RISK_ID", "datatype": "string", "expression": ""},
                        {"name": "RISK_METRIC", "datatype": "decimal", "expression": ""}
                    ]
                },
                {
                    "id": "exp_calc",
                    "name": "EXP_CALCULATE_SCORE",
                    "type": "EXPRESSION",
                    "fields": [
                        {"name": "HK_RISK_ID", "datatype": "string"},
                        {"name": "RISK_METRIC", "datatype": "decimal"},
                        {"name": "RISK_SCORE", "datatype": "decimal", "expression": "RISK_METRIC * 1.5"}
                    ]
                }
            ],
            "connections": [
                {"from": "DV_HUB_RISK", "to": "sq_1"},
                {"from": "sq_1", "to": "exp_calc"},
                {"from": "exp_calc", "to": "STG_OFSAA_RISK"}
            ],
            "target": {
                "name": "STG_OFSAA_RISK",
                "type": "TARGET",
                "fields": [
                    {"name": "HK_RISK_ID", "datatype": "string", "key": True},
                    {"name": "RISK_SCORE", "datatype": "decimal"}
                ]
            }
        },
        {
            "domain": "AS400",
            "pipeline_name": "AS400_CUSTOMER_SYNC",
            "description": "Syncs AS400 Customer Master to Data Warehouse Staging.",
            "source": {
                "name": "AS400_CUST_MST",
                "type": "SOURCE",
                "fields": [
                    {"name": "CUST_NO", "datatype": "integer", "key": True},
                    {"name": "CUST_NAME", "datatype": "string"},
                    {"name": "STATUS", "datatype": "string"}
                ]
            },
            "components": [
                {
                    "id": "sq_cust",
                    "name": "SQ_AS400_CUST_MST",
                    "type": "SOURCE_QUALIFIER",
                    "fields": [
                        {"name": "CUST_NO", "datatype": "integer"},
                        {"name": "CUST_NAME", "datatype": "string"},
                        {"name": "STATUS", "datatype": "string"}
                    ]
                },
                {
                    "id": "fil_active",
                    "name": "FIL_ACTIVE_CUST",
                    "type": "FILTER",
                    "condition": "STATUS = 'ACTIVE'",
                    "fields": [
                        {"name": "CUST_NO", "datatype": "integer"},
                        {"name": "CUST_NAME", "datatype": "string"}
                    ]
                }
            ],
            "connections": [
                {"from": "AS400_CUST_MST", "to": "sq_cust"},
                {"from": "sq_cust", "to": "fil_active"},
                {"from": "fil_active", "to": "STG_CUST_SYNC"}
            ],
            "target": {
                "name": "STG_CUST_SYNC",
                "type": "TARGET",
                "fields": [
                    {"name": "CUST_NO", "datatype": "integer", "key": True},
                    {"name": "CUST_NAME", "datatype": "string"}
                ]
            }
        },
        {
            "domain": "CARD400",
            "pipeline_name": "CARD400_TXN_EXTRACT",
            "description": "Extracts daily credit card transactions.",
            "source": {
                "name": "CARD_TXN",
                "type": "SOURCE",
                "fields": [
                    {"name": "TXN_ID", "datatype": "string", "key": True},
                    {"name": "CARD_NO", "datatype": "string"},
                    {"name": "AMOUNT", "datatype": "decimal"}
                ]
            },
            "components": [
                {
                    "id": "sq_txn",
                    "name": "SQ_CARD_TXN",
                    "type": "SOURCE_QUALIFIER",
                    "fields": [
                        {"name": "TXN_ID", "datatype": "string"},
                        {"name": "CARD_NO", "datatype": "string"},
                        {"name": "AMOUNT", "datatype": "decimal"}
                    ]
                },
                {
                    "id": "exp_mask",
                    "name": "EXP_MASK_CARD",
                    "type": "EXPRESSION",
                    "fields": [
                        {"name": "TXN_ID", "datatype": "string"},
                        {"name": "AMOUNT", "datatype": "decimal"},
                        {"name": "MASKED_CARD", "datatype": "string", "expression": "SUBSTR(CARD_NO, 1, 4) || '****' || SUBSTR(CARD_NO, -4)"}
                    ]
                }
            ],
            "connections": [
                {"from": "CARD_TXN", "to": "sq_txn"},
                {"from": "sq_txn", "to": "exp_mask"},
                {"from": "exp_mask", "to": "STG_CARD_TXN"}
            ],
            "target": {
                "name": "STG_CARD_TXN",
                "type": "TARGET",
                "fields": [
                    {"name": "TXN_ID", "datatype": "string", "key": True},
                    {"name": "MASKED_CARD", "datatype": "string"},
                    {"name": "AMOUNT", "datatype": "decimal"}
                ]
            }
        },
        {
            "domain": "CR2",
            "pipeline_name": "CR2_ATM_LOGS",
            "description": "Processes ATM Switch logs.",
            "source": {
                "name": "ATM_LOG",
                "type": "SOURCE",
                "fields": [
                    {"name": "LOG_ID", "datatype": "integer", "key": True},
                    {"name": "TERMINAL_ID", "datatype": "string"},
                    {"name": "EVENT_TYPE", "datatype": "string"}
                ]
            },
            "components": [
                {
                    "id": "sq_log",
                    "name": "SQ_ATM_LOG",
                    "type": "SOURCE_QUALIFIER",
                    "fields": [
                        {"name": "LOG_ID", "datatype": "integer"},
                        {"name": "TERMINAL_ID", "datatype": "string"},
                        {"name": "EVENT_TYPE", "datatype": "string"}
                    ]
                }
            ],
            "connections": [
                {"from": "ATM_LOG", "to": "sq_log"},
                {"from": "sq_log", "to": "STG_ATM_LOG"}
            ],
            "target": {
                "name": "STG_ATM_LOG",
                "type": "TARGET",
                "fields": [
                    {"name": "LOG_ID", "datatype": "integer", "key": True},
                    {"name": "TERMINAL_ID", "datatype": "string"},
                    {"name": "EVENT_TYPE", "datatype": "string"}
                ]
            }
        },
        {
            "domain": "T24",
            "pipeline_name": "T24_CORE_BANKING_HUB",
            "description": "Loads T24 Core Banking Accounts to Data Vault Hub.",
            "source": {
                "name": "STG_T24_ACCOUNT",
                "type": "SOURCE",
                "fields": [
                    {"name": "ACCT_ID", "datatype": "integer", "key": True}
                ]
            },
            "components": [
                {
                    "id": "sq_acct",
                    "name": "SQ_STG_T24_ACCOUNT",
                    "type": "SOURCE_QUALIFIER",
                    "fields": [
                        {"name": "ACCT_ID", "datatype": "integer"}
                    ]
                },
                {
                    "id": "exp_hash",
                    "name": "EXP_GENERATE_KEYS",
                    "type": "EXPRESSION",
                    "fields": [
                        {"name": "ACCT_ID", "datatype": "integer"},
                        {"name": "HK_ACCT_ID", "datatype": "string", "expression": "MD5(TO_CHAR(ACCT_ID))"}
                    ]
                }
            ],
            "connections": [
                {"from": "STG_T24_ACCOUNT", "to": "sq_acct"},
                {"from": "sq_acct", "to": "exp_hash"},
                {"from": "exp_hash", "to": "HUB_ACCOUNT"}
            ],
            "target": {
                "name": "HUB_ACCOUNT",
                "type": "TARGET",
                "fields": [
                    {"name": "HK_ACCT_ID", "datatype": "string", "key": True},
                    {"name": "ACCT_ID", "datatype": "integer"}
                ]
            }
        }
    ]

def explain_workflow(xml_content, pipeline_name):
    # Parse the XML to provide a good explanation
    try:
        root = ET.fromstring(xml_content)

        sources = [s.get('NAME') for s in root.findall('.//SOURCE')]
        targets = [t.get('NAME') for t in root.findall('.//TARGET')]
        transformations = []
        for t in root.findall('.//TRANSFORMATION'):
            t_name = t.get('NAME')
            t_type = t.get('TYPE')
            transformations.append(f"{t_name} ({t_type})")

        explanation = f"# Workflow Explanation: {pipeline_name}\n\n"
        explanation += "Hello! I am your Workflow Explanation Partner. Let's break down this ETL pipeline.\n\n"

        explanation += "## 🏢 Sources\n"
        if sources:
            for s in sources:
                explanation += f"- **{s}**: This is where we extract our initial data.\n"
        else:
            explanation += "- No sources found.\n"

        explanation += "\n## 🔄 Transformations\n"
        if transformations:
            explanation += "The data flows through the following transformations, where business logic is applied:\n"
            for t in transformations:
                explanation += f"- **{t}**\n"
        else:
            explanation += "- No transformations found.\n"

        explanation += "\n## 🎯 Targets\n"
        if targets:
            explanation += "Finally, the processed data is loaded into the following targets:\n"
            for t in targets:
                explanation += f"- **{t}**: This is the destination of our pipeline.\n"
        else:
            explanation += "- No targets found.\n"

        explanation += "\n---\n*This explanation was automatically generated by your Workflow Explanation Partner.*"
        return explanation
    except Exception as e:
        return f"# Workflow Explanation Error\n\nCould not parse XML for {pipeline_name}: {e}"

def main():
    base_dir = "workflows"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    pipelines = get_complex_pipelines()

    for pipeline in pipelines:
        domain = pipeline.get("domain", "UNKNOWN")
        domain_dir = os.path.join(base_dir, domain)
        if not os.path.exists(domain_dir):
            os.makedirs(domain_dir)

        pipeline_name = pipeline.get("pipeline_name", "pipeline")

        # 1. Generate XML
        try:
            xml_content = generate_powercenter_xml(pipeline, database_type="Oracle")
            xml_filename = os.path.join(domain_dir, f"{pipeline_name}.xml")
            with open(xml_filename, "w", encoding="utf-8") as f:
                f.write(xml_content)
            print(f"Generated XML: {xml_filename}")

            # 2. Read and explain
            with open(xml_filename, "r", encoding="utf-8") as f:
                read_xml_content = f.read()

            explanation = explain_workflow(read_xml_content, pipeline_name)
            md_filename = os.path.join(domain_dir, f"{pipeline_name}_explanation.md")
            with open(md_filename, "w", encoding="utf-8") as f:
                f.write(explanation)
            print(f"Generated Explanation: {md_filename}")

        except Exception as e:
            print(f"Error processing {pipeline_name}: {e}")

if __name__ == "__main__":
    main()
