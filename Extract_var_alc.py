import xml.etree.ElementTree as ET
import pandas as pd
import os
from tqdm import tqdm  # Import tqdm for progress visualization
import re

###############------ CHANGE THIS ------###############
input_file = r"D:\chnge order\FLISR\SERVICE ENGINE\ARAR\SLD_TURIF_OFFICE\RT\FILES\zenon\system\alc.XML"
output_folder = r"D:\Zenon py\Line follower\FLISR\outputs"
###############------ CHANGE THIS ------###############

output_excel = r"{}\alc_machine_var.xlsx".format(output_folder)


print("="*40)
print("  Extracting Machine Names and Variables from XML of ALC")
print("="*40)

# List of prefixes to ignore
ignore_prefixes = [
    
    # "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
    # "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_NON_SMT_SLD_LBS",
    # "INTEGRATION_PROJECT_SMT_SECTIONALIZER",
    # "INTEGRATION_PROJECT_SMT_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_SMT_SLD_LBS",

    # "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
    # "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
    # "INTEGRATION_PROJECT_SMART_SECTIONALIZER",
    # "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_SMART_SLD_LBS",

    "INTEGRATION_PROJECT_SLD_FDR_DSS_1_SEC",
    "INTEGRATION_PROJECT_OH_TRANSFORMER",
    "INTEGRATION_PROJECT_SMART_VOLTAGE",
    "INTEGRATION_PROJECT_NON_SMART_CB",
    "INTEGRATION_PROJECT_SMART_RMU_CB",
    "INTEGRATION_PROJECT_TRANSFORMER",
    "INTEGRATION_PROJECT_Graph_fuse",
    "INTEGRATION_PROJECT_Capacitor",
    "INTEGRATION_PROJECT_SMART_CB",
    "INTEGRATION_PROJECT_NON_ALC",
    "INTEGRATION_PROJECT_PACKEG",
    "INTEGRATION_PROJECT_METER",
    "INTEGRATION_PROJECT_ALARM",
    "INTEGRATION_PROJECT_GRAPH",
    "INTEGRATION_PROJECT_CABLE",
    "INTEGRATION_PROJECT_FUSE",
    "1ELBO_1F_1L_2L_2ELBO_NON",
    "2ELBO_2F_2L_2L_2ELBO_NON",
    "INTEGRATION_PROJECT_UG",
    "Faultcable_button",
    "Combined element",
    "SLD_TERMINATOR",
    "Element group",
    "Info_button",
    "1ELBO_1FUSE",
    "Static text",
    "TERMINATOR",
    "JMP_button",
    "ING_OH_PMT",
    "SLD_SOURCE",
    "Rectangle",
    "LINK_SLD",
    "ALC_LBS",
    "Static",
    "ALC_CB",
    "OH_PMT",
    "1FUSE",
    "Line",
    "CB"
]

special_prefixes = [
    "INTEGRATION_PROJECT_SLD_FDR",
    "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMT_SLD_LBS",
    "INTEGRATION_PROJECT_SMT_SECTIONALIZER",
    "INTEGRATION_PROJECT_SMT_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_SMT_SLD_LBS",

    "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
    "INTEGRATION_PROJECT_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_SMART_SLD_LBS"
]

smart_symbol_names = set([
    "INTEGRATION_PROJECT_SLD_FDR_DSS_1_DOWN_ALIAS",
    "INTEGRATION_PROJECT_SMART_SECTIONALIZER_SCREEN",
    "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER_SCREEN",
    "INTEGRATION_PROJECT_SMART_SLD_LBS_SCREEN_1",
    "INTEGRATION_PROJECT_SMART_SLD_LBS_SCREEN",
    "ALC_LBS_LEFT_1_2",
    "ALC_LBS_LEFT_1_1_1_1_1",
    "ALC_LBS_LEFT_1_1_1_1",
    "ALC_LBS_LEFT_1_1_1",
    "ALC_LBS_1_3",
    "ALC_CB_1_1_2_1",
    "ALC_CB_1_1_1_1",
    "ALC_CB_1_1_3",
    "INTEGRATION_PROJECT_SMART_RMU_CB",
    "INTEGRATION_PROJECT_SMART_CB_SLD",
    "3L1T_SMART_RMU_TR_HOR_L_V2_2ND",
    "3L1T_SMART_RMU_TR_HOR_R_V1_2ND",
    "3L1T_SMART_RMU_TR_HOR_L_V1_2ND",
    "3L_SMART_RMU_VERT_DOWN_V1",
    "3L1T_4_1_2_1_1_2",
    "3L1T_4_1_2_1_1",
    "3L1T_3_1_2_1_1_2",
    "3L1T_3_1_2_1_1",
    "3L1T_1_1_2_1_1_2",
    "3L1T_1_1_2_1_1_3",
    "3L1T_1_1_2_1_1",
    "3L1T_2_1_2_1_1_2",
    "3L1T_2_1_2_1_1",
    "3L1T_3_1_1_1_1",
    "3L1T_3_2_1_1",
    "3L1T_4_1_1_1_1",
    "3L1T_4_2_1_1",
    "3L1T_2_1_1_1_1",
    "3L1T_2_2_1_1",
    "3L1T_1_1_1_1_1",
    "3L1T_1_2_1_1",
    "3L1T_3_1_1",
    "3L1T_3_2",
    "3L1T_4_1_1",
    "3L1T_4_2",
    "3L1T_3_1_1_1",
    "3L1T_3_2_1",
    "3L1T_4_1_1_1",
    "3L1T_4_2_1",
    "3L1T_2_1_1_1",
    "3L1T_2_2_1",
    "3L1T_1_1_1_1",
    "3L1T_1_2_1",
    "3L1T_2_1_1",
    "3L1T_2_2",
    "3L1T_1_1_1_2",
    "3L1T_1_1_1",
    "3L1T_1_2",
    "3L1T_4_1_2_1",
    "3L1T_3_1_2_1",
    "3L1T_1_1_2_1_3",
    "3L1T_1_1_2_1_2_1_1",
    "3L1T_1_1_2_1_2_1",
    "3L1T_1_1_2_1_2",
    "3L1T_1_1_2_1",
    "3L1T_2_1_2_1_2",
    "3L1T_2_1_2_1",
    "2L2T_SMART_RMU_W_TR_VERT_D_V2",
    "2L2T_SMART_RMU_W_TR_VERT_D_V1",
    "2L2T_SMART_RMU_W_TR_HOR_R_V2",
    "2L2T_SMART_RMU_W_TR_HOR_R_V1",
    "2L2T_SMART_RMU_W_TR_HOR_L_V2",
    "2L2T_SMART_RMU_W_TR_HOR_L_V1",
    "2L2T_2_1_1",
    "2L2T_2_2",
    "2L2T_4_1_2",
    "2L2T_4_1_1_1",
    "2L2T_2_1_1_1",
    "2L2T_2_2_1",
    "2L2T_3_1_1",
    "2L2T_3_2",
    "2L2T_1_1_1",
    "2L2T_1_2",
    "MSRMU_2L2T_V3_1_1_1_1",
    "MSRMU_2L2T_V4_1",
    "MSRMU_2L2T_V3_1",
    "MSRMU_2T1L_V1_1_1",
    "MSRMU_2L1T_V1_1",
    "MSRMU_2L2T_V2_2",
    "MSRMU_2L2T_V4_2",
    "MSRMU_2L2T_V1_3",
    "2L1T_4_2",
    "2L1T_4_1_1",
    "2L1T_3_2",
    "2L1T_3_1_1",
    "2L1T_4_3_2",
    "2L1T_4_1_2_1_1",
    "2L1T_3_1_2_1_1",
    "2L1T_3_3_1_1",
    "2L1T_2_1_2_1_1",
    "2L1T_2_3_1_1",
    "2L1T_1_1_2_1_1",
    "2L1T_1_3_1_1",
    "2L1T_4_3_2_2",
    "2L1T_4_1_2_1_1_2",
    "2L1T_3_1_2_1_1_2_1",
    "2L1T_3_1_2_1_1_3",
    "2L1T_2_1_2_1_1_2",
    "2L1T_2_3_1_1_2",
    "2L1T_1_1_2_1_1_3",
    "2L1T_1_3_1_1_2",
    "2L1T_2_1_1",
    "2L1T_2_2",
    "2L1T_1_1_1",
    "2L1T_1_2",
    "2L1T_4_2_1",
    "2L1T_4_1_1_1",
    "2L1T_3_2_1",
    "2L1T_3_1_1_1",
    "2L1T_2_1_1_1",
    "2L1T_2_2_1",
    "2L1T_1_1_1_1",
    "2L1T_1_2_2",
    "2L1T_4_3",
    "2L1T_4_1_2_1",
    "2L1T_3_3_1",
    "2L1T_3_1_2_1",
    "2L1T_2_1_2_1",
    "2L1T_2_3_1",
    "2L1T_1_1_2_1",
    "2L1T_1_3_1",
    "3L1T_4_2_2",
    "3L1T_4_1_1_2",
])



tree = ET.parse(input_file)
root = tree.getroot()

output_rows = []
unique_keys = set()

def is_ignored(machine_name):
    return any(str(machine_name).startswith(prefix) for prefix in ignore_prefixes)

# Get all graph elements once
graph_elements = root.findall(".//GraphElement")
print(f"Found {len(graph_elements)} graph elements")

# Build efficient lookups
print("Building lookup tables...")
variable_lookup = {}  # (picture, machine_id) -> variable
element_refs = []  # Store all element refs for missing key detection

for graph_element in tqdm(graph_elements, desc="Building lookups"):
    picture = graph_element.findtext("Picture", "Unknown_Picture")
    element_ref = graph_element.findtext("ElementRef", "")
    element_type = graph_element.findtext("Type", "")
    variable = graph_element.findtext("Variable", "")
    
    if not element_ref:
        continue
        
    parts = element_ref.split(".")
    if len(parts) < 2:
        continue
        
    element_id = parts[1]
    
    # Store for missing key detection
    if not is_ignored(element_id):
        element_refs.append((picture, element_id))
    
    # Build variable lookup for types 2 and 7 with valid variables
    if (element_type in ["2", "7"] and 
        variable and variable != "<No variable linked>" and 
        (variable.endswith("OC_ST") or "FDR" in element_id)):
        
        key = (picture, element_id)
        if key not in variable_lookup:
            variable_lookup[key] = variable

print("Processing main elements...")
processed_keys = set()

for graph_element in tqdm(graph_elements, desc="Processing Elements"):
    element_ref = graph_element.findtext("ElementRef", "")
    picture = graph_element.findtext("Picture", "")
    
    if not element_ref:
        continue
        
    parts = element_ref.split(".")
    if len(parts) < 2:
        continue
        
    element_id = parts[1]
    
    if is_ignored(element_id):
        continue
        
    key = (picture, element_id)
    if key in processed_keys:
        continue
        
    # Look up variable
    variable = variable_lookup.get(key, "<No variable linked>")
    
    if variable == "<No variable linked>":
        continue
        
    # Process variable to get base
    variable_base = "-"
    if "#" not in variable and "." in variable:  # OLD
        variable_base = variable.rsplit(".", 1)[0]
    elif "#" in variable and ("_Y1" in variable or "_Y2" in variable or "_Y3" in variable or "_Y4" in variable or "_Y5" in variable):  # SMART
        variable_base = variable.split("_Y", 1)[0]
    elif "#" in variable and "_TR_" in variable:  # SMART
        variable_base = variable.split("_TR_", 1)[0]
    elif "#" in variable and "_Y" not in variable and "_TR_" not in variable and not "#ICCP_" in variable:  # FDR
        variable_base = variable.split("_OC_", 1)[0] if "_OC_" in variable else variable.split("_CB_", 1)[0]
    
    if variable_base == "-":
        continue
        
    # Extract machine info
    station = "-"
    feeder_name = "-"
    machine_name = variable_base.split("_")[-1] if "_" in variable_base else "-"
    
    if machine_name != "-" and not any(char.isdigit() for char in machine_name):
        machine_name = "_".join(variable_base.split("_")[-2:]) if "_" in variable_base else "-"
    
    # Determine if SMART
    smart = "NON SMART"
    if "#" in variable_base and any(element_id.startswith(prefix) for prefix in smart_symbol_names):
        smart = "SMART"
    else:
        smart = "NON SMART"
        if "SMART" in element_id and "NON_SMART" not in element_id and "RMU" in element_id:
            smart = "SMART"
    
    if element_id.startswith("INTEGRATION_PROJECT_SLD_FDR"):  #######################
        smart = "SMART"
        if "#" in variable_base and "#ICCP_" not in variable_base:
            station = variable_base.split("#")[1]
            feeder_name = variable_base.split("#")[1]
    
    if element_id.startswith("INTEGRATION_PROJECT_SMART") or element_id.startswith("INTEGRATION_PROJECT_SMT"):
        smart = "SMART"
    
    output_rows.append({
        "ScreenName": picture,
        "ID": element_id,
        "VisualName": machine_name,
        "SMART": smart,
        "Variable": variable_base,
        "Station": station,
        "FeederNo": feeder_name,
    })
    processed_keys.add(key)

print("Adding missing elements...")
# Find missing keys and add them
missing_keys = set(element_refs) - processed_keys
for picture, element_id in missing_keys:
    output_rows.append({
        "ScreenName": picture,
        "ID": element_id,
        "VisualName": element_id,
        "SMART": "NON SMART",
        "Variable": "-",
        "Station": "-",
        "FeederNo": "-",
    })

df = pd.DataFrame(output_rows)
df.drop_duplicates(subset=["ScreenName", "ID"], inplace=True)
df.to_excel(output_excel, index=False)
print(f"The make table exported to {output_excel}")
