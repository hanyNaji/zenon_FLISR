import xml.etree.ElementTree as ET
import pandas as pd
import re


def run(scr_xml_file, output_folder, use_scr_xml):
    if not use_scr_xml:
        print("Screen XML extraction is not enabled.")
        return
    nvar_excel = r"{}\alc_machine_var.xlsx".format(output_folder)
    output_excel = r"{}\scr_machine_var.xlsx".format(output_folder)

    print()
    print("="*40)
    print("  Extracting Machine Names and Variables from XML of SCREENS")
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
        # "INTEGRATION_PROJECT_NON_SMART_CB",
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
        "INTEGRATION_PROJECT_ALC",
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
        "AR_OPEN",
        "Static",
        "ALC_CB",
        "OH_PMT",
        "1FUSE",
        "Line",
        "CB"
    ]


    special_prefixes = [
        "INTEGRATION_PROJECT_SLD_FDR",
        "INTEGRATION_PROJECT_NON_SMART_CB_SLD",
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

    # Parse the XML file
    tree = ET.parse(scr_xml_file)
    root = tree.getroot()

    # Load nvar_excel as DataFrame
    nvar_df = pd.read_excel(nvar_excel)

    # Build a dictionary of all TYPE=130 names for fast lookup
    vname_dict = {}
    picture_dict = {}
    variable_dict = {}

    data = []

    # Build a lookup from XML for (Picture, ID) -> (VisualName, SubstituteDestination, Smart)
    xml_lookup = {}

    for picture in root.findall(".//Picture"):
        screen_name = picture.get("ShortName", "")
        for element in picture.findall(".//*[@TYPE='130']"):
            name_node = element.find("Name")
            if name_node is not None:
                name_text = name_node.text
                if any(name_text.startswith(prefix) for prefix in ignore_prefixes):
                    continue
            visual_name_node = element.find("VisualName")
            sub_dest_node = element.find("SubstituteDestination")
            link_name_node = element.find("LinkName")
            Stationname_node = element.find("Stationname")

            visual_name = visual_name_node.text if visual_name_node is not None else "-"
            sub_dest = sub_dest_node.text if sub_dest_node is not None and sub_dest_node.text is not None else "-"
            link_name = link_name_node.text if link_name_node is not None else ""
            id_val = name_node.text if name_node is not None else "-"

            # Extract only the first part after '#' and before ';'
            if "#" in sub_dest:
                sub_dest = sub_dest.split(";", 1)[0]

            smart = "NON SMART"
            if link_name in smart_symbol_names:
                smart = "SMART"
            else:
                smart = "NON SMART"
                if "SMART" in id_val and "NON_SMART" not in id_val and "RMU" in id_val:
                    smart = "SMART"
                if "FDR" in id_val or "INTEGRATION_PROJECT_NON_SMART_CB_SLD" in id_val:
                    smart = "SMART"
            # also if id contains 'NON_SMART' or 'SMART' in id_val, set smart accordingly
            

            station = "-"
            feeder_name = "-"

            
            
            
            if "FDR" in id_val or "INTEGRATION_PROJECT_NON_SMART_CB_SLD" in id_val:
                feeder_name = sub_dest.split("#")[1] if "#" in sub_dest else sub_dest
                feeder_name = feeder_name.split("_CB")[0] if "_CB" in feeder_name else feeder_name
                feeder_name = feeder_name.split("_OC")[0] if "_OC" in feeder_name else feeder_name
                station = feeder_name
                station = station.replace("ICCP_", "")
                station = station.replace("PTG_", "")
                station = feeder_name.split("_")[0] if "_" in feeder_name else feeder_name
            
            # if "FDR" in id_val or "INTEGRATION_PROJECT_NON_SMART_CB_SLD" in id_val:  #DAMMAM
            #     # Extract station and feeder_name from ExpProps
            #     for exp_prop in element.findall(".//ExpProps_0"):
            #         name_elem = exp_prop.find("Name")
            #         value_elem = exp_prop.find("ExpPropValue")
            #         if (name_elem is not None and "TEXT.Text" in name_elem.text and 
            #             value_elem is not None and value_elem.text is not None):
            #             # Extract text between <Text> tags
            #             text_match = re.search(r'<Text>(.*?)</Text>', value_elem.text)
            #             if text_match:
            #                 station = text_match.group(1)
                
            #     for exp_prop in element.findall(".//ExpProps_1"):
            #         name_elem = exp_prop.find("Name")
            #         value_elem = exp_prop.find("ExpPropValue")
            #         if (name_elem is not None and "Static text_1.Text" in name_elem.text and 
            #             value_elem is not None and value_elem.text is not None):
            #             # Extract text between <Text> tags
            #             text_match = re.search(r'<Text>(.*?)</Text>', value_elem.text)
            #             if text_match:
            #                 feeder_name = text_match.group(1)
            #                 # remove any whitespace without affecting the rest of the string
            #                 feeder_name = re.sub(r'\s+', '', feeder_name)
            #                 #  & if name must start with 'B'
            #                 if not feeder_name.startswith('B'):
            #                     feeder_name = 'B' + feeder_name
            #                 # remove anything except 'B', digits, and "()". only 1 'B'
            #                 feeder_name = re.sub(r'[^B\d()]', '', feeder_name)
            #                 feeder_name = re.sub(r'B+', 'B', feeder_name)
            #                 # if only 1 digit inside the parentheses, add '0' before it inside parentheses
            #                 if re.search(r'\(\d\)', feeder_name):
            #                     feeder_name = re.sub(r'\((\d)\)', r'(0\1)', feeder_name)

            # if "FDR" in id_val or "INTEGRATION_PROJECT_NON_SMART_CB_SLD" in id_val:   #DWADMI
            #     station = Stationname_node.text if Stationname_node is not None else "-"
            #     feeder_name = Stationname_node.text if Stationname_node is not None else "-"

            xml_lookup[(screen_name, id_val)] = {
                "VisualName": visual_name,
                "Variable": sub_dest,
                "SMART": smart,
                "Station": station,
                "FeederNo": feeder_name
            }

    # Now fill missing VisualName in nvar_df using xml_lookup
    for idx, row in nvar_df.iterrows():
        key = (row["ScreenName"], str(row["ID"]))
        if key in xml_lookup:
            nvar_df.at[idx, "SMART"] = xml_lookup[key]["SMART"]
            nvar_df.at[idx, "VisualName"] = xml_lookup[key]["VisualName"] if xml_lookup[key]["VisualName"] != "-" else None
            
            # # Add Station and FeederNo columns if they don't exist
            # if "Station" not in nvar_df.columns:
            #     nvar_df["Station"] = "-"
            # if "FeederNo" not in nvar_df.columns:
            #     nvar_df["FeederNo"] = "-"
            # # Update Station and FeederNo from XML
            # nvar_df.at[idx, "Station"] = xml_lookup[key]["Station"]
            # nvar_df.at[idx, "FeederNo"] = xml_lookup[key]["FeederNo"]
            
            if row["Variable"] == "-":
                nvar_df.at[idx, "VisualName"] = xml_lookup[key]["VisualName"]
                nvar_df.at[idx, "Variable"] = xml_lookup[key]["Variable"]


    # Replace empty cells with a dash
    nvar_df.fillna("-", inplace=True)
    nvar_df.replace("", "-", inplace=True)

    # Save to Excel
    nvar_df.drop_duplicates(subset=["ScreenName", "ID"], inplace=True)
    nvar_df.to_excel(output_excel, index=False)
    print(f"Excel written to {output_excel}")

    #######################################################

