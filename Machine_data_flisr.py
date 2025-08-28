import pandas as pd
from thefuzz import process
from tqdm import tqdm  # Import tqdm for progress visualization
import numpy as np
import ast
import re



def run(output_file, output_folder, project_name, office_name, use_scr_xml):
    if use_scr_xml:
        var_df = pd.read_excel(r"{}\scr_machine_var.xlsx".format(output_folder))
    else:
        var_df = pd.read_excel(r"{}\alc_machine_var.xlsx".format(output_folder))
    # Load your files
    file2 = r"{}\alc_DB_FLIS_with_feeder.xlsx".format(output_folder)

    print()
    print("="*40)
    print("  Extracting Machine Restoration and Finishing up")
    print("="*40)
    

    # df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)

    # Ensure required columns exist before use
    required_cols = [
        'Feeder OCT Variable', 'Feeder CMD Variable',
        'Station', 'FeederNo',
        'Restoration OCT Variables', 'Restoration CMD Variables'
    ]
    for _col in required_cols:
        if _col not in df2.columns:
            df2[_col] = '-'

    # get feeder variables, Station, FeederNo using picture and feeder_id from var_df
    for idx, row in df2.iterrows():
        picture = str(row['Picture'])
        feederID = row['feeder_id']
        # feederNO = row['FeederNo']
        if pd.isna(picture) or picture == "-":
            continue

        if not pd.isna(feederID) and feederID != "-":
            # Use the picture (ScreenName) , feederID (ID) to find the corresponding row in var_df
            matches = var_df[(var_df['ScreenName'] == picture) & (var_df['ID'] == feederID)]
            # If picture matches ScreenName and feederID matches ID, assign the values
            if not matches.empty:
                var_val = matches['Variable'].values[0] if 'Variable' in matches else None
                sta_val = matches['Station'].values[0] if 'Station' in matches else None
                fno_val = matches['FeederNo'].values[0] if 'FeederNo' in matches else None

                # Feeder Variables
                if pd.notna(var_val) and str(var_val) != "-":
                    if var_val.endswith("_FDR"):
                        var_val = var_val.replace("_FDR", "")
                        if var_val.endswith("_OC_ST"):
                            df2.at[idx, 'Feeder OCT Variable'] = var_val
                            df2.at[idx, 'Feeder CMD Variable'] = var_val.replace("_OC_ST", "_OC_CMD")
                        elif var_val.endswith("_CB_IND"):
                            df2.at[idx, 'Feeder OCT Variable'] = var_val
                            df2.at[idx, 'Feeder CMD Variable'] = var_val.replace("_CB_IND", "_CB_CMD")
                        elif "CB_ST" in var_val:
                            df2.at[idx, 'Feeder OCT Variable'] = var_val
                            df2.at[idx, 'Feeder CMD Variable'] = var_val.replace("CB_ST", "_CB_CMD")
                    elif var_val.endswith("_LFDR"):
                        var_val = var_val.replace("_LFDR", "")
                        if var_val.endswith("_OC_ST"):
                            df2.at[idx, 'Feeder OCT Variable'] = project_name + var_val
                            df2.at[idx, 'Feeder CMD Variable'] = project_name + var_val.replace("_OC_ST", "_OC_CMD")
                        elif var_val.endswith("_CB_IND"):
                            df2.at[idx, 'Feeder OCT Variable'] = project_name + var_val
                            df2.at[idx, 'Feeder CMD Variable'] = project_name + var_val.replace("_CB_IND", "_CB_CMD")
                        elif "CB_ST" in var_val:
                            df2.at[idx, 'Feeder OCT Variable'] = project_name + var_val
                            df2.at[idx, 'Feeder CMD Variable'] = project_name + var_val.replace("CB_ST", "_CB_CMD")
                    else:
                        df2.at[idx, 'Feeder OCT Variable'] = "NO VARIABLE"
                        df2.at[idx, 'Feeder CMD Variable'] = "NO VARIABLE"
                else:
                    df2.at[idx, 'Feeder OCT Variable'] = "NO VARIABLE"
                    df2.at[idx, 'Feeder CMD Variable'] = "NO VARIABLE"

                df2.at[idx, 'Station'] = sta_val if pd.notna(sta_val) and str(sta_val).strip() != "-" else "-"
                df2.at[idx, 'FeederNo'] = fno_val if pd.notna(fno_val) and str(fno_val).strip() != "-" else "-"

    # Replace empty cells with a dash
    df2.fillna("-", inplace=True)
    df2.replace("", "-", inplace=True)

    df2.drop_duplicates(subset=["Picture", "ID"], inplace=True)

    ###################### Isolation, Location #####################

    for idx, row in tqdm(df2.iterrows(), total=df2.shape[0], desc="Finishing up 1/2"):
        after_eq = ""
        for i in range(2, 8):
            con = row[f"Con{i}"]
            if con == "-":
                break
            if con.endswith("_FDR"):
                con = con.replace("_FDR", "")
                if con.endswith("_OC_ST"):
                    after_eq = (after_eq + con.replace("_OC_ST", "_EF_ST")+ ",")
                elif con.endswith("_CB_IND"):
                    after_eq = (after_eq + con.replace("_CB_IND", "")+ ",")##@hanyNaji
                elif "CB_ST" in con:
                    after_eq = (after_eq + con.replace("_CB_ST", "")+ ",")
            elif con.endswith("_LFDR"): ##@hanyNaji
                con = con.replace("_LFDR", "")
                if con.endswith("_OC_ST"):
                    after_eq = (after_eq + project_name + con.replace("_OC_ST", "")+ ",")
                elif con.endswith("_CB_IND"):
                    after_eq = (after_eq + project_name + con.replace("_CB_IND", "")+ ",")
                elif "CB_ST" in con:
                    after_eq = (after_eq + project_name + con.replace("_CB_ST", "")+ ",")

            elif con.endswith("_SRECSEC"):
                con = con.replace("_SRECSEC", "")
                after_eq = (after_eq + con.replace("_OC_ST", "_E_FLT")+ ",")
            elif con.endswith("_SLBS"):
                con = con.replace("_SLBS", "")
                after_eq = (after_eq + con.replace("_OC_ST", "_EFI_ST")+ ",")

            else:
                after_eq = (after_eq + project_name + con + ",") if "." in con else (after_eq + con + ",")
            df2.at[idx, "After Equipments EF Variables"] = after_eq

        df2.at[idx, "Machine"] = (project_name + row["Machine"]) if "." in row["Machine"] else row["Machine"]

        con1 = row["Con1"]
        if con1.endswith("_FDR"):
            con1 = con1.replace("_FDR", "")
            if con1.endswith("_OC_ST"):
                df2.at[idx, "Con1"] = con1.replace("_OC_ST", "_EF_ST")
            elif con1.endswith("_CB_IND"):
                df2.at[idx, "Con1"] = con1.replace("_CB_IND", "")
            elif "CB_ST" in con1:
                df2.at[idx, "Con1"] = con1.replace("_CB_ST", "")
        elif con1.endswith("_LFDR"):
            con1 = con1.replace("_LFDR", "")
            if con1.endswith("_OC_ST"):
                df2.at[idx, "Con1"] = project_name + con1.replace("_OC_ST", "")
            elif con1.endswith("_CB_IND"):
                df2.at[idx, "Con1"] = project_name + con1.replace("_CB_IND", "")
            elif "CB_ST" in con1:
                df2.at[idx, "Con1"] = project_name + con1.replace("_CB_ST", "")
        elif con1.endswith("_SRECSEC"):
            con1 = con1.replace("_SRECSEC", "")
            df2.at[idx, "Con1"] = con1.replace("_OC_ST", "_E_FLT")
        elif con1.endswith("_SLBS"):
            con1 = con1.replace("_SLBS", "")
            df2.at[idx, "Con1"] = con1.replace("_OC_ST", "_EFI_ST")
        else:
            df2.at[idx, "Con1"] = (project_name + con1) if "." in con1 else con1

        iso_oct = ""
        iso_cmd = ""
        for i in range(1, 8):
            iso = row[f"ISO{i}"]
            if iso == "-":
                break
            if iso.endswith("_FDR"):
                iso = iso.replace("_FDR", "")
                if iso.endswith("_OC_ST"):
                    iso_oct = iso_oct + iso + ","
                    iso_cmd = iso_cmd + iso.replace("_OC_ST", "_OC_CMD") + ","
                elif iso.endswith("_CB_IND"):
                    iso_oct = iso_oct + iso + ","
                    iso_cmd = iso_cmd + iso.replace("_CB_IND", "_CB_CMD") + ","
                elif "CB_ST" in iso:
                    iso_oct = iso_oct + iso + ","
                    iso_cmd = iso_cmd + iso.replace("_CB_ST", "_CB_CMD") + ","
            elif iso.endswith("_LFDR"):
                iso = iso.replace("_LFDR", "")
                if iso.endswith("_OC_ST"):
                    iso_oct = iso_oct + project_name + iso + ","
                    iso_cmd = iso_cmd + project_name + iso + "," #@hanyNaji
                elif iso.endswith("_CB_IND"):
                    iso_oct = iso_oct + project_name + iso + ","
                    iso_cmd = iso_cmd + project_name + iso + "," #@hanyNaji
                elif "CB_ST" in iso:
                    iso_oct = iso_oct + project_name + iso + ","
                    iso_cmd = iso_cmd + project_name + iso + "," #@hanyNaji
            elif iso.endswith("_SRECSEC"):
                iso = iso.replace("_SRECSEC", "")
                iso_oct = iso_oct + iso + ","
                iso_cmd = iso_cmd + iso.replace("_OC_ST", "_OPN_CMD") + ","
            elif iso.endswith("_SLBS"):
                iso = iso.replace("_SLBS", "")
                iso_oct = iso_oct + iso + ","
                iso_cmd = iso_cmd + iso.replace("_OC_ST", "_OC_CMD") + ","
            elif "#" not in iso:
                    iso_oct = iso_oct + project_name + iso + ","
                    iso_cmd = iso_cmd + project_name + iso.replace("_ST", "_CMD") + ","
            else:
                iso_oct = iso_oct + iso + "_OC_ST" + ","
                iso_cmd = iso_cmd + iso + "_OC_CMD" + ","

            df2.at[idx, "Isolation OCT Variables"] = iso_oct
            df2.at[idx, "Isolation CMD Variables"] = iso_cmd
        # Initialize Restoration OCT and CMD Variables
        df2.at[idx, "Restoration OCT Variables"] = row['Feeder OCT Variable']
        df2.at[idx, "Restoration CMD Variables"] = row['Feeder CMD Variable']


    def _to_list(val):
        """Normalize isolation vars into a list of strings."""
        if pd.isna(val):
            return []
        if isinstance(val, (list, tuple, set)):
            return [str(x).strip() for x in val if str(x).strip()]
        s = str(val).strip()
        if not s or s == "-":
            return []
        # If Excel reloaded a Python-list-as-string like "['A','B']"
        if s.startswith('[') and s.endswith(']'):
            try:
                parsed = ast.literal_eval(s)
                if isinstance(parsed, (list, tuple, set)):
                    return [str(x).strip() for x in parsed if str(x).strip()]
            except Exception:
                pass
        # Comma-separated string
        return [t for t in (x.strip() for x in s.split(',')) if t]

    ###### Isolation fix for smart machines ######
    """
        Processes only rows with SMART == "SMART".
        If machine is in last machines in feeder, or Equipment Index invalid, or no next machine found → set both isolation fields to "-" and skip.
        “Next machines” = same Picture, same feeder_id, and Equipment Index = current + 1; also must be in Location Equipments IDs.
        Normalizes isolation fields to lists via _to_list(...) for both current and next rows.
        For SMART next machines:
        If machine_vname appears in next’s vars → append the matching nxt_oct[i]/nxt_cmd[i].
        If nxt_vname appears in current vars → append the matching cur_oct[i]/cur_cmd[i].
        For non-SMART next machines: append all of their nxt_oct/nxt_cmd.
        Skips candidates with empty nxt_oct.
        Writes back only if something was collected; values are comma-joined, empties dropped, no dedup.
    """
    for idx, row in df2.iterrows():
        picture = str(row['Picture'])
        machine_id = str(row['ID'])
        machine_vname = str(row['VisualName'])
        machine_type = str(row['SMART'])
        machine_index = row.get('Equipment Index', np.nan)
        feeder_id = row.get('feeder_id', '-')

        # Skip non-SMART rows early
        if machine_type != "SMART":
            continue

        # If this machine is one of the last in its feeder -> set "-" and move on
        last_in_feeder = _to_list(row['last machines in feeder'])
        if machine_id in last_in_feeder:
            df2.at[idx, "Isolation OCT Variables"] = "-"
            df2.at[idx, "Isolation CMD Variables"] = "-"
            continue

        # Need a valid numeric Equipment Index to search for the "next" machine
        try:
            next_index = int(machine_index) + 1
        except (TypeError, ValueError):
            df2.at[idx, "Isolation OCT Variables"] = "-"
            df2.at[idx, "Isolation CMD Variables"] = "-"
            continue

        # Find the next machine within same Picture and feeder_id
        candidates = df2[
            (df2['Picture'].astype(str) == picture) &
            (df2['feeder_id'].astype(str) == str(feeder_id)) &
            (df2['Equipment Index'] == next_index)
        ]

        if candidates.empty:
            # No next machine found
            df2.at[idx, "Isolation OCT Variables"] = "-"
            df2.at[idx, "Isolation CMD Variables"] = "-"
            continue
        
        loc_ids = _to_list(row['Location Equipments IDs'])

        # Collect results from all matching next machines
        oct_vals, cmd_vals = [], []

        # Current machine variables (always normalized to list)
        cur_oct = _to_list(row.get("Isolation OCT Variables"))
        cur_cmd = _to_list(row.get("Isolation CMD Variables"))

        for _, nxt in candidates.iterrows():
            nxt_id = str(nxt.get('ID', ''))
            nxt_vname = str(nxt.get('VisualName', ''))

            if nxt_id not in loc_ids:
                continue

            nxt_oct = _to_list(nxt.get("Isolation OCT Variables", []))
            nxt_cmd = _to_list(nxt.get("Isolation CMD Variables", []))
            
            if not nxt_oct or nxt_oct == "-":
                continue

            if str(nxt.get('SMART', '')) == "SMART":
                # Cross-check using VisualName
                for i, var in enumerate(nxt_oct):
                    if machine_vname in var:
                        oct_vals.extend([nxt_oct[i]])
                        cmd_vals.extend([nxt_cmd[i]])
                for i, var in enumerate(cur_oct):
                    if nxt_vname in var:
                        oct_vals.extend([cur_oct[i]])
                        cmd_vals.extend([cur_cmd[i]])
            else:
                # Non-SMART → inherit directly
                oct_vals.extend(nxt_oct if nxt_oct else None)
                cmd_vals.extend(nxt_cmd if nxt_cmd else None)

        if oct_vals or cmd_vals:
            # Join as comma-separated string, drop empties
            df2.at[idx, "Isolation OCT Variables"] = ",".join([v for v in oct_vals if v]) if oct_vals else "-"
            df2.at[idx, "Isolation CMD Variables"] = ",".join([v for v in cmd_vals if v]) if cmd_vals else "-"

    ############################ Restoration ###########################

    for picture, picture_group in tqdm(df2.groupby("Picture"), total=df2['Picture'].nunique(), desc="Finishing up 2/2"):
        for feeder, feeder_group in picture_group.groupby("feeder_id"):
            for idx, row in feeder_group.iterrows():
                ID = row['ID']
                First_Machine = row['first machine in feeder']
                type = row['SMART']
                if "NOP" not in ID or feeder == "-" or First_Machine == ID or type != "SMART":
                    continue
                
                # Extract NOP Ys and Ts from ID
                NOP_Ys = [f"Y{y}" for y in re.findall(r'Y(\d+)', ID)]
                NOP_Ts = [f"Q{t}" for t in re.findall(r'T(\d+)', ID)]  # T1 becomes Q1, T2 becomes Q2, etc.

                # Combine for later matching
                NOP_codes = NOP_Ys + NOP_Ts

                con_machines = [x for x in row['Location Equipments IDs'].split(",")] if pd.notna(row['Location Equipments IDs']) else []
                nop_vars_cmd = []
                nop_vars_oct = row['NOP_Variables'].split(",") if pd.notna(row['NOP_Variables']) else []
                # Join and assign nop_vars_oct and replace OC_ST with OC_CMD
                if "_GND_ST" in row['NOP_Variables']:
                    nop_vars_oct = [var.replace("_GND_ST", "_OC_ST") for var in nop_vars_oct]
                nop_vars_cmd = [var.replace("_OC_ST", "_OC_CMD") for var in nop_vars_oct]

                # Remove duplicates
                nop_vars_oct = list(set(nop_vars_oct))
                nop_vars_cmd = list(set(nop_vars_cmd))

                if len(nop_vars_oct) > 1:
                    # Filter NOP variables based on NOP_codes (Y... and Q... codes)
                    nop_vars_oct = [var for var in nop_vars_oct if any(code in var for code in NOP_codes)]
                    nop_vars_cmd = [var for var in nop_vars_cmd if any(code in var for code in NOP_codes)]

                # Remove variables that do not have any of the NOP_Ys
                oct_val = ",".join(nop_vars_oct)
                cmd_val = ",".join(nop_vars_cmd)

                # Get feeder name of connected machines
                connected_feeders = []
                connected_feeders.append(feeder)
                for con_machine in con_machines:
                    if con_machine in df2['ID'].values:
                        feeder_id = df2.loc[(df2['Picture'] == picture) & (df2['ID'] == con_machine), 'feeder_id'].values[0]
                        if feeder_id not in connected_feeders and feeder_id != "-":
                            # Get current restoration values for this feeder
                            feeder_mask = (df2['Picture'] == picture) & (df2['feeder_id'] == feeder_id)
                            current_oct = df2.loc[feeder_mask, 'Restoration OCT Variables'].iloc[0]
                            current_cmd = df2.loc[feeder_mask, 'Restoration CMD Variables'].iloc[0]

                            # Update restoration variables
                            if current_oct != "-":
                                df2.loc[feeder_mask, 'Restoration OCT Variables'] += ("," + oct_val)
                                df2.loc[feeder_mask, 'Restoration CMD Variables'] += ("," + cmd_val)
                            else:
                                df2.loc[feeder_mask, 'Restoration OCT Variables'] = oct_val
                                df2.loc[feeder_mask, 'Restoration CMD Variables'] = cmd_val

                # # Add the feeder variable if it's not already in the list and not '-'
                # if row['Feeder OCT Variable'] != '-':
                #     oct_val = row['Feeder OCT Variable'] if oct_val == "" or First_Machine == ID else oct_val + "," + row['Feeder OCT Variable']
                # if row['Feeder CMD Variable'] != '-':
                #     cmd_val = row['Feeder CMD Variable'] if cmd_val == "" or First_Machine == ID else cmd_val + "," + row['Feeder CMD Variable']


                feeder_mask = (df2['Picture'] == picture) & (df2['feeder_id'] == feeder)
                current_oct = df2.loc[feeder_mask, 'Restoration OCT Variables'].iloc[0]
                if current_oct != "-":
                    df2.loc[feeder_mask, 'Restoration OCT Variables'] += "," + oct_val
                    df2.loc[feeder_mask, 'Restoration CMD Variables'] += "," + cmd_val
                else:
                    df2.loc[feeder_mask, 'Restoration OCT Variables'] = oct_val
                    df2.loc[feeder_mask, 'Restoration CMD Variables'] = cmd_val

                
                # if picture == "QASSIM-BUKERIYAH-13.8KV_LINKED":
                #     print(f"Updating feeder: {feeder}")
                #     print(f"Current OCT: {current_oct}")
                #     print(f"Adding OCT: {oct_val}")

    # Handle some specific cases for Restoration OCT and CMD Variables
    for idx, row in df2.iterrows():
        ID = row['ID']
        if "NOP" not in ID:
            continue
        con_machines = [x for x in row['Location Equipments IDs'].split(",")] if pd.notna(row['Location Equipments IDs']) else []
        picture = str(row['Picture'])
        feeder = row['FeederNo']

        # Get feeder name of connected machines
        for con_machine in con_machines:
            if con_machine == "-":
                continue
            feeder_con_mask = (df2['Picture'] == picture) & (df2['ID'] == con_machine)
            feeder_con_results = df2.loc[feeder_con_mask, 'FeederNo']
            
            if feeder_con_results.empty:
                continue  # Skip if no matching machine found
                
            feeder_con = feeder_con_results.values[0]
            if feeder_con != feeder and feeder_con != "-":
                # Get current restoration values for this feeder
                con_id_mask = (df2['Picture'] == picture) & (df2['ID'] == con_machine)
                current_oct = df2.loc[con_id_mask, 'Restoration OCT Variables'].iloc[0]
                current_cmd = df2.loc[con_id_mask, 'Restoration CMD Variables'].iloc[0]
                current_oct_nop = row['Restoration OCT Variables']

                # Update restoration variables
                if current_oct != "-":
                    # Remove some variable from current_oct
                    current_oct = ",".join([var for var in current_oct.split(",") if "EOA_ICCP#" not in var])
                    current_cmd = ",".join([var for var in current_cmd.split(",") if "EOA_ICCP#" not in var])
                    if current_oct_nop != "-":
                        df2.at[idx, 'Restoration OCT Variables'] += ("," + current_oct)
                        df2.at[idx, 'Restoration CMD Variables'] += ("," + current_cmd)
                    else:
                        df2.at[idx, 'Restoration OCT Variables'] = current_oct
                        df2.at[idx, 'Restoration CMD Variables'] = current_cmd
        
    for idx, row in df2.iterrows():
        ID = row['ID']
        # Remove duplicate variables in Restoration OCT and CMD Variables
        df2.at[idx, 'Restoration OCT Variables'] = ",".join(sorted(set(df2.at[idx, 'Restoration OCT Variables'].split(","))))
        df2.at[idx, 'Restoration CMD Variables'] = ",".join(sorted(set(df2.at[idx, 'Restoration CMD Variables'].split(","))))
        
        if "NOP" not in ID:
            continue
        current_oct_nop = row['Restoration OCT Variables']
        current_cmd_nop = row['Restoration CMD Variables']
        machine_var = row['Machine']
        sub_str = ""
        if not pd.isna(machine_var) and machine_var != "-":
            sub_str = machine_var.replace("_EF_ST", "")
        else:
            sub_str = row['VisualName']

        # for the nop machine itself remove its own variables that contain visual_name
        if current_oct_nop != "-":
            oct_val_nop = ",".join([var for var in current_oct_nop.split(",") if sub_str not in var and var.strip() != ""])
            cmd_val_nop = ",".join([var for var in current_cmd_nop.split(",") if sub_str not in var and var.strip() != ""])
            
            # Clean up empty values
            oct_val_nop = oct_val_nop if oct_val_nop and oct_val_nop != "" else "-"
            cmd_val_nop = cmd_val_nop if cmd_val_nop and cmd_val_nop != "" else "-"
            
            df2.at[idx, 'Restoration OCT Variables'] = oct_val_nop
            df2.at[idx, 'Restoration CMD Variables'] = cmd_val_nop


    ########################################

    df2['Screen Function Name'] = df2['Picture']
    df2['Functional Location'] = df2['VisualName']
    # df2['Administration'] = Administration
    df2['Project Name'] = project_name.replace("#", "")
    df2['Office'] = office_name

    # Rename columns to match your output
    df2 = df2.rename(columns={
        "Office": "Office Name",
        "Station": "Station Name",
        "FeederNo": "Feeder Name",
        "VisualName": "Equipment Number",
        "Machine": "Equipment EF Variable",
        "Con1": "Before Equipment EF Variable",
        "SMART": "Equipment Type",
        "Picture": "Screen Name",
        "ID": "Equipment ID"
    })

    ########################################

    # Drop helper column
    df2.drop(columns=['Con2', 'Con3', 'Con4', 'Con5', 'Con6', 'Con7', 'Con8', 'Con9', 'Con10', 'Con11', 'Con12', 'Con13', 'Con14', 'ISO1', 'ISO2', 'ISO3', 'ISO4', 'ISO5', 'ISO6', 'ISO7', 'ISO8', 'ISO9', 'ISO10', 'ISO11', 'ISO12', 'ISO13', 'ISO14', 'NOP_Variables',
                    'NOP', 'feeder_id', 'Isolation Equipments Numbers', 'Location Equipments IDs', 'first machine in feeder', 'last machines in feeder'], inplace=True)

    # Drop helper column
    # df2.drop(columns=['FeederNo_clean', 'VisualName_clean', 'best_match', 'Con2', 'Con3', 'Con4', 'Con5',  
    #                   'Con6', 'Con7', 'ISO1', 'ISO2', 'ISO3', 'ISO4', 'ISO5', 'ISO6', 'ISO7'], inplace=True)

    # df2.drop(columns=['NOP_Variables', 'feeder_id'], inplace=True)
    # df2.drop(columns=['Before Equipments Variables', 'After Equipments Variables'], inplace=True)

    # Replace empty cells with a dash
    df2.fillna("-", inplace=True)
    df2.replace("", "-", inplace=True)


    def arrange_columns(df, custom_order=None):
        """
        Arrange DataFrame columns in a specific order.
        
        Args:
            df: DataFrame to reorder
            custom_order: List of column names in desired order. If None, uses default order.
        
        Returns:
            DataFrame with reordered columns
        """
        if custom_order is None:
            # Default column order - modify this list to change the arrangement
            custom_order = [
                "Office Name",
                "Station Name",
                "Feeder Name",
                "Feeder OCT Variable",
                "Feeder CMD Variable",
                "Equipment Index",
                "Equipment ID",
                "Equipment Number",
                "Functional Location",
                "Equipment EF Variable",
                "Before Equipment Number",
                "After Equipment Number",
                "Before Equipment EF Variable",
                "After Equipments EF Variables",
                "Equipment Type",
                "Project Name",
                "Screen Name",
                "Screen Function Name",
                "Isolation OCT Variables",
                "Isolation CMD Variables",
                "Isolation Equipments Numbers",
                "Restoration OCT Variables",
                "Restoration CMD Variables",
                "Restoration Equipments Numbers",
                "ID", 
                "NOP"
            ]
        
        # Get existing columns and add any missing ones to the end
        existing_columns = df.columns.tolist()
        missing_columns = [col for col in existing_columns if col not in custom_order]
        final_column_order = [col for col in custom_order if col in existing_columns] + missing_columns
        
        return df[final_column_order]


    # Arrange columns using the helper function
    df2 = arrange_columns(df2)

    from remove_cell_duplicates import remove_duplicates_from_cell

    # Remove duplicates from a specific column (e.g., Restoration OCT Variables)
    col = "Restoration OCT Variables"
    df2[col], dups_removed = zip(*df2[col].apply(lambda x: remove_duplicates_from_cell(x, separator=",")))
    print(f"Column '{col}': {sum(dups_removed)} duplicates removed.")
    # Remove duplicates from a specific column (e.g., Restoration CMD Variables)
    col = "Restoration CMD Variables"
    df2[col], dups_removed = zip(*df2[col].apply(lambda x: remove_duplicates_from_cell(x, separator=",")))
    print(f"Column '{col}': {sum(dups_removed)} duplicates removed.")
    # Remove duplicates from a specific column (e.g., After Equipments EF Variables)
    col = "After Equipments EF Variables"
    df2[col], dups_removed = zip(*df2[col].apply(lambda x: remove_duplicates_from_cell(x, separator=",")))
    print(f"Column '{col}': {sum(dups_removed)} duplicates removed.")


    # Remove duplicates based on Picture and VisualName
    df2.drop_duplicates(subset=["Screen Name", "Equipment Number"], inplace=True)


    # Save the result
    df2.to_excel(output_file, index=False, sheet_name="FLISR_Data")

    print(f"Merged file saved as {output_file}")

