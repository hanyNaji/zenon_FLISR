import pandas as pd
from thefuzz import process
from tqdm import tqdm  # Import tqdm for progress visualization
import re



def run(output_file, output_folder, project_name, Administration, office_name, use_scr_xml):
    if use_scr_xml:
        var_df = pd.read_excel(r"{}\scr_machine_var.xlsx".format(output_folder))
    else:
        var_df = pd.read_excel(r"{}\alc_machine_var.xlsx".format(output_folder))
    # Load your files
    file2 = r"{}\alc_DB_FLIS_with_feeder.xlsx".format(output_folder)

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
        picture = row['Picture']
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
                if pd.notna(var_val) and str(var_val).strip() != "-":
                    if Administration in ["DWD", "RUH", "QAS", "KHA"]:
                        df2.at[idx, 'Feeder OCT Variable'] = f"{var_val}_CB_IND" if "_CB_" not in var_val else var_val
                        df2.at[idx, 'Feeder CMD Variable'] = f"{var_val}_CB_CMD" if "_CB_" not in var_val else var_val
                        if "#" not in var_val:
                            df2.at[idx, 'Feeder OCT Variable'] = project_name + df2.at[idx, 'Feeder OCT Variable']
                            df2.at[idx, 'Feeder CMD Variable'] = project_name + df2.at[idx, 'Feeder CMD Variable']
                    else:
                        df2.at[idx, 'Feeder OCT Variable'] = f"{var_val}_OC_ST"
                        df2.at[idx, 'Feeder CMD Variable'] = f"{var_val}_OC_CMD"
                else:
                    df2.at[idx, 'Feeder OCT Variable'] = "NO VARIABLE"
                    df2.at[idx, 'Feeder CMD Variable'] = "NO VARIABLE"

                # if Administration in ["DWD", "RUH"]:
                #     sta_val = var_val.split("#")[1] if "#" in var_val else var_val
                #     sta_val = sta_val.split("_CB_")[0] if "_CB_" in sta_val else sta_val
                #     fno_val = sta_val
                #     df2.at[idx, 'Station'] = sta_val 
                #     df2.at[idx, 'FeederNo'] = fno_val
                # else:
                df2.at[idx, 'Station'] = sta_val if pd.notna(sta_val) and str(sta_val).strip() != "-" else "-"
                df2.at[idx, 'FeederNo'] = fno_val if pd.notna(fno_val) and str(fno_val).strip() != "-" else "-"
            # else:
            #     # No match found for this feeder ID in var_df
            #     df2.at[idx, 'Feeder OCT Variable'] = "<only ID found>"
            #     df2.at[idx, 'Feeder CMD Variable'] = "<only ID found>"
            #     df2.at[idx, 'Station'] = "<only ID found>"
            #     df2.at[idx, 'FeederNo'] = "<only ID found>"

        # elif pd.notna(feederNO) and feederNO != "-":
        #     matches_2 = var_df[(var_df['ScreenName'] == picture) & (var_df['FeederNo'] == feederNO)]
        #     # If picture matches ScreenName and feederNo matches FeederNo, assign the values  
        #     if not matches_2.empty:
        #         df2.at[idx, 'Feeder OCT Variable'] = matches_2['Variable'].values[0] + "_OC_ST" if matches_2['Variable'].values[0] != "-" else "-"
        #         df2.at[idx, 'Feeder CMD Variable'] = matches_2['Variable'].values[0] + "_OC_CMD" if matches_2['Variable'].values[0] != "-" else "-"
        #         df2.at[idx, 'Station'] = matches_2['Station'].values[0]
        #         df2.at[idx, 'FeederNo'] = matches_2['FeederNo'].values[0] 
    # # Additional matching: If picture and feederNo from df2 matches ScreenName and FeederNo from var_df
    # for idx, row in df2.iterrows():
    #     picture = row['Picture']
    #     feederNO = row['FeederNo']
    # 
    #     # Skip if essential data is missing
    #     if pd.isna(picture) or picture == "-" or pd.isna(feederNO) or feederNO == "-":
    #         continue
    #
    #     # Find exact matches where both picture (ScreenName) and FeederNo match
    #     exact_matches = var_df[(var_df['ScreenName'] == picture) & (var_df['FeederNo'] == feederNO)]
    #
    #     if not exact_matches.empty:
    #         # Assign all the values from the matching row
    #         match_row = exact_matches.iloc[0]  # Take first match if multiple
    #         df2.at[idx, 'Station'] = match_row['Station'] if pd.notna(match_row['Station']) else row['Station']
    #         df2.at[idx, 'FeederNo'] = match_row['FeederNo'] if pd.notna(match_row['FeederNo']) else row['FeederNo']
    #
    #         # Assign feeder variables
    #         substitute_dest = match_row['Variable'] if pd.notna(match_row['Variable']) else "-"
    #         if substitute_dest != "-":
    #             df2.at[idx, 'Feeder OCT Variable'] = substitute_dest + "_OC_ST"
    #             df2.at[idx, 'Feeder CMD Variable'] = substitute_dest + "_OC_CMD"

    # Replace empty cells with a dash
    df2.fillna("-", inplace=True)
    df2.replace("", "-", inplace=True)

    df2.drop_duplicates(subset=["Picture", "ID"], inplace=True)

    ###################### Isolation, Location #####################

    smart_rec_sec = set([
        "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
        "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
        "INTEGRATION_PROJECT_SMT_SECTIONALIZER",
        "INTEGRATION_PROJECT_SMT_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_SMART_SECTIONALIZER"
    ])

    for idx, row in tqdm(df2.iterrows(), total=df2.shape[0], desc="Finishing up 1/2"):
        after_eq = ""
        for i in range(2, 8):
            con = row[f"Con{i}"]
            if con == "-":
                break
            if Administration in ["DWD", "RUH", "QAS", "KHA"] and "ICCP" in con:
                after_eq = (after_eq + con.replace("_EF_ST", "")+ ",")
            else:
                after_eq = (after_eq + project_name + con + ",") if "." in con else (after_eq + con + ",")
            df2.at[idx, "After Equipments EF Variables"] = after_eq

        df2.at[idx, "Machine"] = (project_name + row["Machine"]) if "." in row["Machine"] else row["Machine"]
        if Administration in ["DWD", "RUH", "QAS", "KHA"] and "ICCP" in row["Con1"]:
            df2.at[idx, "Con1"] = (row["Con1"].replace("_EF_ST", ""))
        else:
            df2.at[idx, "Con1"] = (project_name + row["Con1"]) if "." in row["Con1"] else row["Con1"]

        iso_oct = ""
        iso_cmd = ""
        for i in range(1, 8):
            iso = row[f"ISO{i}"]
            if iso == "-":
                break
            if "#" in iso:
                if "_DCC#" in iso and "_OPN_CMD" in iso:
                    iso_oct = iso_oct + iso.replace("_OPN_CMD", "_OC_ST") + ","
                    iso_cmd = iso_cmd + iso + ","
                elif Administration in ["DWD", "RUH", "QAS", "KHA"]:
                    if "ICCP" in iso:
                        iso_oct = iso_oct + iso + "_CB_IND" + ","
                        iso_cmd = iso_cmd + iso + "_CB_CMD" + ","
                    elif "CB_ST" in iso:
                        iso_oct = iso_oct + iso + ","
                        iso_cmd = iso_cmd + iso.replace("_ST", "_CMD") + ","
                    else:
                        iso_oct = iso_oct + iso + "_OC_ST" + ","
                        iso_cmd = iso_cmd + iso + "_OC_CMD" + ","
                else:
                    iso_oct = iso_oct + iso + "_OC_ST" + ","
                    iso_cmd = iso_cmd + iso + "_OC_CMD" + ","
            else:
                if "ICCP#" in iso:
                    iso_oct = iso_oct + iso + "_CB_IND" + ","
                    iso_cmd = iso_cmd + iso + "_CB_CMD" + ","
                elif "CB_ST" in iso:
                    iso_oct = iso_oct + iso + ","
                    iso_cmd = iso_cmd + iso.replace("_ST", "_CMD") + ","
                else:
                    iso_oct = iso_oct + iso + ","
                    iso_cmd = iso_cmd + iso + ","

            df2.at[idx, "Isolation OCT Variables"] = iso_oct
            df2.at[idx, "Isolation CMD Variables"] = iso_cmd
        # Initialize Restoration OCT and CMD Variables
        df2.at[idx, "Restoration OCT Variables"] = row['Feeder OCT Variable']
        df2.at[idx, "Restoration CMD Variables"] = row['Feeder CMD Variable']


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

                con_machines = [x for x in row['Location Equipments IDs'].split(",") if x.strip()] if pd.notna(row['Location Equipments IDs']) else []
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
                    # con_machine = con_machine.strip()
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
        con_machines = row['NOP'].split(",")
        picture = row['Picture']
        feeder = row['FeederNo']

        # Get feeder name of connected machines
        for con_machine in con_machines:
            if con_machine == "NOP":
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
                    'NOP', 'feeder_id', 'Location Equipments IDs', 'first machine in feeder', 'last machines in feeder'], inplace=True)

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

