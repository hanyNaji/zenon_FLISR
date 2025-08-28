import os
import ctypes
import subprocess
from pathlib import Path

# Main project runner for FLISR
# Define all input/output paths here
use_scr_xml = False

###############------ CHANGE THIS ------###############
alc_xml_file = r"D:\chnge order\FLISR\SERVICE ENGINE\HASA_REGIN_ZENON\ABQIQ_PROJECT_SLD\RT\FILES\zenon\system\alc.XML"
# scr_xml_file = r"D:\chnge order\FLISR\JUBAIL\SCREENS\HFR.XML"
output_folder = r"D:\chnge order\FLISR"

# ADMINISTRATION = "NED"
# OFFICE_NAME = "HFR"
# OFFICE_NO = ""
###############------ CHANGE THIS ------###############

# --- Paths ---
alc_path = Path(alc_xml_file)
# scr_path = Path(scr_xml_file)
project_name_only = alc_path.parents[4].name
workspace_name = alc_path.parents[5].name

out_dir = Path(output_folder) / "outputs" / workspace_name / project_name_only  # outputs/Workspace/Project

# Make sure output folder exists
out_dir.mkdir(parents=True, exist_ok=True)


PROJECT_NAME = project_name_only + "#"

output_file_base = Path(output_folder) / workspace_name
output_file_base.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = output_file_base / f"{workspace_name.split("_")[0]}_{project_name_only}_DB_FLISR.xlsx"


def run_all():
    print("Running all FLISR scripts...")
    print("---------------------------------------------------------")
    print(f"  Project:   {project_name_only}")
    print(f"  Workspace: {workspace_name}")
    print("---------------------------------------------------------")
    print(f"  ALC XML:        {alc_path}")
    # print(f"  SCR XML:        {scr_path} (use_scr_xml={use_scr_xml})") 
    print(f"  Output folder:  {out_dir}")
    print(f"  Final Output :  {OUTPUT_FILE}")
    print("---------------------------------------------------------")
    print()

    # Imports placed here so the script can print path info even if a module is missing
    import pandas as pd  # noqa: F401
    from Extract_data_ALC import run as Extract_data_ALC
    from Extract_data_SCREENS import run as Extract_data_SCREENS
    from Alc_Machines_loc_Iso import run as Alc_Machines_loc_Iso
    from Assign_feeder_to_machines_V5 import run as assign_feeder_to_machines
    from Machine_data_flisr import run as machine_data_flisr

    Extract_data_ALC(str(alc_path), str(out_dir))
    # Extract_data_SCREENS(str(scr_path), str(out_dir), use_scr_xml)
    Alc_Machines_loc_Iso(str(alc_path), str(out_dir), use_scr_xml)
    assign_feeder_to_machines(str(alc_path), str(out_dir), use_scr_xml)
    machine_data_flisr(str(OUTPUT_FILE), str(out_dir), PROJECT_NAME, project_name_only, use_scr_xml)

if __name__ == "__main__":
    run_all()

    # --- Message box & sound & top-most; ask to open the output ---
    msg = (
        f"FLISR processing complete...\n"
        f"Project: {project_name_only}\n"
        f"Workspace: {workspace_name}\n\n"
        f"Open the final output now?"
    )

    MB_ICONINFORMATION = 0x00000040
    MB_YESNO           = 0x00000004
    MB_SETFOREGROUND   = 0x00010000
    MB_TOPMOST         = 0x00040000
    flags = MB_ICONINFORMATION | MB_YESNO | MB_SETFOREGROUND | MB_TOPMOST

    # Show the dialog (Windows will play the info sound automatically)
    ret = ctypes.windll.user32.MessageBoxW(0, msg, "FLISR", flags)

    IDYES = 6
    if ret == IDYES:
        try:
            if OUTPUT_FILE.exists():
                os.startfile(str(OUTPUT_FILE))  # open with default app
            else:
                # If final output doesn't exist (e.g., generated elsewhere), open the output folder
                os.startfile(str(out_dir))
        except Exception:
            # Attempt to select the expected output in Explorer (works if it exists)
            try:
                subprocess.run(["explorer", "/select,", str(OUTPUT_FILE)], check=False)
            except Exception:
                pass
