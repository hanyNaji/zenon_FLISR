import ctypes
# Main project runner for FLISR
# Define all input/output paths here
use_scr_xml = True

###############------ CHANGE THIS ------###############
alc_xml_file = r"D:\chnge order\FLISR\SERVICE ENGINE\QASSIM\WEST_QASSIM_OFFICE\RT\FILES\zenon\system\alc.XML"
scr_xml_file = r"D:\chnge order\FLISR\QASSIM\SCREENS\WST.XML"
output_folder = r"D:\Zenon py\Line follower\FLISR\outputs\3"

# OUTPUT_FILE = r"D:\chnge order\FLISR\KHARJ\KHA_SUL_DB_FLISR.xlsx"
ADMINISTRATION = "QAS"
OFFICE_NAME = "WST"
OFFICE_NO = ""
###############------ CHANGE THIS ------###############

PROJECT_NAME = alc_xml_file.split("\\")[-6] + "#"
# use scr_xml_file, ADMINISTRATION, OFFICE_NAME to define OUTPUT_FILE
OUTPUT_FILE = r"{}\{}_{}{}_DB_FLISR.xlsx".format(scr_xml_file.split("\\")[0] + 
                                               "\\" + scr_xml_file.split("\\")[1] + 
                                               "\\" + scr_xml_file.split("\\")[2] + 
                                               "\\" + scr_xml_file.split("\\")[3], 
                                               ADMINISTRATION, OFFICE_NAME, OFFICE_NO)


def run_all():
    print("Running all FLISR scripts...")
    import pandas as pd
    from Extract_data_ALC import run as Extract_data_ALC
    from Extract_data_SCREENS import run as Extract_data_SCREENS
    from Alc_Machines_loc_Iso import run as Alc_Machines_loc_Iso
    from Assign_feeder_to_machines_V5 import run as assign_feeder_to_machines
    from Machine_data_flisr import run as machine_data_flisr

    Extract_data_ALC(alc_xml_file, output_folder)
    Extract_data_SCREENS(scr_xml_file, output_folder, use_scr_xml)
    Alc_Machines_loc_Iso(alc_xml_file, output_folder, use_scr_xml)
    assign_feeder_to_machines(alc_xml_file, output_folder, use_scr_xml)
    machine_data_flisr(OUTPUT_FILE, output_folder, PROJECT_NAME, ADMINISTRATION, OFFICE_NAME, use_scr_xml)

if __name__ == "__main__":
    run_all()


    # Show a message box & sound alert 
    ctypes.windll.user32.MessageBoxW(0, f"FLISR processing complete...\nProject: {PROJECT_NAME}\nWorkspace: {scr_xml_file.split("\\")[3]}", "Notification", 0x40 | 0x1)
