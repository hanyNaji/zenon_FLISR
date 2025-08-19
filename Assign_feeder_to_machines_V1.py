import xml.etree.ElementTree as ET
from collections import defaultdict, deque
import pandas as pd
import re

EMERGENCY_FILTER = True



###############------ CHANGE THIS ------###############
xml_file = r"D:\chnge order\FLISR\SERVICE ENGINE\ARAR\SLD_TURIF_OFFICE\RT\FILES\zenon\system\alc.XML"
output_folder = r"D:\Zenon py\Line follower\FLISR\outputs"
###############------ CHANGE THIS ------###############

machines = pd.read_excel(r"{}\alc_DB_FLIS.xlsx".format(output_folder))
subdest_df = pd.read_excel(r"{}\scr_machine_var.xlsx".format(output_folder))

output_file_1 = r"{}\feeder_nop_paths_summary.xlsx".format(output_folder)
output_file_2 = r"{}\alc_DB_FLIS_with_feeder.xlsx".format(output_folder)

# Add progress settings
PROGRESS_EVERY = 50000          # BFS dequeue progress step
ASSIGN_PROGRESS_EVERY = 200     # Assignment progress step

print("="*40)
print("  Network Traversal Debug Script")
print("="*40)


id_to_FeederNo = {}

for _, row in subdest_df.iterrows():
    if "FDR" not in row["ID"]:
        continue
    key = (row["ScreenName"], str(row["ID"]))
    id_to_FeederNo[key] = row["FeederNo"]

def map_id_to_FeederNo(picture, id_val):
    return id_to_FeederNo.get((picture, str(id_val)), "-")

print(f"Loaded {len(id_to_FeederNo)} feeder mappings from subdest_df.")

# Load XML
tree = ET.parse(xml_file)
root = tree.getroot()
graph_elements = root.find('GraphElements')
elements = graph_elements.findall('GraphElement')

# Build element lookup and type map
all_elements = {}
element_types = defaultdict(int)
elementid_to_graph_element = {}  # Map element ID to XML element
for elem in elements:
    elem_id = elem.find('ID').text
    elem_type = int(elem.find('Type').text)
    all_elements[elem_id] = elem
    elementid_to_graph_element[elem_id] = elem  # Add mapping
    element_types[elem_type] += 1

print("Element type distribution:")
for t, c in sorted(element_types.items()):
    print(f"  Type {t}: {c}")

# Build connection graph (element <-> node bipartite)
connections = defaultdict(set)
for elem in elements:
    elem_id = elem.find('ID').text
    node1 = elem.find('Node1IDs')
    node2 = elem.find('Node2IDs')
    if node1 is not None:
        node1_ids = [n.text for n in node1.findall('ID')]
        for n1 in node1_ids:
            connections[elem_id].add(n1) if n1 not in connections[elem_id] else None
            connections[n1].add(elem_id) if elem_id not in connections[n1] else None
    if node2 is not None:
        node2_ids = [n.text for n in node2.findall('ID')]
        for n2 in node2_ids:
            connections[elem_id].add(n2) if n2 not in connections[elem_id] else None
            connections[n2].add(elem_id) if elem_id not in connections[n2] else None


# Group elements by Picture
picture_groups = defaultdict(list)
for elem in elements:
    picture = elem.find('Picture').text if elem.find('Picture') is not None else 'NO_PICTURE'
    if EMERGENCY_FILTER and 'EMERGENCY' in str(picture).upper() or 'EMRGENCY' in str(picture).upper() or '_ALL' in str(picture).upper():
        continue
    picture_groups[picture].append(elem)

print(f"\nFound {len(picture_groups)} unique pictures.")

# For each picture, find unique feeders (FDR in ElementRef, use second part as key)
picture_feeders = {}
for picture, elems in picture_groups.items():
    feeders = {}
    for elem in elems:
        elem_id = elem.find('ID').text
        elem_type = int(elem.find('Type').text)
        element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else ""
        if "INTEGRATION_PROJECT_SLD_FDR_DSS_1_DOWN_ALIAS" in element_ref:
            parts = element_ref.split('.')
            feeder_key = parts[1] if len(parts) > 1 else elem_id
            feeders[feeder_key] = elem_id
    picture_feeders[picture] = feeders
    print(f"Picture: {picture} | Unique feeders: {len(feeders)}")
    # for k, v in feeders.items():
        # print(f"  Feeder key: {k}, ID: {v}")

# Traverse from each unique feeder per picture, BFS


specialPrefixes = set([
    "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMT_SLD_LBS",
    "INTEGRATION_PROJECT_SMT_SECTIONALIZER",
    "INTEGRATION_PROJECT_SMT_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_SMT_SLD_LBS",

    "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
    "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_SMART_SLD_LBS"
])


# List of prefixes to ignore for machine names
ignore_prefixes = [
    "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMT_SLD_LBS",
    # "INTEGRATION_PROJECT_SMT_SECTIONALIZER",
    # "INTEGRATION_PROJECT_SMT_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_SMT_SLD_LBS",

    "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
    # "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
    # "INTEGRATION_PROJECT_SMART_SECTIONALIZER",
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

import re


### ADD: variable check for the connection between nop machine and machine before it

# Helper to get adjacent elements through shared nodes using 'connections'
def get_element_neighbors(elem_id: str, current_path=None):
    """
    Return only elements that are directly connected to elem_id via a shared node,
    and not already in the current path (if provided).
    This avoids cycles and indirect connections.
    """
    neigh_elements = set()
    for nid in connections.get(elem_id, ()):  # nodes connected to this element
        for e2 in connections.get(nid, ()):   # elements connected to that node
            if e2 != elem_id and e2 in all_elements:
                if current_path is None or e2 not in current_path:
                    neigh_elements.add(e2)
    return neigh_elements



def get_machine_name(elem_id):
    elem = all_elements[elem_id] if elem_id in all_elements else None
    if elem is None:
        return elem_id  # Return ID if element not found
    element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else elem_id
    # Use second part after splitting by dot, if available
    parts = element_ref.split('.')
    name = parts[1] if len(parts) > 1 else element_ref
    return name

def get_machine_name_filtered(elem_id):
    name = get_machine_name(elem_id)
    # Only filter out ignored prefixes at the end, but allow NOPs
    if '_NOP' in name.upper():
        return name
    for prefix in ignore_prefixes:
        if name.startswith(prefix):
            return None
    return name

def is_valid_machine(elem_id):
    name = get_machine_name(elem_id)
    # Always allow NOPs as valid endpoints
    if '_NOP' in name.upper():
        return name
    for prefix in ignore_prefixes:
        if name.startswith(prefix):
            return False
    return name

def get_machine_legs(machine_name):
    """Extract the number of legs for a machine based on its num before L in its name (2L, 3L, etc.) at the start."""
    if not machine_name:
        return []
    # Look for L pattern in the machine name and get number before L
    # e.g. L2, L3, etc.
    if machine_name.startswith('2L'):
        return 2
    elif machine_name.startswith('3L'):
        return 3
    elif machine_name.startswith('4L'):
        return 4
    elif machine_name.startswith('5L'):
        return 5
    return 2

def is_multi_leg_machine(elem_id):
    """Check if a machine has more than 2 legs"""
    machine_name = get_machine_name(elem_id)
    legs = get_machine_legs(machine_name)
    return legs > 2

            
def add_path_to_summary(picture, feeder_key, path, nop_machine, end_reason, feeders, legs_traversed = 10):
    """Helper function to add a path to summary with proper processing"""
    path_names = []
    seen_names = set()
    path_all_machines = []  # ALL machines without any filtering
    last_conn = None  # Initialize last_conn properly
    Full_ID_Path_str = ' -> '.join(path)  # Use original path for Full_ID_Path

    legs_traversed = int(path[-1]) if int(path[-1]) < 10 else legs_traversed
    path.remove(str(legs_traversed)) if str(legs_traversed) in path else None  # Remove legs traversed from path
    
    for i, pid in enumerate(path):
        name_unfiltered = get_machine_name(pid)
        name_filtered = get_machine_name_filtered(pid)
        print(f"  DEBUG: Processing machine '{name_filtered}'") if name_filtered == "2L1T_4_1_2_1_1_15_1" else None
        # Add ALL machines to the unfiltered list (no prefix filtering)
        if name_unfiltered:
            path_all_machines.append(name_unfiltered)
            if name_unfiltered.startswith('Line'):
                last_conn = i + 1
        else:
            path_all_machines.append(pid[:20])
        # Deduplication: always allow NOPs, filter others
        if name_filtered is not None:
            if name_filtered or ('_NOP' in name_filtered.upper()):
                if name_filtered not in seen_names:
                    print(f"  DEBUG: Adding machine to feeder '{name_filtered}'") if name_filtered == "2L1T_4_1_2_1_1_15_1" else None
                    path_names.append(name_filtered)
                    seen_names.add(name_filtered)
                    
        

    if not path_names:
        return False
    last_machine = path_names[-1] if len(path_names) > 1 else None
    if legs_traversed < 2:
        # print(f"  DEBUG: Removing last machine '{last_machine}' from path due to legs traversed {legs_traversed}")
        path_names = [n for n in path_names if n != last_machine]
    
    # Remove last machine if it appears less than 4 and more than 1 times in path_all_machines
    if last_machine:
        count_last = path_all_machines.count(last_machine)
        if count_last < 4 and count_last > 1:
            path_names = [n for n in path_names if n != last_machine]
            # print(f"  DEBUG: Removed last machine '{last_machine}' from path {path_str}") if legs_traversed < 10 else None

    # if last_conn and last_conn < len(path) and last_machine and "NOP" in last_machine:
    #     last_conn_variable = find_variable_stand_alone(path[last_conn])
    #     if last_conn_variable:
    #         m = re.search(r'NOP_((?:Y\d+_?)+)', last_machine)
    #         if m:
    #             codes_str = m.group(1)
    #             codes = re.findall(r'Y\d+', codes_str)
    #             # Check if there is a code is in variable
    #             valid_code = [code for code in codes if code in last_conn_variable]
    #             if valid_code:
    #                 print(f"  DEBUG: Found valid NOP legs {valid_code} in variable '{last_conn_variable}' for last machine '{last_machine}'")
    #                 path_names = [n for n in path_names if n != last_machine]

    # path_all_machines.append(str(legs_traversed))

    path_str = ' -> '.join(path_names)
    path_all_machines_str = ' -> '.join(path_all_machines)  # ALL machines, no filtering
    first_machine = path_names[1] if len(path_names) > 1 else '-'
    last_machine = path_names[-1] if len(path_names) > 1 else '-'
    machine_count = len(path_names)
    
    # Check for direct feeder-to-feeder connection in deduplicated path
    feeder_names_in_path = [n for n in path_names if n in feeders]
    if len(set(feeder_names_in_path)) > 1:
        return False
    
    summary_rows.append({
        'Picture': picture,
        'Feeder': feeder_key,
        'Path': path_str,
        'Full_Path': path_all_machines_str,  # ALL machines without filtering
        'Full_ID_Path': Full_ID_Path_str,  # Use the original path for Full_ID_Path
        'NOP_Machine': nop_machine,
        'End_Reason': end_reason,
        'First_Machine': first_machine,
        'Last_Machine': last_machine,
        'Machine_Count': machine_count - 1  # Exclude the feeder itself from count
    })
    return True

# Track special prefix NOP machines to ensure they're only added to the first feeder
special_nop_machines_assigned = set()
NOP_END_POINTS = set()  # Track NOP end points to avoid revisiting

summary_rows = []
last_machines_list = []
# Add picture-level progress
_pictures_items = list(picture_feeders.items())
_total_pictures = len(_pictures_items)
for _pic_idx, (picture, feeders) in enumerate(_pictures_items, 1):
    print(f"\n--- Picture {_pic_idx}/{_total_pictures}: {picture} ---")
    print(f"  Feeders detected: {list(feeders.keys())}")
    # Per-picture feeder progress
    _feeders_items = list(feeders.items())
    _total_feeders = len(_feeders_items)
    for _f_idx, (feeder_key, feeder_id) in enumerate(_feeders_items, 1):
        print(f"Feeder {_f_idx}/{_total_feeders}: {feeder_key} (ID: {feeder_id})")
        # Skip feeders whose key/name ends with _NOP
        if str(feeder_key).strip().upper().endswith('_NOP'):
            print(f"  Skipping feeder '{feeder_key}' because it ends with _NOP")
            continue
        visited = set()
        visited_count = defaultdict(int)  # Track visit count for each element
        queue = deque([(feeder_id, [feeder_id], None)])  # (current_id, path, leg_context)
        path_count = 0
        traversed_codes = {}
        dequeues = 0  # BFS progress counter
        while queue and path_count < 2000000:
            current, path, leg_context = queue.popleft()
            dequeues += 1
            if dequeues % PROGRESS_EVERY == 0:
                print(f"    progress: {dequeues:,} dequeued, paths={path_count}, queue={len(queue):,}")
            visit_key = (current, leg_context)
            current_machine_name = get_machine_name(current) or current
            print(f"    Visiting {current} ({current_machine_name}), visited count: {visited_count[visit_key]}") if current_machine_name == "2L1T_4_1_2_1_1_15_1" else None
            if visited_count[visit_key] >= 10 or current in NOP_END_POINTS:
                continue
            
            is_special_machine = any(current_machine_name.startswith(prefix) for prefix in specialPrefixes)
            if is_special_machine and visited_count[visit_key] >= 1:
                continue
            visited_count[visit_key] += 1
            if current != feeder_id:
                print(f"    Visiting {current} ({current_machine_name}), visited count: {visited_count[visit_key]}") if current_machine_name == "2L1T_4_1_2_1_1_15_1" else None
                elem = all_elements[current]
                element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else ""
                element_ref_id = element_ref.split('.')[1] if '.' in element_ref else element_ref
                variable = elem.find('Variable').text if elem.find('Variable') is not None else ""
                # Check if current element is a feeder (but not the starting feeder)
                if "INTEGRATION_PROJECT_SLD_FDR_DSS_1_DOWN_ALIAS" in element_ref and current != feeder_id:
                    continue
                # Handle NOP machines - enhanced for multi-leg NOPs
                if 'NOP' in element_ref:
                    current_machine_name = get_machine_name(current) or current
                    if (picture, current_machine_name) not in traversed_codes:
                        traversed_codes[(picture, current_machine_name)] = []
                    is_special_machine = any(current_machine_name.startswith(prefix) for prefix in specialPrefixes)
                    if is_special_machine:
                        NOP_END_POINTS.add(current)
                        for neighbor in connections.get(current, set()):
                            NOP_END_POINTS.add(neighbor)
                        if current_machine_name in special_nop_machines_assigned:
                            path = [p for p in path if p != current_machine_name]
                            if add_path_to_summary(picture, feeder_key, path, current_machine_name, 'SPECIAL_MACHINE_ENDPOINT', feeders):
                                path_count += 1
                                # append last valid machine name
                                # for last_machine in reversed(path):
                                #     name = get_machine_name_filtered(last_machine)
                                #     if name != None:
                                #         last_machines_list.append(name)
                                #         break
                                
                                name = get_machine_name_filtered(current)
                                if name != None:
                                    last_machines_list.append(name)
                                # traversed_codes = []
                        else:
                            if add_path_to_summary(picture, feeder_key, path, current_machine_name, 'SPECIAL_NOP_ENDPOINT', feeders):
                                path_count += 1
                                # append last valid machine name
                                # for last_machine in reversed(path):
                                #     name = get_machine_name_filtered(last_machine)
                                #     if name != None:
                                #         last_machines_list.append(name)
                                #         break
                                name = get_machine_name_filtered(current)
                                if name != None:
                                    last_machines_list.append(name)
                                # traversed_codes = []
                            special_nop_machines_assigned.add(current_machine_name)
                        continue
                    else:
                        # m = re.search(r'NOP_((?:[YT]\d+_?)+)', element_ref)
                        # if m:
                        #     codes_str = m.group(1)
                        #     codes = re.findall(r'[YT]\d+', codes_str)
                        #     valid_code = []
                        #     for code in codes:
                        #         if code.startswith('Y'):
                        #             # Y codes must appear as-is in variable
                        #             if code in variable:
                        #                 valid_code.append(code)
                        #         elif code.startswith('T'):
                        #             # T codes must appear as Q in variable (T1 -> Q1)
                        #             q_code = 'Q' + code[1:]
                        #             if q_code in variable:
                        #                 valid_code.append(code)

                        allYcodes = ["Y" + str(i) for i in range(1, 6)] # Y codes
                        allYcodes += ["Q" + str(i) for i in range(1, 3)] # Q codes
                        allYcodes += ["TR_RIGHT"] # TR_RIGHT codes
                        allYcodes += ["TR_LEFT"] # TR_LEFT codes
                        allYcodes += ["TR"] # TR codes
                        # Get NOP codes
                        nop_codes = []
                        for code in allYcodes:
                            if code in element_ref_id:
                                nop_codes.append(code)

                        if nop_codes:
                            valid_code = []
                            for code in nop_codes:
                                # codes must appear as-is in variable
                                if code in variable:
                                    valid_code.append(code)
                                    
                            # check if any of allYcodes are in variable
                            for code in allYcodes:
                                if code in variable:
                                    traversed_codes[(picture, current_machine_name)].extend(code)
                            # Remove duplicates
                            traversed_codes[(picture, current_machine_name)] = list(set(traversed_codes[(picture, current_machine_name)]))
                            if valid_code:
                                nop_machine = get_machine_name(current) or current
                                end_reason = "NOP_Y_MATCH"
                                path.append(str(len(traversed_codes)))
                                if add_path_to_summary(picture, feeder_key, path, nop_machine, end_reason, feeders):
                                    path_count += 1
                                    name = get_machine_name_filtered(current)
                                    if name != None:
                                        last_machines_list.append(name)
                                    # traversed_codes = []
                                NOP_END_POINTS.add(current)
                                if is_multi_leg_machine(current):
                                    for neighbor in connections.get(current, set()):
                                        NOP_END_POINTS.add(neighbor)
                                    pass
                                else:
                                    for neighbor in connections.get(current, set()):
                                        if get_machine_name(neighbor) == current_machine_name:
                                            NOP_END_POINTS.add(neighbor)
                                            # if current_machine_name == "2L1T_4_1_2_1_1_15_NOP_Y2":
                                            #     print("Adding neighbor:", neighbor)
                                            #     print("For machine name:", get_machine_name(neighbor))
                                            for neighbor2 in connections.get(neighbor, set()):
                                                if get_machine_name(neighbor2) == current_machine_name:
                                                    NOP_END_POINTS.add(neighbor2)
                                                    for neighbor3 in connections.get(neighbor2, set()):
                                                        if get_machine_name(neighbor3) == current_machine_name:
                                                            NOP_END_POINTS.add(neighbor3)
                                    continue
                        else:
                            nop_machine = get_machine_name(current) or current
            # Traverse neighbors for all machines - improved approach
            all_neighbors = list(get_element_neighbors(current, path))
            # print(f"DEBUG: Current {current}, Path {path}, Neighbors {all_neighbors}")
            neighbors = [neighbor for neighbor in all_neighbors if visited_count[(neighbor, leg_context)] < 10]
            if not neighbors and current != feeder_id:
                current_machine_name = get_machine_name(current) or current
                end_reason = f"DEAD_END_AT_{current_machine_name[:30]}"
                if add_path_to_summary(picture, feeder_key, path, current_machine_name, end_reason, feeders):
                    path_count += 1
                    # append last valid machine name
                    # for last_machine in reversed(path):
                    #     name = get_machine_name_filtered(last_machine)
                    #     if name != None:
                    #         last_machines_list.append(name)
                    #         break
                    
                    name = get_machine_name_filtered(current)
                    if name != None:
                        last_machines_list.append(name)
                    # traversed_codes = []
                continue
            for neighbor in neighbors:
                queue.append((neighbor, path + [neighbor], leg_context))
        if path_count == 0:
            print("  No NOP end paths found from this feeder.")
        else:
            print(f"  Found {path_count} paths from this feeder.")

# Always define df, even if summary_rows is empty
if summary_rows:
    df = pd.DataFrame(summary_rows)
    df.drop_duplicates(subset=["Picture", "Feeder", "First_Machine", "Last_Machine", "Machine_Count"], inplace=True)
    # Remove paths with less than 1 machine names
    df = df[df['Path'].apply(lambda x: len([n for n in x.split('->') if n.strip()]) >= 1)]
    # Remove sub-paths
    to_remove = set()
    paths = df['Path'].tolist()
    for i, path_i in enumerate(paths):
        for j, path_j in enumerate(paths):
            if i != j and path_i and path_j and path_i in path_j:
                if len(path_i) < len(path_j):
                    to_remove.add(i)
    if to_remove:
        df = df.drop(df.index[list(to_remove)])
    df.to_excel(output_file_1, index=False)
    print(f"\nSummary written to {output_file_1} with {len(df)} rows.")
else:
    df = pd.DataFrame()
    print("\nNo valid NOP paths found to write to summary file.")

total_feeders = sum(len(f) for f in picture_feeders.values())
print(f"\nTotal unique feeders across all pictures: {total_feeders}")
print("Traversal complete.")

#############################################################################################


## Now assign feeder info to machines in the DB based on the summary

# Load summary and machine DB
summary = df

print("\n")
print("="*40)
print("Assign Feeder Info to Machines Script")
print("="*40)

# Prepare mapping: (Picture, machine ID) -> (feeder_id, first machine, last machine)
machine_to_feeder = {}
for _, row in summary.iterrows():
    feeder_id = row['Feeder']
    first_machine = row['First_Machine']
    last_machine = row['Last_Machine']
    picture = row['Picture']
    # Split path into machine IDs (use the same delimiter as in the summary)
    path_ids = [n.strip() for n in str(row['Path']).split('->') if n.strip()] if 'Path' in row else []
    for m_id in path_ids:
        machine_to_feeder[(picture, m_id)] = (feeder_id, first_machine, last_machine)

# Assign feeder info to each machine in DB using (Picture, machine) as key
feeder_ids = []
first_machines = []
last_machines = []

_total_rows = len(machines)
assigned_count = 0

# Prepare mapping: (Picture, machine ID) -> (feeder_id, first machine, last machines)
feeder_to_last_machines = defaultdict(set)
for _, row in summary.iterrows():
    feeder_id = row['Feeder']
    last_machine = row['Last_Machine']
    picture = row['Picture']
    if last_machine and last_machine != '-':
        feeder_to_last_machines[(picture, feeder_id)].add(last_machine)

machine_to_feeder = {}
for _, row in summary.iterrows():
    feeder_id = row['Feeder']
    first_machine = row['First_Machine']
    picture = row['Picture']
    # Split path into machine IDs (use the same delimiter as in the summary)
    path_ids = [n.strip() for n in str(row['Path']).split('->') if n.strip()] if 'Path' in row else []
    last_machines = ','.join(sorted(feeder_to_last_machines[(picture, feeder_id)]))
    for m_id in path_ids:
        machine_to_feeder[(picture, m_id)] = (feeder_id, first_machine, last_machines)

# Assign feeder info to each machine in DB using (Picture, machine) as key
feeder_ids = []
first_machines = []
last_machines = []

_total_rows = len(machines)
assigned_count = 0

for _idx, (_, row) in enumerate(machines.iterrows(), 1):
    picture = str(row['Picture'])
    machine_id = str(row['ID'])
    feeder_info = machine_to_feeder.get((picture, machine_id), ('-', '-', '-'))
    if feeder_info[0] != '-':
        assigned_count += 1
    if _idx % ASSIGN_PROGRESS_EVERY == 0:
        print(f"  assignment progress: {_idx}/{_total_rows} processed, assigned={assigned_count}")
    feeder_ids.append(feeder_info[0])
    first_machines.append(feeder_info[1])
    last_machines.append(feeder_info[2])  # Now this is a comma-separated list

machines['feeder_id'] = feeder_ids
machines['first machine in feeder'] = first_machines
machines['last machines in feeder'] = last_machines  # <-- updated column name

print(f"Assigned feeder info to {assigned_count}/{_total_rows} machines.")

last_machines_list = list(set(last_machines_list))  # Remove duplicates


# After feeder assignment, post-process Isolation Equipments Numbers
if 'Isolation Equipments Numbers' in machines.columns:
    iso_cols = [f'ISO{i}' for i in range(1, 21) if f'ISO{i}' in machines.columns]
    for idx, row in machines.iterrows():
        picture = str(row['Picture'])
        feeder_id = row.get('feeder_id', '-')
        iso_equip = str(row.get('Isolation Equipments Numbers', ''))
        if not iso_equip or feeder_id == '-' or iso_equip == '-':
            continue
        visual_names = [v.strip() for v in iso_equip.split(',') if v.strip()]
        iso_num = len(visual_names)
        # Get feeder assignments for all machines
        valid_visuals = []
        for vname in visual_names:
            # Find matching row for this VisualName
            match = machines[(machines['VisualName'] == vname) & (machines['feeder_id'] == feeder_id)]
            match_fdr = vname in id_to_FeederNo.get((picture, feeder_id), [])
            if not match.empty:
                valid_visuals.append(vname)
            if match_fdr:
                valid_visuals.append(vname) if vname not in valid_visuals else None
        # If any were removed, update Isolation Equipments Numbers and ISO columns
        if set(valid_visuals) != set(visual_names):
            machines.at[idx, 'Isolation Equipments Numbers'] = ','.join(valid_visuals)
            # Remove ISO variable if its VisualName was removed
            for i, vname in enumerate(visual_names):
                if vname not in valid_visuals:
                    machines.at[idx, iso_cols[i]] = '-'
            valid_iso_vars = []
            for i, iso_col in enumerate(iso_cols):
                valid_iso_vars.append(machines.at[idx, iso_col]) if machines.at[idx, iso_col] != '-' else None
            for i, iso_col in enumerate(iso_cols):
                if i < len(valid_iso_vars):
                    machines.at[idx, iso_col] = valid_iso_vars[i]
                else:
                    machines.at[idx, iso_col] = '-'

# After feeder assignment, post-process Location Equipments IDs
if 'Location Equipments IDs' in machines.columns:
    loc_cols = [f'Con{i}' for i in range(1, 21) if f'Con{i}' in machines.columns]
    for idx, row in machines.iterrows():
        picture = str(row['Picture'])
        feeder_id = row.get('feeder_id', '-')
        loc_equip = str(row.get('Location Equipments IDs', ''))
        if not loc_equip or feeder_id == '-' or loc_equip == '-':
            continue
        loc_ids = [v.strip() for v in loc_equip.split(',') if v.strip()]
        loc_num = len(loc_ids)
        # Get feeder assignments for all machines
        valid_ids = []
        for id in loc_ids:
            # Find matching row for this ID
            match = machines[(machines['ID'] == id) & (machines['feeder_id'] == feeder_id)]
            if not match.empty or id == feeder_id or id in last_machines_list:
                valid_ids.append(id)
        # If any were removed, update Location Equipments IDs and LOC columns
        if set(valid_ids) != set(loc_ids):
            machines.at[idx, 'Location Equipments IDs'] = ','.join(valid_ids)
            # Remove LOC variable if its VisualName was removed
            for i, id in enumerate(loc_ids):
                if id not in valid_ids:
                    machines.at[idx, loc_cols[i]] = '-'
            valid_loc_vars = []
            for i, loc_col in enumerate(loc_cols):
                valid_loc_vars.append(machines.at[idx, loc_col]) if machines.at[idx, loc_col] != '-' else None
            for i, loc_col in enumerate(loc_cols):
                if i < len(valid_loc_vars):
                    machines.at[idx, loc_col] = valid_loc_vars[i]
                else:
                    machines.at[idx, loc_col] = '-'

machines.to_excel(output_file_2, index=False)
print(f'Exported {output_file_2} with feeder assignments.')

# Export last machines list
output_file_3 = r"{}\all_last_machines.xlsx".format(output_folder)
last_machines_df = pd.DataFrame(last_machines_list, columns=['All Last Machines'])
last_machines_df.to_excel(output_file_3, index=False)
print(f'Exported {output_file_3} with last machines list.')