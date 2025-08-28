import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict, deque

import pandas as pd

EMERGENCY_FILTER = True

# Add progress settings
PROGRESS_EVERY = 50000          # BFS dequeue progress step
ASSIGN_PROGRESS_EVERY = 200     # Assignment progress step


def run(xml_file, output_folder, use_scr_xml):
    # ---------- helpers for files ----------
    def safe_picture_name(p):
        # Make a filesystem-friendly file stem
        return re.sub(r'[^A-Za-z0-9_.-]+', '_', str(p))[:120]

    def per_picture_summary_path(picture):
        return os.path.join(output_folder, f"feeder_nop_paths_summary__{safe_picture_name(picture)}.csv")

    os.makedirs(output_folder, exist_ok=True)

    # ---------- load inputs ----------
    if use_scr_xml:
        subdest_df = pd.read_excel(r"{}\scr_machine_var.xlsx".format(output_folder))
    else:
        subdest_df = pd.read_excel(r"{}\alc_machine_var.xlsx".format(output_folder))
    machines = pd.read_excel(r"{}\alc_DB_FLIS.xlsx".format(output_folder))
    alc_nodes_df = pd.read_excel(r"{}\alc_Consolidated_Var.xlsx".format(output_folder))

    output_excel = r"{}\alc_DB_FLIS_with_feeder.xlsx".format(output_folder)

    print()
    print("="*40)
    print("  Network Traversal Debug Script (Per-Picture)")
    print("="*40)

    # ---------- constants / prefixes ----------
    feederPrefixes = set([
        "INTEGRATION_PROJECT_SLD_FDR_DSS_1_DOWN_ALIAS",
        # "INTEGRATION_PROJECT_NON_SMART_CB_SLD"
    ])

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
        # "INTEGRATION_PROJECT_NON_SMART_CB", # FDR
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

    # ---------- small helpers that depend on XML structures ----------
    def get_machine_name(elem_id):
        elem = all_elements[elem_id] if elem_id in all_elements else None
        if elem is None:
            return elem_id
        element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else elem_id
        parts = element_ref.split('.')
        name = parts[1] if len(parts) > 1 else element_ref
        return name

    def get_machine_name_filtered(elem_id):
        name = get_machine_name(elem_id)
        if '_NOP' in name.upper():
            return name
        for prefix in ignore_prefixes:
            if name.startswith(prefix):
                return None
        return name

    def is_valid_machine(elem_id):
        name = get_machine_name(elem_id)
        if '_NOP' in name.upper():
            return name
        for prefix in ignore_prefixes:
            if name.startswith(prefix):
                return False
        return name

    def get_machine_legs(machine_name):
        if not machine_name:
            return []
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
        machine_name = get_machine_name(elem_id)
        legs = get_machine_legs(machine_name)
        return legs > 2

    def is_feeder(elem_id):
        name = get_machine_name(elem_id)
        if any(name.startswith(prefix) for prefix in feederPrefixes):
            return True
        return False

    # ---------- map tables ----------
    id_to_NOP_Variables = {}
    for _, row in machines.iterrows():
        key = (row["Picture"], str(row["ID"]))
        id_to_NOP_Variables[key] = row["NOP_Variables"]

    id_to_FeederNo = {}
    for _, row in subdest_df.iterrows():
        key = (row["ScreenName"], str(row["ID"]))
        id_to_FeederNo[key] = row["FeederNo"]

    print(f"Loaded {len(id_to_FeederNo)} feeder mappings from subdest_df.")

    # ---------- load XML ----------
    tree = ET.parse(xml_file)
    root = tree.getroot()
    graph_elements = root.find('GraphElements')
    elements = graph_elements.findall('GraphElement')

    # Build element lookup and type map
    all_elements = {}
    element_types = defaultdict(int)
    elementid_to_graph_element = {}
    for elem in elements:
        elem_id = elem.find('ID').text
        elem_type = int(elem.find('Type').text)
        all_elements[elem_id] = elem
        elementid_to_graph_element[elem_id] = elem
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
        elem_name = get_machine_name(elem_id)
        if node1 is not None:
            node1_ids = [n.text for n in node1.findall('ID')]
            for n1 in node1_ids:
                if n1 not in connections[elem_id]:
                    connections[elem_id].add(n1)
                if elem_id not in connections[n1]:
                    connections[n1].add(elem_id)
        if node2 is not None:
            node2_ids = [n.text for n in node2.findall('ID')]
            for n2 in node2_ids:
                if n2 not in connections[elem_id]:
                    connections[elem_id].add(n2)
                if elem_id not in connections[n2]:
                    connections[n2].add(elem_id)

    # Group elements by Picture
    picture_groups = defaultdict(list)
    for elem in elements:
        picture = elem.find('Picture').text if elem.find('Picture') is not None else 'NO_PICTURE'
        if EMERGENCY_FILTER and ('EMERGENCY' in str(picture).upper() or 'EMRGENCY' in str(picture).upper()):
            continue
        picture_groups[picture].append(elem)

    print(f"\nFound {len(picture_groups)} unique pictures.")

    # For each picture, find unique feeders
    picture_feeders = {}
    for picture, elems in picture_groups.items():
        feeders = {}
        for elem in elems:
            elem_id = elem.find('ID').text
            element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else ""
            if is_feeder(elem_id):
                parts = element_ref.split('.')
                feeder_key = parts[1] if len(parts) > 1 else elem_id
                feeders[feeder_key] = elem_id
        picture_feeders[picture] = feeders
        print(f"Picture: {picture} | Unique feeders: {len(feeders)}")

    # ---------- var finder ----------
    def find_variable_stand_alone(element_id):
        graph_element = elementid_to_graph_element.get(element_id)
        if graph_element is None:
            return None
        element_ref = graph_element.find("ElementRef").text if graph_element.find("ElementRef") is not None else None
        variable = graph_element.find("Variable").text if graph_element.find("Variable") is not None else "-"
        element_type = graph_element.find("Type").text if graph_element.find("Type") is not None else None
        parts_ = element_ref.split(".") if element_ref else ["", ""]

        if len(parts_) > 1 and any(parts_[1].startswith(prefix) for prefix in ignore_prefixes):
            return None
        if len(parts_) > 1 and any(parts_[1].startswith(prefix) for prefix in specialPrefixes) and variable != "<No variable linked>":
            return variable

        if len(parts_) > 3 and element_type in ["2", "7"]:
            if parts_[2].startswith("INTEGRATION_PROJECT_ALC_ES") and parts_[3] == "DC" and variable != "<No variable linked>":
                return variable
        if len(parts_) > 3 and element_type in ["2", "7"]:
            if parts_[2].startswith("ALC_LBS") and variable != "<No variable linked>":
                return variable

        visited = set()
        stack = [element_id]
        counter = 0
        while stack:
            if counter > 50000:
                return None
            counter += 1
            current_id = stack.pop()
            if current_id in visited:
                continue
            visited.add(current_id)
            graph_element = elementid_to_graph_element.get(current_id)
            if graph_element is None:
                continue
            element_ref = graph_element.find("ElementRef").text if graph_element.find("ElementRef") is not None else None
            variable = graph_element.find("Variable").text if graph_element.find("Variable") is not None else "-"
            element_type = graph_element.find("Type").text if graph_element.find("Type") is not None else None
            parts = element_ref.split(".") if element_ref else ["", ""]

            if element_ref and len(parts) > 1 and len(parts_) > 1 and parts[1] == parts_[1]:
                if any(parts[1].startswith(prefix) for prefix in specialPrefixes) and variable != "<No variable linked>":
                    return variable
                if len(parts) > 3 and parts[2].startswith("ALC_LBS") and variable != "<No variable linked>":
                    return variable
                if element_type in ["2", "7"]:
                    if variable != "<No variable linked>" and variable.endswith("OC_ST"):
                        return variable
            else:
                continue

            next_node1_ids = [n.text for n in graph_element.findall(".//Node1IDs/ID")]
            next_node2_ids = [n.text for n in graph_element.findall(".//Node2IDs/ID")]
            stack.extend(next_node1_ids + next_node2_ids)
        return None

    # ---------- traversal state shared across pictures (small) ----------
    LINES_RESTRICTED = set()
    NOP_MACHINES_RESTRICTED = set()
    special_nop_machines_assigned = set()
    NOP_END_POINTS = set()
    NOP_MACHINES_2LEG = {}
    FEEDER_PATHS = {}
    last_machines_list = []

    # ---------- per-picture traversal and on-disk flush ----------
    _pictures_items = list(picture_feeders.items())
    _total_pictures = len(_pictures_items)

    for _pic_idx, (picture, feeders) in enumerate(_pictures_items, 1):
        print(f"\n--- Picture {_pic_idx}/{_total_pictures}: {picture} ---")
        print(f"  Feeders detected: {list(feeders.keys())}")

        # accumulator only for this picture
        summary_rows_pic = []

        def add_path_to_summary(picture_, feeder_key, path, nop_machine, end_reason, feeders_, legs_traversed=10):
            path_names = []
            seen_names = set()
            path_all_machines = []
            Full_ID_Path_str = ' -> '.join(path)
            path_len = len(path)
            for i, pid in enumerate(path):
                name_filtered = get_machine_name_filtered(pid)
                if pid in LINES_RESTRICTED:
                    path_len = i
                    break
                else:
                    path_all_machines.append(pid[:20])
                if name_filtered is not None:
                    if (picture_, name_filtered) in NOP_MACHINES_RESTRICTED:
                        break
                    if name_filtered not in seen_names:
                        path_names.append(name_filtered)
                        seen_names.add(name_filtered)

            if not path_names:
                return False
            path_str = ' -> '.join(path_names)
            path_all_machines_str = ' -> '.join(path_all_machines)
            first_machine = path_names[1] if len(path_names) > 1 else '-'
            last_machine = path_names[-1] if len(path_names) > 1 else '-'
            machine_count = len(path_names)

            if last_machine in feeders_:
                return False

            feeder_names_in_path = [n for n in path_names if n in feeders_]
            if len(set(feeder_names_in_path)) > 1 and path_names[len(set(feeder_names_in_path)) - 1] == feeder_names_in_path[-1]:
                for fn in feeder_names_in_path[:-1]:
                    if fn in path_names:
                        path_names.remove(fn)
                first_machine = path_names[1] if len(path_names) > 1 else '-'
                last_machine = path_names[-1] if len(path_names) > 1 else '-'
                machine_count = len(path_names)

            elif len(set(feeder_names_in_path)) > 1:
                return False

            summary_rows_pic.append({
                'Picture': picture_,
                'Feeder': feeder_key,
                'Path': ' -> '.join(path_names),
                'Full_Path': path_all_machines_str,
                'Full_ID_Path': Full_ID_Path_str,
                'NOP_Machine': nop_machine,
                'End_Reason': end_reason,
                'First_Machine': first_machine,
                'Last_Machine': last_machine,
                'Machine_Count': machine_count - 1
            })
            return True

        # BFS per feeder
        _feeders_items = list(feeders.items())
        _total_feeders = len(_feeders_items)
        for _f_idx, (feeder_key, feeder_id) in enumerate(_feeders_items, 1):
            print(f"Feeder {_f_idx}/{_total_feeders}: {feeder_key} (ID: {feeder_id})")
            if str(feeder_key).strip().upper().endswith('_NOP'):
                print(f"  Skipping feeder '{feeder_key}' because it ends with _NOP")
                continue

            visited_count = defaultdict(int)
            queue = deque([(feeder_id, [feeder_id], None, 0)])  # (current, path, leg_context, current_path_count)
            path_count = 0
            traversed_codes = {}
            dequeues = 0

            while queue and path_count < 2000000:
                current, path, leg_context, current_path_count = queue.popleft()
                dequeues += 1
                if dequeues % PROGRESS_EVERY == 0:
                    print(f"    progress: {dequeues:,} dequeued, paths={path_count}, queue={len(queue):,}")

                visit_key = (current, leg_context)
                if visited_count[visit_key] >= 10 or current in NOP_END_POINTS:
                    continue

                current_machine_name = get_machine_name(current) or current
                is_special_machine = any(current_machine_name.startswith(prefix) for prefix in specialPrefixes)
                if is_special_machine and visited_count[visit_key] >= 2:
                    continue

                visited_count[visit_key] += 1

                if current != feeder_id:
                    elem = all_elements[current]
                    element_ref = elem.find('ElementRef').text if elem.find('ElementRef') is not None else ""
                    element_ref_id = element_ref.split('.')[1] if '.' in element_ref else element_ref
                    variable = elem.find('Variable').text if elem.find('Variable') is not None else ""

                    # NOP handling
                    if 'NOP' in element_ref:
                        current_machine_name = get_machine_name(current) or current
                        if (picture, current_machine_name) not in traversed_codes:
                            traversed_codes[(picture, current_machine_name)] = []

                        if is_special_machine:
                            if current_machine_name in special_nop_machines_assigned:
                                path_ = [p for p in path if get_machine_name(p) != current_machine_name]
                                if add_path_to_summary(picture, feeder_key, path_, current_machine_name, 'SPECIAL_MACHINE_ENDPOINT', feeders):
                                    path_count += 1
                                    name = get_machine_name_filtered(current)
                                    if name is not None:
                                        last_machines_list.append(name)
                            else:
                                if add_path_to_summary(picture, feeder_key, path, current_machine_name, 'SPECIAL_NOP_ENDPOINT', feeders):
                                    path_count += 1
                                    name = get_machine_name_filtered(current)
                                    if name is not None:
                                        last_machines_list.append(name)
                                special_nop_machines_assigned.add(current_machine_name)
                            NOP_END_POINTS.add(current)
                            continue
                        else:
                            if id_to_NOP_Variables.get((picture, current_machine_name)) == "-":
                                NOP_MACHINES_RESTRICTED.add((picture, current_machine_name))
                                nop_machine = get_machine_name(current) or current
                                end_reason = "NOP_Y_MATCH"
                                if add_path_to_summary(picture, feeder_key, path, nop_machine, end_reason, feeders):
                                    path_count += 1
                                    name = get_machine_name_filtered(current)
                                    if name is not None:
                                        last_machines_list.append(name)
                                continue

                            allYcodes = ["Y" + str(i) for i in range(1, 6)]
                            allYcodes += ["Q" + str(i) for i in range(1, 3)]
                            allYcodes += ["TR_RIGHT", "TR_LEFT", "TR"]

                            m = re.search(r'NOP_((?:[YT]\d+_?)+)', element_ref_id)
                            if m:
                                codes_str = m.group(1)
                                nop_codes = re.findall(r'[YT]\d+', codes_str)
                                valid_code = []
                                for code in nop_codes:
                                    if code.startswith('Y'):
                                        if code in variable:
                                            valid_code.append(code)
                                    elif code.startswith('T'):
                                        q_code = '.Q' + code[1:]
                                        TR_code = 'TR' if code == "T1" and "1T" in element_ref_id else None
                                        TR_code_2T = "TR_RIGHT" if code == "T2" else "TR_LEFT"
                                        if q_code in variable:
                                            valid_code.append(code)
                                        elif TR_code and TR_code in variable:
                                            valid_code.append(code)
                                        elif TR_code_2T in variable:
                                            valid_code.append(code)

                                for code in allYcodes:
                                    if code in variable:
                                        traversed_codes[(picture, current_machine_name)].extend(code)
                                traversed_codes[(picture, current_machine_name)] = list(
                                    set(traversed_codes[(picture, current_machine_name)])
                                )

                                if valid_code:
                                    pass_flag = True
                                    # validate via node connections in alc_nodes_df
                                    for idx, row in alc_nodes_df.iterrows():
                                        if row["Picture"] != picture:
                                            continue
                                        node1_connections = str(row["Node1 connections"]).split(", ") if pd.notna(row["Node1 connections"]) else []
                                        node2_connections = str(row["Node2 connections"]).split(", ") if pd.notna(row["Node2 connections"]) else []
                                        for node in node1_connections + node2_connections:
                                            element_name = node.split('>')[1]
                                            if element_name == current_machine_name:
                                                for id_ in path:
                                                    if id_ == node.split('>')[0]:
                                                        con_variable = find_variable_stand_alone(id_)
                                                        if con_variable is None:
                                                            continue
                                                        vlist = []
                                                        for code in nop_codes:
                                                            if code.startswith('Y'):
                                                                if con_variable and code in con_variable:
                                                                    vlist.append(code)
                                                            elif code.startswith('T'):
                                                                q_code = '.Q' + code[1:]
                                                                TR_code = 'TR' if code == "T1" and "1T" in element_ref_id else None
                                                                TR_code_2T = "TR_RIGHT" if code == "T2" else "TR_LEFT"
                                                                if q_code in con_variable:
                                                                    vlist.append(code)
                                                                elif TR_code and TR_code in con_variable:
                                                                    vlist.append(code)
                                                                elif TR_code_2T in con_variable:
                                                                    vlist.append(code)
                                                        if vlist:
                                                            pass_flag = False
                                                            NOP_END_POINTS.add(id_)
                                                            for neighbor in connections.get(id_, set()):
                                                                if get_machine_name(neighbor).startswith("Line"):
                                                                    LINES_RESTRICTED.add(neighbor)
                                                            path2 = [p for p in path if get_machine_name(p) != current_machine_name]
                                                            nop_machine = get_machine_name(current) or current
                                                            end_reason = "NOP_Y_MATCH_2"
                                                            if add_path_to_summary(picture, feeder_key, path2, nop_machine, end_reason, feeders):
                                                                path_count += 1
                                                                name = get_machine_name_filtered(current)
                                                                if name is not None:
                                                                    last_machines_list.append(name)
                                                            break

                                    if pass_flag is False:
                                        continue

                                    nop_machine = get_machine_name(current) or current
                                    end_reason = "NOP_Y_MATCH"
                                    if add_path_to_summary(picture, feeder_key, path, nop_machine, end_reason, feeders):
                                        path_count += 1
                                        name = get_machine_name_filtered(current)
                                        if name is not None:
                                            last_machines_list.append(name)
                                    NOP_END_POINTS.add(current)

                                    if is_multi_leg_machine(current) and pass_flag:
                                        pass
                                    elif not is_multi_leg_machine(current):
                                        for neighbor in connections.get(current, set()):
                                            if get_machine_name(neighbor) == current_machine_name:
                                                NOP_END_POINTS.add(neighbor)
                                                for neighbor2 in connections.get(neighbor, set()):
                                                    if get_machine_name(neighbor2) == current_machine_name:
                                                        NOP_END_POINTS.add(neighbor2)
                                        NOP_MACHINES_2LEG[(picture, current_machine_name)] = (picture, current_machine_name)
                                        continue

                # neighbors
                all_neighbors = connections.get(current, set())
                neighbors = [neighbor for neighbor in all_neighbors if visited_count[(neighbor, leg_context)] < 10]
                if not neighbors and current != feeder_id:
                    current_machine_name = get_machine_name(current) or current
                    end_reason = f"DEAD_END_AT_{current_machine_name[:30]}"
                    if add_path_to_summary(picture, feeder_key, path, current_machine_name, end_reason, feeders):
                        path_count += 1
                        name = get_machine_name_filtered(current)
                        if name is not None:
                            last_machines_list.append(name)
                    continue

                for neighbor in neighbors:
                    queue.append((neighbor, path + [neighbor], leg_context, path_count))

            if path_count == 0:
                print("  No NOP end paths found from this feeder.")
            else:
                print(f"  Found {path_count} paths from this feeder.")

        # ---- end feeders in this picture: flush to disk ----
        if summary_rows_pic:
            df_pic = pd.DataFrame(summary_rows_pic).drop_duplicates(
                subset=["Picture", "Feeder", "First_Machine", "Last_Machine", "Machine_Count"]
            )
            # Filter out too-short paths
            df_pic = df_pic[df_pic['Path'].apply(lambda x: len([n for n in x.split('->') if n.strip()]) >= 1)]

            # Remove sub-paths within picture
            to_remove = set()
            paths = df_pic['Path'].tolist()
            pictures_col = df_pic['Picture'].tolist()
            for i, path_i in enumerate(paths):
                for j, path_j in enumerate(paths):
                    if i != j and path_i and path_j and path_i in path_j and pictures_col[i] == pictures_col[j]:
                        if len(path_i) < len(path_j):
                            to_remove.add(i)
            if to_remove:
                df_pic = df_pic.drop(df_pic.index[list(to_remove)])

            out_path = per_picture_summary_path(picture)
            df_pic.to_csv(out_path, index=False, encoding="utf-8")
            print(f"  → Wrote {len(df_pic)} rows to {out_path}")
            del df_pic
            summary_rows_pic.clear()
        else:
            print("  No valid NOP paths for this picture.")

    total_feeders = sum(len(f) for f in picture_feeders.values())
    print(f"\nTotal unique feeders across all pictures: {total_feeders}")
    print("Traversal complete (per-picture outputs written).")

    #############################################################################################
    # Now assign feeder info to machines based on per-picture summaries (streamed)

    print("\n")
    print("="*40)
    print("Assign Feeder Info to Machines Script (Per-Picture)")
    print("="*40)

    # Pre-create columns
    machines['feeder_id'] = '-'
    machines['first machine in feeder'] = '-'
    machines['last machines in feeder'] = '-'
    machines['Equipment Index'] = 0

    assigned_count = 0
    _total_rows = len(machines)

    # Iterate pictures that we actually traversed
    for _idx_pic, picture in enumerate(picture_groups.keys(), 1):
        out_path = per_picture_summary_path(picture)
        if not os.path.exists(out_path):
            continue

        summary_pic = pd.read_csv(out_path)

        # Build feeder -> last machines (within this picture)
        feeder_to_last_machines = defaultdict(set)
        for _, row in summary_pic.iterrows():
            feeder_id = row['Feeder']
            last_machine = row['Last_Machine']
            if pd.notna(last_machine) and last_machine != '-':
                feeder_to_last_machines[(str(picture), feeder_id)].add(last_machine)

        # Build (picture, machine) -> (feeder, first, last, index)
        machine_to_feeder = {}
        for _, row in summary_pic.iterrows():
            feeder_id = row['Feeder']
            first_machine = row['First_Machine']
            path_ids = [n.strip() for n in str(row['Path']).split('->') if n.strip()]
            last_machines_csv = ','.join(sorted(feeder_to_last_machines[(str(picture), feeder_id)]))
            for idx_m, m_id in enumerate(path_ids):
                machine_to_feeder[(str(picture), m_id)] = (feeder_id, first_machine, last_machines_csv, idx_m)

        # Apply only to machines from this picture
        mask_pic = machines['Picture'].astype(str) == str(picture)
        sub_idx = machines.index[mask_pic]
        for i in sub_idx:
            m_id = str(machines.at[i, 'ID'])
            key = (str(picture), m_id)
            if key in machine_to_feeder:
                feeder_id, first_m, last_ms, eq_idx = machine_to_feeder[key]
                machines.at[i, 'feeder_id'] = feeder_id
                machines.at[i, 'first machine in feeder'] = first_m
                machines.at[i, 'last machines in feeder'] = last_ms
                machines.at[i, 'Equipment Index'] = eq_idx
                assigned_count += 1

        # progress logging
        if _idx_pic % 5 == 0:
            print(f"  assignment progress: picture {_idx_pic}/{len(picture_groups)} processed; assigned so far={assigned_count}")

        del summary_pic
        del machine_to_feeder
        del feeder_to_last_machines

    print(f"Assigned feeder info to {assigned_count}/{_total_rows} machines.")

    # ---------- Post-processing Isolation Equipments Numbers ----------
    if 'Isolation Equipments Numbers' in machines.columns:
        iso_cols = [f'ISO{i}' for i in range(1, 21) if f'ISO{i}' in machines.columns]
        for idx, row in machines.iterrows():
            picture = str(row['Picture'])
            feeder_id = row.get('feeder_id', '-')
            iso_equip = str(row.get('Isolation Equipments Numbers', ''))
            if not iso_equip or feeder_id == '-' or iso_equip == '-':
                continue
            visual_names = [v for v in iso_equip.split(',') if v.strip()]
            valid_visuals = []
            for vname in visual_names:
                match = machines[(machines['VisualName'] == vname) & (machines['feeder_id'] == feeder_id)]
                match_fdr = vname in id_to_FeederNo.get((picture, feeder_id), [])
                if not match.empty:
                    valid_visuals.append(vname)
                if match_fdr and vname not in valid_visuals:
                    valid_visuals.append(vname)

            if set(valid_visuals) != set(visual_names):
                machines.at[idx, 'Isolation Equipments Numbers'] = ','.join(valid_visuals)
                for i, vname in enumerate(visual_names):
                    if vname not in valid_visuals and i < len(iso_cols):
                        machines.at[idx, iso_cols[i]] = '-'
                valid_iso_vars = []
                for i, iso_col in enumerate(iso_cols):
                    val = machines.at[idx, iso_col]
                    if val != '-':
                        valid_iso_vars.append(val)
                for i, iso_col in enumerate(iso_cols):
                    machines.at[idx, iso_col] = valid_iso_vars[i] if i < len(valid_iso_vars) else '-'

    # ---------- Post-processing Location Equipments IDs ----------
    # uses last_machines_list as in your original code; if you want to avoid this global growth,
    # you can compute from the per-picture CSV on demand.
    if 'Location Equipments IDs' in machines.columns:
        loc_cols = [f'Con{i}' for i in range(1, 21) if f'Con{i}' in machines.columns]
        for idx, row in machines.iterrows():
            picture = str(row['Picture'])
            feeder_id = row.get('feeder_id', '-')
            loc_equip = str(row.get('Location Equipments IDs', ''))
            if not loc_equip or feeder_id == '-' or loc_equip == '-':
                continue

            # build last_machines_set on demand from per-picture file
            pic_csv = per_picture_summary_path(picture)
            if os.path.exists(pic_csv):
                tmp = pd.read_csv(pic_csv, usecols=['Last_Machine'])
                last_machines_set = set(tmp['Last_Machine'].dropna().astype(str).tolist())
            else:
                last_machines_set = set()

            loc_ids = [v for v in loc_equip.split(',') if v.strip()]
            valid_ids = []
            for id_ in loc_ids:
                match = machines[(machines['ID'] == id_) & (machines['feeder_id'] == feeder_id)]
                if not match.empty or id_ == feeder_id or id_ in last_machines_set:
                    valid_ids.append(id_)

            if set(valid_ids) != set(loc_ids):
                machines.at[idx, 'Location Equipments IDs'] = ','.join(valid_ids)
                for i, id_ in enumerate(loc_ids):
                    if id_ not in valid_ids and i < len(loc_cols):
                        machines.at[idx, loc_cols[i]] = '-'
                valid_loc_vars = []
                for i, loc_col in enumerate(loc_cols):
                    val = machines.at[idx, loc_col]
                    if val != '-':
                        valid_loc_vars.append(val)
                for i, loc_col in enumerate(loc_cols):
                    machines.at[idx, loc_col] = valid_loc_vars[i] if i < len(valid_loc_vars) else '-'

    machines.to_excel(output_excel, index=False)
    print(f'Exported {output_excel} with feeder assignments.')

    # Optional: write a tiny index of all per-picture summary CSVs
    summary_index = []
    for picture in picture_groups.keys():
        path = per_picture_summary_path(picture)
        if os.path.exists(path):
            summary_index.append({"Picture": picture, "SummaryFile": path})
    if summary_index:
        idx_path = os.path.join(output_folder, "feeder_summary_index.csv")
        pd.DataFrame(summary_index).to_csv(idx_path, index=False)
        print(f"Wrote per-picture summary index: {idx_path}")
