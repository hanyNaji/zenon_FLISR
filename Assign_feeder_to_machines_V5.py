import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from functools import lru_cache

import pandas as pd

EMERGENCY_FILTER = True

# Progress settings
PROGRESS_EVERY = 50_000          # BFS dequeue progress step
ASSIGN_PROGRESS_EVERY = 200      # Assignment progress step


def run(xml_file, output_folder, use_scr_xml):
    # ---------- helpers ----------
    def safe_picture_name(p):
        return re.sub(r'[^A-Za-z0-9_.-]+', '_', str(p))[:120]

    def per_picture_summary_path(picture):
        return os.path.join(output_folder, f"feeder_nop_paths_summary__{safe_picture_name(picture)}.csv")

    os.makedirs(output_folder, exist_ok=True)

    # ---------- read inputs ----------
    # subdest_df: ScreenName, ID, FeederNo (for some post checks)
    sub_cols = ["ScreenName", "ID", "FeederNo"]
    sub_path = os.path.join(output_folder, "scr_machine_var.xlsx" if use_scr_xml else "alc_machine_var.xlsx")
    try:
        subdest_df = pd.read_excel(sub_path, usecols=sub_cols, dtype=str, engine="openpyxl")
    except Exception:
        subdest_df = pd.read_excel(sub_path, dtype=str, engine="openpyxl")
        subdest_df = subdest_df[[c for c in sub_cols if c in subdest_df.columns]]

    # exact original columns to preserve from alc_DB_FLIS
    con_cols = [f"Con{i}" for i in range(1, 15)]     # Con1..Con14
    iso_cols = [f"ISO{i}" for i in range(1, 15)]     # ISO1..ISO14
    base_cols = [
        "Picture","ID","Machine","VisualName","SMART",
        *con_cols, *iso_cols,
        "NOP","NOP_Variables",
        "Isolation Equipments Numbers","Location Equipments IDs"
    ]

    machines_path = os.path.join(output_folder, "alc_DB_FLIS.xlsx")
    try:
        machines = pd.read_excel(machines_path, usecols=lambda c: c in base_cols, dtype=str, engine="openpyxl")
    except Exception:
        machines = pd.read_excel(machines_path, dtype=str, engine="openpyxl")

    # ensure all expected columns exist (filled with "-")
    for c in base_cols:
        if c not in machines.columns:
            machines[c] = "-"

    # nodes df (lean)
    nodes_cols = ["Picture", "Node1 connections", "Node2 connections"]
    nodes_path = os.path.join(output_folder, "alc_Consolidated_Var.xlsx")
    try:
        alc_nodes_df = pd.read_excel(nodes_path, usecols=nodes_cols, dtype=str, engine="openpyxl")
    except Exception:
        alc_nodes_df = pd.read_excel(nodes_path, dtype=str, engine="openpyxl")
        alc_nodes_df = alc_nodes_df[[c for c in nodes_cols if c in alc_nodes_df.columns]]

    output_excel = os.path.join(output_folder, "alc_DB_FLIS_with_feeder.xlsx")

    print()
    print("="*40)
    print("  Network Traversal Debug Script (Fast/Light Per-Picture)")
    print("="*40)

    # ---------- constants / prefixes ----------
    feederPrefixes = (
        "INTEGRATION_PROJECT_SLD_FDR_DSS_1_DOWN_ALIAS",
    )
    specialPrefixes = (
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
        "INTEGRATION_PROJECT_SMART_SLD_LBS",
    )
    ignore_prefixes = (
        "INTEGRATION_PROJECT_NON_SMT_SECTIONALIZER",
        "INTEGRATION_PROJECT_NON_SMT_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_NON_SMT_SLD_LBS",
        "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
        "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
        "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
        "INTEGRATION_PROJECT_SLD_FDR_DSS_1_SEC",
        "INTEGRATION_PROJECT_OH_TRANSFORMER",
        "INTEGRATION_PROJECT_SMART_VOLTAGE",
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
        "CB",
    )

    NOP_PATTERN_YT = re.compile(r'NOP_((?:[YT]\d+_?)+)')

    # ---------- quick maps ----------
    id_to_NOP_Variables = {(row["Picture"], str(row["ID"])): row["NOP_Variables"]
                           for _, row in machines.iterrows()}
    id_to_FeederNo = {(row["ScreenName"], str(row["ID"])): row["FeederNo"]
                      for _, row in subdest_df.iterrows() if "ScreenName" in subdest_df.columns and "FeederNo" in subdest_df.columns}
    print(f"Loaded {len(id_to_FeederNo)} feeder mappings from subdest_df.")

    # ---------- parse XML ----------
    tree = ET.parse(xml_file)
    root = tree.getroot()
    graph_elements = root.find('GraphElements')
    elements = graph_elements.findall('GraphElement')

    name_by_id = {}
    picture_by_id = {}
    type_by_id = {}
    variable_by_id = {}
    element_ref_by_id = {}
    element_nodes_by_id = defaultdict(set)
    element_is_feeder = {}
    element_is_special = {}

    element_types_count = defaultdict(int)
    node_to_elements = defaultdict(list)
    node_feeder_names = defaultdict(set)

    def _extract_name_from_element_ref(ref):
        if not ref:
            return ""
        parts = ref.split(".")
        return parts[1] if len(parts) > 1 else ref

    for ge in elements:
        eid = ge.findtext('ID')
        etype = ge.findtext('Type') or ""
        eref = ge.findtext('ElementRef') or ""
        pic = ge.findtext('Picture') or 'NO_PICTURE'
        var = ge.findtext('Variable') or "-"

        nm = _extract_name_from_element_ref(eref)
        name_by_id[eid] = nm
        picture_by_id[eid] = pic
        type_by_id[eid] = etype
        variable_by_id[eid] = var
        element_ref_by_id[eid] = eref
        element_is_feeder[eid] = nm.startswith(feederPrefixes)
        element_is_special[eid] = nm.startswith(specialPrefixes)

        element_types_count[int(etype) if etype.isdigit() else -1] += 1

        for tag in ("Node1IDs", "Node2IDs"):
            node_tag = ge.find(tag)
            if node_tag is not None:
                for n in node_tag.findall('ID'):
                    nid = n.text
                    element_nodes_by_id[eid].add(nid)
                    node_to_elements[nid].append(eid)

    print("Element type distribution:")
    for t, c in sorted(element_types_count.items()):
        print(f"  Type {t}: {c}")

    for nid, els in node_to_elements.items():
        for e in els:
            if element_is_feeder.get(e, False):
                node_feeder_names[nid].add(name_by_id[e])

    picture_groups = defaultdict(list)
    for eid, pic in picture_by_id.items():
        if EMERGENCY_FILTER and ('EMERGENCY' in str(pic).upper() or 'EMRGENCY' in str(pic).upper()):
            continue
        picture_groups[pic].append(eid)

    print(f"\nFound {len(picture_groups)} unique pictures.")

    picture_feeders = {}
    for picture, elem_ids in picture_groups.items():
        feeders = {}
        for eid in elem_ids:
            if element_is_feeder.get(eid, False):
                feeders[name_by_id[eid]] = eid
        picture_feeders[picture] = feeders
        print(f"Picture: {picture} | Unique feeders: {len(feeders)}")

    # ---------- helpers ----------
    def get_machine_legs_from_name(name: str) -> int:
        if not name:
            return 2
        if name.startswith('2L'): return 2
        if name.startswith('3L'): return 3
        if name.startswith('4L'): return 4
        if name.startswith('5L'): return 5
        return 2

    @lru_cache(maxsize=None)
    def is_multi_leg_machine(eid: str) -> bool:
        return get_machine_legs_from_name(name_by_id.get(eid, "")) > 2

    def name_filtered(eid: str):
        nm = name_by_id.get(eid, eid)
        if "_NOP" in nm.upper():
            return nm
        return None if nm.startswith(ignore_prefixes) else nm

    # build quick connection index from consolidated sheet
    con_index = defaultdict(lambda: defaultdict(set))  # picture -> element_name -> {connected_element_ids}
    if not alc_nodes_df.empty:
        for _, r in alc_nodes_df.iterrows():
            pic = str(r.get("Picture", ""))
            for col in ("Node1 connections", "Node2 connections"):
                v = r.get(col, "")
                if pd.isna(v) or not v:
                    continue
                for item in str(v).split(", "):  # "id>element_name"
                    if ">" not in item:
                        continue
                    cid, ename = item.split(">", 1)
                    con_index[pic][ename].add(cid)

    @lru_cache(maxsize=None)
    def find_variable_stand_alone(element_id: str):
        eref0 = element_ref_by_id.get(element_id, "")
        var0 = variable_by_id.get(element_id, "-")
        etype0 = type_by_id.get(element_id, None)

        parts_ = eref0.split(".") if eref0 else []
        if len(parts_) > 1 and parts_[1].startswith(ignore_prefixes):
            return None
        if len(parts_) > 1 and parts_[1].startswith(specialPrefixes) and var0 != "<No variable linked>":
            return var0

        if len(parts_) > 3 and etype0 in ("2","7"):
            if parts_[2].startswith("INTEGRATION_PROJECT_ALC_ES") and parts_[3] == "DC" and var0 != "<No variable linked>":
                return var0
        if len(parts_) > 3 and etype0 in ("2","7"):
            if parts_[2].startswith("ALC_LBS") and var0 != "<No variable linked>":
                return var0

        visited = set()
        stack = [element_id]
        counter = 0
        while stack:
            if counter > 50_000:
                return None
            counter += 1
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)

            eref = element_ref_by_id.get(cur, "")
            var = variable_by_id.get(cur, "-")
            etype = type_by_id.get(cur, None)
            parts = eref.split(".") if eref else []

            if eref and len(parts) > 1 and len(parts_) > 1 and parts[1] == parts_[1]:
                if parts[1].startswith(specialPrefixes) and var != "<No variable linked>":
                    return var
                if len(parts) > 3 and parts[2].startswith("ALC_LBS") and var != "<No variable linked>":
                    return var
                if etype in ("2","7") and var != "<No variable linked>" and str(var).endswith("OC_ST"):
                    return var

            # expand via nodes
            for nid in element_nodes_by_id.get(cur, ()):
                for nb in node_to_elements.get(nid, ()):
                    if nb != cur and nb not in visited:
                        stack.append(nb)
        return None

    # ---------- traversal state ----------
    LINES_RESTRICTED = set()
    NOP_MACHINES_RESTRICTED = set()
    special_nop_machines_assigned = set()
    NOP_END_POINTS = set()
    NOP_MACHINES_2LEG = {}
    FEEDER_PATHS = {}
    last_machines_list = []

    # ---------- per-picture traversal ----------
    _pictures_items = list(picture_feeders.items())
    _total_pictures = len(_pictures_items)

    for _pic_idx, (picture, feeders) in enumerate(_pictures_items, 1):
        print(f"\n--- Picture {_pic_idx}/{_total_pictures}: {picture} ---")
        print(f"  Feeders detected: {list(feeders.keys())}")

        summary_rows_pic = []

        def add_path_to_summary(picture_, feeder_key, path, nop_machine, end_reason, feeders_):
            path_names = []
            seen_names = set()
            full_id_path = ' -> '.join(path)

            for pid in path:
                if pid in LINES_RESTRICTED:
                    break
                nmf = name_filtered(pid)
                if nmf is not None:
                    if (picture_, nmf) in NOP_MACHINES_RESTRICTED:
                        break
                    if nmf not in seen_names:
                        path_names.append(nmf)
                        seen_names.add(nmf)

            if not path_names:
                return False

            first_machine = path_names[1] if len(path_names) > 1 else '-'
            last_machine = path_names[-1] if len(path_names) > 1 else '-'
            if last_machine in feeders_:
                return False

            feeder_names_in_path = [n for n in path_names if n in feeders_]
            if len(set(feeder_names_in_path)) > 1 and path_names[len(set(feeder_names_in_path)) - 1] == feeder_names_in_path[-1]:
                for fn in feeder_names_in_path[:-1]:
                    if fn in path_names:
                        path_names.remove(fn)
                first_machine = path_names[1] if len(path_names) > 1 else '-'
                last_machine = path_names[-1] if len(path_names) > 1 else '-'
            elif len(set(feeder_names_in_path)) > 1:
                return False

            summary_rows_pic.append({
                'Picture': picture_,
                'Feeder': feeder_key,
                'Path': ' -> '.join(path_names),
                'Full_Path': ' -> '.join([p[:20] for p in path]),
                'Full_ID_Path': full_id_path,
                'NOP_Machine': nop_machine,
                'End_Reason': end_reason,
                'First_Machine': first_machine,
                'Last_Machine': last_machine,
                'Machine_Count': max(0, len(path_names) - 1),
            })
            return True

        # feeder loop
        _feeders_items = list(feeders.items())
        _total_feeders = len(_feeders_items)

        for _f_idx, (feeder_key, feeder_id) in enumerate(_feeders_items, 1):
            print(f"Feeder {_f_idx}/{_total_feeders}: {feeder_key} (ID: {feeder_id})")
            if str(feeder_key).strip().upper().endswith('_NOP'):
                print(f"  Skipping feeder '{feeder_key}' because it ends with _NOP")
                continue

            start_feeder_name = feeder_key

            # neighbor lister (uses start_feeder_name)
            def list_neighbors(elem_id: str):
                out = set()
                for nid in element_nodes_by_id.get(elem_id, ()):
                    for nb in node_to_elements.get(nid, ()):
                        if nb == elem_id:
                            continue
                        if element_is_feeder.get(nb, False) and name_by_id.get(nb) != start_feeder_name:
                            continue
                        out.add(nb)
                return out

            visited_count = defaultdict(int)
            queue = deque([(feeder_id, [feeder_id], 0)])  # (current_element, path_ids, path_counter)
            path_count = 0
            dequeues = 0

            while queue and path_count < 2_000_000:
                current, path, current_path_count = queue.popleft()
                dequeues += 1
                if dequeues % PROGRESS_EVERY == 0:
                    print(f"    progress: {dequeues:,} dequeued, paths={path_count}, queue={len(queue):,}")

                if visited_count[current] >= 10 or current in NOP_END_POINTS:
                    continue
                visited_count[current] += 1

                if current != feeder_id:
                    current_name = name_by_id.get(current, current)
                    eref = element_ref_by_id.get(current, "")
                    variable = variable_by_id.get(current, "")

                    # NOP handling
                    if "NOP" in eref:
                        if element_is_special.get(current, False):
                            if current_name in special_nop_machines_assigned:
                                path2 = [p for p in path if name_by_id.get(p, "") != current_name]
                                if add_path_to_summary(picture, feeder_key, path2, current_name, 'SPECIAL_MACHINE_ENDPOINT', feeders):
                                    path_count += 1
                                    nmf = name_filtered(current)
                                    if nmf is not None:
                                        last_machines_list.append(nmf)
                            else:
                                if add_path_to_summary(picture, feeder_key, path, current_name, 'SPECIAL_NOP_ENDPOINT', feeders):
                                    path_count += 1
                                    nmf = name_filtered(current)
                                    if nmf is not None:
                                        last_machines_list.append(nmf)
                                special_nop_machines_assigned.add(current_name)
                            NOP_END_POINTS.add(current)
                            continue
                        else:
                            # sheet override
                            if id_to_NOP_Variables.get((picture, current_name)) == "-":
                                NOP_MACHINES_RESTRICTED.add((picture, current_name))
                                if add_path_to_summary(picture, feeder_key, path, current_name, "NOP_Y_MATCH", feeders):
                                    path_count += 1
                                    nmf = name_filtered(current)
                                    if nmf is not None:
                                        last_machines_list.append(nmf)
                                continue

                            m = NOP_PATTERN_YT.search(eref.split(".")[1] if "." in eref else eref)
                            if m:
                                nop_codes = re.findall(r'[YT]\d+', m.group(1))

                                valid_code = []
                                for code in nop_codes:
                                    if code.startswith('Y'):
                                        if code in variable:
                                            valid_code.append(code)
                                    else:
                                        q_code = f".Q{code[1:]}"
                                        if (q_code in variable or
                                            "TR_RIGHT" in variable or "TR_LEFT" in variable or "TR" in variable):
                                            valid_code.append(code)

                                pass_flag = True
                                if valid_code:
                                    connected_ids = con_index[str(picture)].get(current_name, set())
                                    if connected_ids:
                                        for id_on_path in path:
                                            if id_on_path in connected_ids:
                                                con_variable = find_variable_stand_alone(id_on_path)
                                                if con_variable:
                                                    vlist = []
                                                    for code in nop_codes:
                                                        if code.startswith('Y'):
                                                            if code in con_variable:
                                                                vlist.append(code)
                                                        else:
                                                            q_code = f".Q{code[1:]}"
                                                            if (q_code in con_variable or
                                                                "TR_RIGHT" in con_variable or
                                                                "TR_LEFT" in con_variable or "TR" in con_variable):
                                                                vlist.append(code)
                                                    if vlist:
                                                        pass_flag = False
                                                        NOP_END_POINTS.add(id_on_path)
                                                        for nb in list_neighbors(id_on_path):
                                                            if name_by_id.get(nb, "").startswith("Line"):
                                                                LINES_RESTRICTED.add(nb)
                                                        path2 = [p for p in path if name_by_id.get(p, "") != current_name]
                                                        if add_path_to_summary(picture, feeder_key, path2, current_name, "NOP_Y_MATCH_2", feeders):
                                                            path_count += 1
                                                            nmf = name_filtered(current)
                                                            if nmf is not None:
                                                                last_machines_list.append(nmf)
                                                        break

                                    if not pass_flag:
                                        continue

                                    if add_path_to_summary(picture, feeder_key, path, current_name, "NOP_Y_MATCH", feeders):
                                        path_count += 1
                                        nmf = name_filtered(current)
                                        if nmf is not None:
                                            last_machines_list.append(nmf)
                                    NOP_END_POINTS.add(current)

                                    if not is_multi_leg_machine(current):
                                        for nid in element_nodes_by_id.get(current, ()):
                                            for nb in node_to_elements.get(nid, ()):
                                                if name_by_id.get(nb, "") == current_name:
                                                    NOP_END_POINTS.add(nb)
                                                    for nb2 in list_neighbors(nb):
                                                        if name_by_id.get(nb2, "") == current_name:
                                                            NOP_END_POINTS.add(nb2)
                                        NOP_MACHINES_2LEG[(picture, current_name)] = (picture, current_name)
                                        continue

                # neighbors
                neighbors = [nb for nb in list_neighbors(current) if visited_count[nb] < 10]
                if not neighbors and current != feeder_id:
                    current_name = name_by_id.get(current, current)
                    end_reason = f"DEAD_END_AT_{current_name[:30]}"
                    if add_path_to_summary(picture, feeder_key, path, current_name, end_reason, feeders):
                        path_count += 1
                        nmf = name_filtered(current)
                        if nmf is not None:
                            last_machines_list.append(nmf)
                    continue

                for nb in neighbors:
                    queue.append((nb, path + [nb], path_count))

            if path_count == 0:
                print("  No NOP end paths found from this feeder.")
            else:
                print(f"  Found {path_count} paths from this feeder.")

        # ---- flush per-picture ----
        if summary_rows_pic:
            df_pic = pd.DataFrame(summary_rows_pic).drop_duplicates(
                subset=["Picture", "Feeder", "First_Machine", "Last_Machine", "Machine_Count"]
            )
            df_pic = df_pic[df_pic['Path'].apply(lambda x: len([n for n in x.split('->') if n.strip()]) >= 1)]

            to_remove = set()
            paths = df_pic['Path'].tolist()
            pics_col = df_pic['Picture'].tolist()
            for i, p_i in enumerate(paths):
                if not p_i:
                    continue
                for j, p_j in enumerate(paths):
                    if i != j and p_j and p_i in p_j and pics_col[i] == pics_col[j] and len(p_i) < len(p_j):
                        to_remove.add(i)
                        break
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
    print("Traversal complete (fast, per-picture).")

    #############################################################################################
    # Assignment (stream per-picture summaries)

    print("\n")
    print("="*40)
    print("Assign Feeder Info to Machines Script (Per-Picture, Lean)")
    print("="*40)

    # Ensure target columns exist (new ones)
    for c, default in [
        ("feeder_id","-"),
        ("first machine in feeder","-"),
        ("last machines in feeder","-"),
        ("Equipment Index",0),
    ]:
        if c not in machines.columns:
            machines[c] = default

    assigned_count = 0
    _total_rows = len(machines)

    for _idx_pic, picture in enumerate(picture_groups.keys(), 1):
        out_path = per_picture_summary_path(picture)
        if not os.path.exists(out_path):
            continue

        summary_pic = pd.read_csv(out_path, dtype=str)

        feeder_to_last_machines = defaultdict(set)
        for _, row in summary_pic.iterrows():
            feeder_id = row['Feeder']
            last_machine = row['Last_Machine']
            if pd.notna(last_machine) and last_machine != '-':
                feeder_to_last_machines[(str(picture), feeder_id)].add(last_machine)

        machine_to_feeder = {}
        for _, row in summary_pic.iterrows():
            feeder_id = row['Feeder']
            first_machine = row['First_Machine']
            path_ids = [n.strip() for n in str(row['Path']).split('->') if n.strip()]
            last_machines_csv = ','.join(sorted(feeder_to_last_machines[(str(picture), feeder_id)]))
            for idx_m, m_id in enumerate(path_ids):
                machine_to_feeder[(str(picture), m_id)] = (feeder_id, first_machine, last_machines_csv, idx_m)

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

        if _idx_pic % 5 == 0:
            print(f"  assignment progress: picture {_idx_pic}/{len(picture_groups)} processed; assigned so far={assigned_count}")

        del summary_pic
        del machine_to_feeder
        del feeder_to_last_machines

    print(f"Assigned feeder info to {assigned_count}/{_total_rows} machines.")

    # ---------- Post-processing Isolation Equipments Numbers ----------
    if 'Isolation Equipments Numbers' in machines.columns:
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
                # compact ISO vars (keep order, fill '-' to 14)
                vals = [machines.at[idx, c] for c in iso_cols if machines.at[idx, c] != '-']
                for i, c in enumerate(iso_cols):
                    machines.at[idx, c] = vals[i] if i < len(vals) else '-'

    # ---------- Post-processing Location Equipments IDs ----------
    if 'Location Equipments IDs' in machines.columns:
        for idx, row in machines.iterrows():
            picture = str(row['Picture'])
            feeder_id = row.get('feeder_id', '-')
            loc_equip = str(row.get('Location Equipments IDs', ''))
            if not loc_equip or feeder_id == '-' or loc_equip == '-':
                continue

            # build last_machines_set on demand
            pic_csv = per_picture_summary_path(picture)
            if os.path.exists(pic_csv):
                tmp = pd.read_csv(pic_csv, usecols=['Last_Machine'], dtype=str)
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
                vals = [machines.at[idx, c] for c in con_cols if machines.at[idx, c] != '-']
                for i, c in enumerate(con_cols):
                    machines.at[idx, c] = vals[i] if i < len(vals) else '-'

    # ---------- final column order (exactly as requested) ----------
    final_order = base_cols + ["feeder_id", "first machine in feeder", "last machines in feeder", "Equipment Index"]
    # ensure all are present
    for c in final_order:
        if c not in machines.columns:
            machines[c] = "-" if c != "Equipment Index" else 0
    machines = machines[final_order]

    machines.to_excel(output_excel, index=False)
    print(f'Exported {output_excel} with feeder assignments.')

    # Optional: index of per-picture summary CSVs
    summary_index = []
    for picture in picture_groups.keys():
        path = per_picture_summary_path(picture)
        if os.path.exists(path):
            summary_index.append({"Picture": picture, "SummaryFile": path})
    if summary_index:
        idx_path = os.path.join(output_folder, "feeder_summary_index.csv")
        pd.DataFrame(summary_index).to_csv(idx_path, index=False)
        print(f"Wrote per-picture summary index: {idx_path}")
