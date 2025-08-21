# FLISR Database Creation Project

## Project Overview

The **FLISR (Fault Location, Isolation, and Service Restoration) Database Creation** project is a comprehensive electrical network analysis system designed to process SCADA XML data and create machine databases for power distribution networks. The project consists of 5 interconnected Python scripts that work together to extract, process, and analyze electrical network topology data.

## Project Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   name_var.py   │    │ Extract_var.py  │    │Alc_Machines_Iso.│
│   (Script 1)    │    │   (Script 2)    │    │    py (Script 3)│
│                 │    │                 │    │                 │
│ Extract machine │    │ Extract screen  │    │ Build machine   │
│ names & vars    │────│ variables &     │────│ connections &   │
│ from XML        │    │ smart flags     │    │ isolation paths │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                ┌─────────────────▼─────────────────┐
                │   assign_feeder_to_machines.py    │
                │            (Script 4)             │
                │                                   │
                │    Network traversal & feeder     │
                │    assignment using BFS algorithm │
                └─────────────────┬─────────────────┘
                                 │
                ┌─────────────────▼─────────────────┐
                │     machine_machines_V3.py       │
                │            (Script 5)            │
                │                                   │
                │   Final database formatting &    │
                │   FLISR database generation      │
                └───────────────────────────────────┘
```

## Scripts Overview

### 1. Extract_data_ALC.py - Machine Name and Variable Extractor
**Purpose**: Extract machine names and associated variables from XML SCADA data

**Input**: 
- XML file: `alc.XML` (SCADA system export)

**Output**: 
- Excel file: `alc_name_var.xlsx`

**Key Functions**:
- Parse XML GraphElements to extract machine names
- Filter out ignored prefixes (transformers, meters, etc.)
- Extract variable associations for each machine
- Identify SMART vs NON-SMART equipment

**Process**:
1. Parse XML and group elements by picture/screen
2. Extract ElementRef names and split by '.' delimiter
3. Filter machines using ignore_prefixes list
4. Find variable associations through graph traversal
5. Generate clean machine name database

### 2. Extract_data_SCREENS.py - Screen Variable and Smart Flag Extractor  
**Purpose**: Extract screen variables and determine SMART equipment flags from screen definitions

**Input**: 
- XML file: Screen definition XML (e.g., `RUW_SCR.XML`)
- Excel file: `alc_name_var.xlsx` (from Script 1)

**Output**: 
- Excel file: `scr_machine_var.xlsx`

**Key Functions**:
- Parse screen XML TYPE=130 elements
- Extract VisualName, SubstituteDestination, LinkName
- Determine SMART/NON-SMART classification
- Extract station and feeder information for FDR elements

**Process**:
1. Parse screen XML for TYPE=130 elements
2. Extract visual names and substitute destinations
3. Match with smart_symbol_names set to determine SMART flag
4. Build lookup table for machine properties
5. Merge with name_var data to create comprehensive machine database

### 3. Alc_Machines_loc_Iso.py - Machine Connection and Isolation Analysis
**Purpose**: Build machine-to-machine connections and isolation equipment paths

**Input**: 
- XML file: `alc.XML` (SCADA system export)
- Excel file: `scr_machine_var.xlsx` (from Script 2)

**Output**: 
- Excel files: 
  - `alc_Consolidated_Var.xlsx` (connection data)
  - `machine_connections_alone.xlsx` (direct connections)
  - `machine_connections_iso.xlsx` (with isolation paths)
  - `alc_DB_FLIS.xlsx` (final machine database)

**Key Functions**:
- Build connection graph from XML line/connection data
- Find machine-to-machine connections using BFS traversal
- Identify isolation equipment (SMART devices) for each machine
- Generate variable names for control operations

**Process**:
1. Parse XML and build connection dictionary
2. Create consolidated connection data
3. Use BFS to find all connected machines for each machine
4. Identify isolation equipment paths using SMART device detection
5. Generate control variables (EF_ST suffixes) for operations

### 4. assign_feeder_to_machines.py - Network Traversal and Feeder Assignment
**Purpose**: Traverse electrical network and assign machines to feeders using advanced BFS algorithm

**Input**: 
- XML file: `alc.XML` (SCADA system export)
- Excel file: `alc_DB_FLIS.xlsx` (from Script 3)

**Output**: 
- Excel files:
  - `feeder_nop_paths_summary.xlsx` (path analysis)
  - `alc_DB_FLIS_with_feeder.xlsx` (machines with feeder assignments)

**Key Features**:
- **Enhanced BFS Algorithm**: Uses all connection types (not just Type 128)
- **Special Machine Handling**: Single-assignment logic for auto reclosers, sectionalizers
- **NOP Detection**: Multi-leg NOP machine support with Y-leg pattern matching
- **Visit Limits**: Prevents infinite loops with 100-visit limits per element
- **Debug Framework**: Comprehensive debugging for specific feeder analysis

**Process**:
1. Build comprehensive connection graph using all element types
2. Group elements by picture and identify feeders
3. For each feeder, perform BFS traversal to find connected machines
4. Handle special machines (single assignment to first encountering feeder)
5. Detect NOP endpoints with enhanced multi-leg support
6. Generate path summaries with end reasons
7. Assign feeder information to machine database

### 5. Machine_data_flisr.py - Final Database Formatting
**Purpose**: Format final FLISR database with project-specific configurations

**Input**: 
- Excel file: `alc_DB_FLIS_with_feeder.xlsx` (from Script 4)
- Excel file: `scr_machine_var.xlsx` (from Script 2)

**Output**: 
- Excel file: Project-specific FLISR database (e.g., `DWD_RUW_DB_FLISR.xlsx`)

**Key Functions**:
- Apply project-specific naming conventions
- Add administration and office information
- Format feeder numbers and station names
- Generate final database structure for FLISR system

## Technical Details

### Connection Graph Algorithm
The project uses an enhanced BFS (Breadth-First Search) algorithm with the following improvements:

1. **All Connection Types**: Uses all XML element types with Node1IDs/Node2IDs, not just Type 128
2. **Visit Tracking**: Implements visit limits to prevent infinite loops in circular networks
3. **Multi-leg Support**: Handles complex NOP machines with multiple Y-legs
4. **Special Machine Logic**: Ensures auto reclosers and sectionalizers are only assigned to first feeder

### Data Flow
```
XML SCADA Data → Machine Names → Screen Variables → Connections → Network Traversal → Final Database
```

### Key Data Structures
- **connections**: `defaultdict(set)` - Network connectivity graph
- **all_elements**: `dict` - Element ID to XML element mapping
- **picture_feeders**: `dict` - Picture to feeder mapping
- **visited_count**: `defaultdict(int)` - Visit tracking for loop prevention

### Error Handling
- **Recursion Prevention**: Iterative algorithms replace recursive functions
- **Visit Limits**: 10-100 visit limits prevent infinite loops
- **Data Validation**: Comprehensive checks for missing or malformed data
- **Debug Output**: Detailed logging for troubleshooting network issues

## Configuration Requirements

### File Paths (Update These)
```python
# Script 1 - name_var.py
input_file = r"C:\Users\USER\DAWADMI_RUWAYDAH\RT\FILES\zenon\system\alc.XML"

# Script 2 - Extract_var.py  
xml_file = r"D:\Line follower\DWD\RUW_SCR.XML"

# Script 3 - Alc_Machines_Iso.py
input_file_1 = r"C:\Users\USER\DAWADMI_RUWAYDAH\RT\FILES\zenon\system\alc.XML"

# Script 4 - assign_feeder_to_machines.py
xml_file = r"C:\Users\USER\DAWADMI_RUWAYDAH\RT\FILES\zenon\system\alc.XML"

# Script 5 - machine_machines_V3.py
project_name = "DAWADMI_RUWAYDAH#"
Administration = "DWD"
office_name = "RUW"
```

### Prefix Configuration
The system uses comprehensive prefix lists to filter relevant equipment:

```python
# Equipment to ignore (transformers, meters, etc.)
ignore_prefixes = [
    "INTEGRATION_PROJECT_OH_TRANSFORMER",
    "INTEGRATION_PROJECT_TRANSFORMER",
    "INTEGRATION_PROJECT_SMART_VOLTAGE",
    # ... (29 total prefixes)
]

# Special equipment requiring single assignment
specialPrefixes = set([
    "INTEGRATION_PROJECT_NON_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_NON_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_NON_SMART_SLD_LBS",
    "INTEGRATION_PROJECT_SMART_AUTO_RECLOSER",
    "INTEGRATION_PROJECT_SMART_SECTIONALIZER",
    "INTEGRATION_PROJECT_SMART_SLD_LBS"
])
```

## Execution Sequence

1. **Run name_var.py**: Extract machine names and variables
2. **Run Extract_var.py**: Extract screen variables and SMART flags
3. **Run Alc_Machines_Iso.py**: Build connections and isolation paths
4. **Run assign_feeder_to_machines.py**: Perform network traversal and feeder assignment
5. **Run machine_machines_V3.py**: Generate final FLISR database

## Output Files

| File | Purpose | Key Columns |
|------|---------|-------------|
| `alc_name_var.xlsx` | Machine names and variables | Picture, ID, VisualName, SMART, Variable |
| `scr_machine_var.xlsx` | Screen variables and properties | ScreenName, ID, VisualName, SubstituteDestination, SMART |
| `alc_DB_FLIS.xlsx` | Complete machine database | Picture, ID, Machine, VisualName, SMART, Con1-7, ISO1-7 |
| `feeder_nop_paths_summary.xlsx` | Network path analysis | Picture, Feeder, Path, NOP_Machine, End_Reason |
| `alc_DB_FLIS_with_feeder.xlsx` | Machines with feeder assignments | All previous + feeder id, first machine, last machine |
| Final FLISR Database | Project-specific formatted database | All machine data formatted for FLISR system |

## Advanced Features

### Enhanced Network Traversal
- **Multi-type Connections**: Processes all XML connection types for comprehensive network mapping
- **Circular Network Handling**: Advanced visit tracking prevents infinite loops in meshed networks
- **NOP Detection**: Sophisticated endpoint detection with multi-leg NOP support

### Special Equipment Handling
- **Single Assignment Logic**: Auto reclosers and sectionalizers assigned only to first encountering feeder
- **SMART Device Detection**: Automatic classification of intelligent vs conventional equipment
- **Variable Generation**: Automatic control variable generation with appropriate suffixes

### Debug Capabilities
- **Targeted Debugging**: Can debug specific feeders for connectivity analysis
- **Path Tracing**: Detailed path tracing with end reasons
- **Visit Monitoring**: Real-time visit count tracking for performance optimization

## Performance Considerations

- **Memory Efficient**: Uses generators and iterative algorithms to minimize memory usage
- **Progress Tracking**: tqdm progress bars for long-running operations
- **Optimized Lookups**: Dictionary-based lookups for O(1) element access
- **Batch Processing**: Processes data in picture-based batches for efficiency

## Troubleshooting

### Common Issues
1. **Recursion Errors**: Fixed by implementing iterative algorithms with visit limits
2. **Missing Connections**: Resolved by using all connection types instead of just Type 128
3. **Infinite Loops**: Prevented by visit count tracking and limits
4. **Memory Issues**: Handled by batch processing and efficient data structures

### Debug Features
- Set `debug_this_feeder = True` for specific feeder analysis
- Monitor queue sizes and visit counts during traversal
- Check end reasons for path termination analysis

## Dependencies

```python
import xml.etree.ElementTree as ET
import pandas as pd
import re
from collections import defaultdict, deque
from tqdm import tqdm
from thefuzz import process
```

## Version History

- **v1.0**: Initial implementation with basic BFS traversal
- **v2.0**: Added special machine handling and NOP detection
- **v3.0**: Enhanced with all connection types and visit limits
- **v4.0**: Added comprehensive debug framework and error handling
- **v5.0**: Current version with optimized performance and full feature set

## License and Usage

This project is designed for electrical utility SCADA system analysis and FLISR database creation. Ensure proper configuration of file paths and prefix lists for your specific SCADA system before execution.

---

## Output Files Summary

| Script                        | Output Files                                    | Description                                                      |
|-------------------------------|-------------------------------------------------|------------------------------------------------------------------|
| assign_feeder_to_machines_V1.py | feeder_nop_paths_summary.xlsx,<br>alc_DB_FLIS_with_feeder.xlsx,<br>all_last_machines.xlsx | Feeder path summary, full DB with feeder info, all last machines |
| Alc_Machines_Iso.py           | alc_DB_FLIS.xlsx,<br>machine_connections_alone.xlsx,<br>machine_connections_iso.xlsx | Main machine DB, direct connections, isolation connections       |
| machine_machines_V3.py        | NOB_TRF_DB_FLISR.xlsx (or similar)              | Final restoration-ready DB                                       |
| name_var.py                   | alc_machine_var.xlsx                            | Machine variable lookup table                                    |
| Extract_var.py                | scr_machine_var.xlsx                            | Screen/project variable table                                    |

**Project Created**: 2025  
**Last Updated**: July 29, 2025  
**Version**: 5.0  
**Author**: FLISR Database Creation Team
