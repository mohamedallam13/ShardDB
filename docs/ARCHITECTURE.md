# ShardDB — Architecture & Flow Reference

## What it is

ShardDB is a shard-aware JSON document database layered over Google Drive. Each "table" (`dbMain`) splits its rows across multiple fragment files on Drive. A single master INDEX file holds all routing metadata so reads never need to scan every shard.

---

## 1. System Overview

```mermaid
flowchart TD
    subgraph Drive["Google Drive (persistent storage)"]
        IDX["📄 Master INDEX\n(one file, always read on init)"]
        F1["📄 Fragment USERS_1\n≤ 1000 rows"]
        F2["📄 Fragment USERS_2\n≤ 1000 rows"]
        F3["📄 Fragment USERS_3\n≤ 1000 rows"]
    end

    subgraph Memory["In-memory (per script execution)"]
        INDEX["INDEX object\nkeyToFragment\nidRangesSorted\nindexRoutingDirty"]
        OPEN["OPEN_DB map\nonly opened fragments\n{ toWrite, isChanged }"]
    end

    subgraph Adapter["ToolkitAdapter (I/O layer)"]
        A1["readFromJSON(fileId)"]
        A2["writeToJSON(fileId, payload)"]
        A3["createJSON(name, folder, payload)"]
        A4["deleteFile(fileId)"]
    end

    IDX -->|init reads once| INDEX
    INDEX -->|route → fragment fileId| Adapter
    Adapter -->|lazy load on first access| OPEN
    OPEN -->|flush on saveToDBFiles| Adapter
    Adapter --> Drive
```

---

## 2. Master INDEX Structure

```mermaid
flowchart LR
    subgraph INDEX["INDEX (in memory + persisted as one Drive JSON)"]
        direction TB
        subgraph USERS["USERS (dbMain)"]
            direction TB
            Props["properties\n──────────\ncumulative: true\nrootFolder: Drive folder ID\nfilesPrefix: 'chk'\nfragmentsList: [USERS_1, USERS_2]\nkeyToFragment: { alice→USERS_1, bob→USERS_2 }\nidRangesSorted: sorted array\nindexRoutingDirty: bool"]
            Frags["dbFragments\n──────────\nUSERS_1: { fileId, idRange{min,max} }\nUSERS_2: { fileId, idRange{min,max} }"]
        end
    end

    KTF["keyToFragment\nO(1) lookup\nkey → fragment name"]
    IRS["idRangesSorted\nO(log F) binary search\n[{fragment, min, max}, ...]"]

    Props --> KTF
    Props --> IRS
```

---

## 3. Fragment File Structure

```mermaid
flowchart LR
    subgraph Frag["Fragment JSON file (Drive)"]
        direction TB
        Idx["index\n──────\nalice → 1\ndave  → 2\n...key→id map"]
        Data["data\n──────\n1 → { id:1, key:'alice', email:... }\n2 → { id:2, key:'dave',  role:... }\n...id→document map"]
    end

    Idx -->|fast key→id within shard| Data
    Note["ignoreIndex=true:\nindex is empty;\nonly data+keyToFragment used"]
```

---

## 4. Lookup by Key

```mermaid
flowchart TD
    A([lookUpByKey key, dbMain]) --> B{keyToFragment\nhas key?}
    B -->|no| Z1([return null])
    B -->|yes → fragment| C{OPEN_DB\nhas fragment?}
    C -->|yes| E
    C -->|no| D[readFromJSON fileId\nfrom Drive\ncache in OPEN_DB]
    D --> E[fragment.toWrite.index\nkey → id]
    E -->|id found| F[fragment.toWrite.data\nid → row]
    F --> G([return row])
    E -->|id missing — ignoreIndex path| H[scan data values\nfor row.key === key]
    H --> G
```

---

## 5. Lookup by ID

```mermaid
flowchart TD
    A([lookUpById id, dbMain]) --> B[normalizeId]
    B --> C[binary search\nidRangesSorted\nO log F]
    C -->|no range contains id| Z1([return null])
    C -->|fragment found| D{OPEN_DB\nhas fragment?}
    D -->|yes| F
    D -->|no| E[readFromJSON fileId\nfrom Drive]
    E --> F[toWrite.data id]
    F -->|found| G([return row])
    F -->|null| Z2([return null])
```

---

## 6. addToDB Flow

```mermaid
flowchart TD
    A([addToDB entry, dbMain]) --> B[normalizeId + normalizeKey]
    B --> C{explicit\ndbFragment?}

    C -->|yes| D[getProperFragment\nuse named fragment]
    C -->|no| E{findFragmentForId\nid already exists?}
    E -->|yes → fragment| F[route to that fragment\nopen if needed]
    E -->|no| G[getProperFragment\nlatest fragment]

    D --> H
    F --> H
    G --> H

    H{cumulative?\ncheckOpenDBSize}
    H -->|fragment at capacity\nnew id| I[createNewCumulativeFragment\nUSERS_N+1\naddInIndexFile]
    H -->|space available\nor existing id| J

    I --> J[open fragment if not in OPEN_DB]

    J --> K{prior row exists\nfor this id?}
    K -->|yes, key changed| L[remove old key\nfrom keyToFragment\n+ fragment index]
    K -->|yes, same key+same routing| M[indexRoutingDirty = false]
    K -->|no prior row| N[indexRoutingDirty = true]

    L --> O
    M --> O
    N --> O

    O[update tw.index key→id\nupdate keyToFragment key→fragment\nupdate tw.data id→row]
    O --> P{id range\nchanged?}
    P -->|yes min or max moved| Q[rebuildIdRangeSorted]
    P -->|no — id within existing range| R
    Q --> R
    R{indexRoutingDirty?}
    R -->|yes| S[markIndexRoutingDirty]
    R -->|no| T
    S --> T[isChanged = true]
    T --> Z([done])
```

---

## 7. saveToDBFiles Flow

```mermaid
flowchart TD
    A([saveToDBFiles]) --> B[for each entry in OPEN_DB]
    B --> C{isChanged?}
    C -->|no| B
    C -->|yes| D{fileId empty?\nnew file}
    D -->|yes — first save| E[createJSON on Drive\nstore returned fileId\nmarkIndexRoutingDirty]
    D -->|no — existing file| F[writeToJSON fileId, toWrite\nDrive write]
    E --> G[isChanged = false]
    F --> G
    G --> B
    B -->|all fragments done| H{any dbMain\nindexRoutingDirty?}
    H -->|no — pure payload update\nsame id+key+fragment| I([done — INDEX skipped])
    H -->|yes — routing changed| J[writeToJSON indexFileId, INDEX\nDrive write]
    J --> K[clearIndexRoutingDirty]
    K --> L([done])
```

---

## 8. Fragment Lifecycle

```mermaid
stateDiagram-v2
    [*] --> OnDrive : createJSON (first saveToDBFiles)

    OnDrive --> Opened : openDBFragment\nreadFromJSON → OPEN_DB

    Opened --> Dirty : addToDB / deleteFromDB\nisChanged = true

    Dirty --> Opened : saveToDBFiles\nwriteToJSON → Drive\nisChanged = false

    Opened --> Closed : closeDB\ndelete from OPEN_DB

    Closed --> Opened : next read/write\nopenDBFragment

    Opened --> Cleared : clearDB\nwipe data+index+routing\nwriteToJSON empty

    Cleared --> Opened : addToDB\nfragment re-used

    Opened --> Destroyed : destroyDB\ndeleteFile on Drive\nremove from INDEX

    OnDrive --> Destroyed : destroyDB\n(never opened)

    Destroyed --> [*]
```

---

## 9. indexRoutingDirty Decision

This flag is the key to avoiding redundant INDEX writes. The INDEX is only re-written to Drive when routing metadata actually changed.

```mermaid
flowchart LR
    subgraph Marks_dirty["Sets indexRoutingDirty = true"]
        N1["New row insert\n(new key+id)"]
        N2["Key change on update\n(old key evicted)"]
        N3["Fragment created\n(new fileId)"]
        N4["Fragment destroyed"]
        N5["clearDB / destroyDB"]
        N6["addExternalConfig"]
    end

    subgraph Stays_clean["indexRoutingDirty stays false"]
        C1["In-place payload update\nsame id + same key\nalready mapped to same fragment"]
    end

    subgraph Result["saveToDBFiles outcome"]
        W1["✅ Fragment write\n+ INDEX write"]
        W2["✅ Fragment write only\nINDEX skipped"]
    end

    Marks_dirty --> W1
    Stays_clean --> W2
```

---

## 10. Adapter Layer

```mermaid
flowchart TD
    subgraph Adapters
        DA["createDriveToolkitAdapter\nDriveApp only\n(no extra dependencies)"]
        CA["Custom ToolkitAdapter\nAtlasToolkit / Advanced Drive\nor test mock"]
        WR["wrapWithBackupRestore\nwraps any adapter\nauto backup on write\nauto restore on read failure"]
    end

    DA --> Core["SHARD_DB.init\nindexFileId, adapter"]
    CA --> Core
    WR --> Core

    Core --> Ops["addToDB\nlookUpById\nlookUpByKey\nlookupByCriteria\ndeleteFromDBById\ndeleteFromDBByKey\nsaveToDBFiles\nsaveIndex\ncloseDB\nclearDB\ndestroyDB"]
```

---

## Performance Characteristics

| Operation | Complexity | Notes |
|---|---|---|
| `lookUpByKey` | O(1) | `keyToFragment` map lookup → fragment open if needed |
| `lookUpById` | O(log F) | Binary search on `idRangesSorted`, F = fragment count |
| `addToDB` new row | O(log F) | Route + range check; `rebuildIdRangeSorted` only if range changed |
| `addToDB` in-place update | O(1) | Same id in range → no rebuild, no INDEX write |
| `deleteFromDB` | O(1) | `prior.key` direct path, no index scan |
| `lookupByCriteria` by id | O(log F) | Id fast-path, single fragment open |
| `lookupByCriteria` by other field | O(N) | Full scan across all opened fragments |
| `saveToDBFiles` routing unchanged | O(dirty fragments) | INDEX write skipped |
| `saveToDBFiles` routing changed | O(dirty fragments + 1) | +1 for INDEX write |
| Drive I/O per fragment | ~200–600ms | Dominant cost; all else is negligible by comparison |

F = number of fragments = ⌈total rows / MAX_ENTRIES_COUNT⌉ (MAX_ENTRIES_COUNT = 1000 by default)
