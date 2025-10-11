# Hierarchy Table Schema

The `Hierarchy` table is the central navigational backbone for the entire canon structure. It stores every node—from the top-level pitakas down to individual suttas (leaves)—and defines their structural relationships, sequence, and depth. This denormalized table is optimized for fast and flexible navigation queries.

---

## Table Structure

**Table Name:** `Hierarchy`

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `uid` | `TEXT` | **Primary Key.** The unique identifier for the node (e.g., `sutta`, `dn`, `dn1`). |
| `parent_uid` | `TEXT` | The `uid` of the node's direct parent. Used for building breadcrumbs upwards. `NULL` for the absolute top-level nodes. |
| `type` | `TEXT` | The classification of the node. Can be `root`, `branch`, or `leaf`. |
| `pitaka_root`| `TEXT` | The ultimate root of the pitaka this node belongs to (e.g., `sutta`, `vinaya`, `abhidhamma`). |
| `book_root` | `TEXT` | The root of the "book" this node belongs to (e.g., `dn`, `mn`). For nodes defined in `super-tree.json`, this is set to `buddha`. |
| `pitaka_depth` | `INTEGER` | The **absolute depth** of the node, calculated from the pitaka root (where `sutta`, `vinaya` are at depth 0). |
| `book_depth` | `INTEGER` | The **relative depth** of the node within its book, calculated from the `book_root` (where `dn`, `mn` are at depth 0). Set to `-1` for nodes not belonging to a specific book (i.e., super-tree nodes). |
| `sibling_position`| `INTEGER` | The 0-based index of the node relative to its siblings under the same `parent_uid`. |
| `depth_position` | `INTEGER` | The 0-based index of the node relative to all other nodes that share the same `book_root` and `pitaka_depth`. Useful for paginating content at a specific level within a book. |
| `global_position`| `INTEGER` | The absolute 0-based index of the node in the final, globally ordered, depth-first traversal list. Useful for absolute sorting. |
| `prev_uid` | `TEXT` | The `uid` of the previous node at the same `book_root` and `pitaka_depth`. `NULL` if it is the first node in its group. |
| `next_uid` | `TEXT` | The `uid` of the next node at the same `book_root` and `pitaka_depth`. `NULL` if it is the last node in its group. |

---

## Example Queries

### Get Breadcrumbs to the Book Root

This query retrieves the ancestry of a node up to its `book_root`.

```sql
WITH RECURSIVE
  ancestry_to_book_root(uid, parent_uid, book_root, pitaka_depth) AS (
    SELECT uid, parent_uid, book_root, pitaka_depth
    FROM Hierarchy
    WHERE uid = 'dn1'
    UNION ALL
    SELECT h.uid, h.parent_uid, h.book_root, h.pitaka_depth
    FROM Hierarchy h JOIN ancestry_to_book_root a ON h.uid = a.parent_uid
    WHERE a.uid != a.book_root
  )
SELECT uid FROM ancestry_to_book_root ORDER BY pitaka_depth ASC;
```

To query the ancestry of a node up to its `pitaka_root`:

```sql
WITH RECURSIVE
  full_ancestry(uid, parent_uid, pitaka_depth) AS (

    -- Điểm khởi đầu: Lấy node bạn muốn tra cứu
    SELECT
      uid,
      parent_uid,
      pitaka_depth
    FROM Hierarchy
    WHERE uid = :start_uid -- Thay :start_uid bằng uid bạn muốn

    UNION ALL

    -- Phần đệ quy: Tìm cha của node vừa tìm được (không có điều kiện dừng)
    SELECT
      h.uid,
      h.parent_uid,
      h.pitaka_depth
    FROM Hierarchy h
    JOIN full_ancestry a ON h.uid = a.parent_uid
  )
-- Lấy kết quả và sắp xếp
SELECT
  uid
FROM full_ancestry
ORDER BY pitaka_depth ASC;
```

### Get the First 50 Leaves of a Book

This query uses depth_position to efficiently paginate through leaf nodes within a specific book.

```sql
SELECT uid FROM Hierarchy
WHERE
    book_root = 'dn'
    AND type = 'leaf'
ORDER BY
    depth_position ASC
LIMIT 50;
```