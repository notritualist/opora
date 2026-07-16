SELECT 'digraph G { rankdir=TB; node [shape=box, style=filled];'
UNION ALL
SELECT format(
  '  "%s" [label="%s", fillcolor=%s];',
  id,
  regexp_replace(left(description, 60), '"', '\\"', 'g'),  -- экранируем кавычки
  CASE form_code
    WHEN 'fact'   THEN '"#a6cee3"'
    WHEN 'task'   THEN '"#fb9a99"'
    WHEN 'goal'   THEN '"#b2df8a"'
    ELSE '"#fdbf6f"'
  END
)
FROM memory.graph_nodes
WHERE is_active = true
UNION ALL
SELECT format(
  '  "%s" -> "%s" [label="%s"];',
  source_node_id,
  target_node_id,
  relation_type
)
FROM memory.graph_edges
WHERE is_active = true
--  AND relation_type = 'refines'   -- раскомментируй, если нужна только иерархия
UNION ALL
SELECT '}';
