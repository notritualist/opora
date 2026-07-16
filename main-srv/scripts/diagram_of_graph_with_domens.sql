SELECT line
FROM (
    -- 0. Заголовок
    SELECT 0 AS priority, 'digraph G { rankdir=TB; node [shape=box, style=filled];' AS line

    UNION ALL

    -- 1. Домены (вершины)
    SELECT 1 AS priority,
           format(
             '  "%s" [label="%s", fillcolor=%s, shape=ellipse, style=filled];',
             id,
             regexp_replace(name, '"', '\\"', 'g'),
             '"#e6e6fa"'   -- цвет передан без лишних кавычек
           )
    FROM memory.knowledge_domains
    WHERE is_active = true

    UNION ALL

    -- 2. Узлы (entity выделены отдельным цветом и формой)
    SELECT 2 AS priority,
           format(
             '  "%s" [label="%s", fillcolor=%s%s%s];',
             id,
             regexp_replace(left(description, 60), '"', '\\"', 'g'),
             CASE form_code
               WHEN 'fact'   THEN '"#a6cee3"'
               WHEN 'task'   THEN '"#fb9a99"'
               WHEN 'goal'   THEN '"#b2df8a"'
               WHEN 'entity' THEN '"#ffff99"'      -- ярко-жёлтый для entity
               ELSE '"#fdbf6f"'
             END,
             CASE WHEN form_code = 'entity' THEN ', shape=component' ELSE '' END,
             CASE WHEN form_code = 'entity' THEN ', color=black' ELSE '' END
           )
    FROM memory.graph_nodes
    WHERE is_active = true

    UNION ALL

    -- 3. Рёбра между узлами (без изменений)
    SELECT 3 AS priority,
           format(
             '  "%s" -> "%s" [label="%s"];',
             source_node_id,
             target_node_id,
             relation_type
           )
    FROM memory.graph_edges
    WHERE is_active = true

    UNION ALL

    -- 4. Рёбра домен → узел (разный стиль для entity и остальных)
    SELECT 4 AS priority,
           format(
             '  "%s" -> "%s" [label="%s", style=%s, color=%s];',
             domain_id,
             id,
             CASE WHEN form_code = 'entity' THEN 'contains' ELSE 'belongs_to' END,
             CASE WHEN form_code = 'entity' THEN 'solid' ELSE 'dashed' END,
             CASE WHEN form_code = 'entity' THEN 'black' ELSE 'gray' END
           )
    FROM memory.graph_nodes
    WHERE is_active = true
      AND domain_id IS NOT NULL

    UNION ALL

    -- 5. Закрывающая скобка
    SELECT 5 AS priority, '}' AS line
) AS dot
