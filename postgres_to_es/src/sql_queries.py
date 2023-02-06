"""Список подготовленных SQL запросов."""

filmwork_fw_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   fw.modified,
   fw.creation_date,
   fw.tag,
   fw.price,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'writer'),
       '[]'
   ) as writers,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'actor'),
       '[]'
   ) as actors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'director'),
       '[]'
   ) as directors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', g.id,
               'name', g.name
           )
       ) FILTER (WHERE g.id IS NOT NULL),
       '[]'
   ) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > %(modified)s
GROUP BY fw.id
ORDER BY fw.modified
"""

filmwork_p_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.creation_date,
   min(p.created) as created,
   max(p.modified) as modified,
   fw.tag,
   fw.price,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'writer'),
       '[]'
   ) as writers,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'actor'),
       '[]'
   ) as actors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'director'),
       '[]'
   ) as directors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', g.id,
               'name', g.name
           )
       ) FILTER (WHERE g.id IS NOT NULL),
       '[]'
   ) as genres
FROM content.person p
LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE p.modified > %(modified)s AND fw.id IS NOT NULL
GROUP BY fw.id
ORDER BY modified
"""

filmwork_g_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.creation_date,
   min(g.created) as created,
   max(g.modified) as modified,
   fw.tag,
   fw.price,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'writer'),
       '[]'
   ) as writers,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'actor'),
       '[]'
   ) as actors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'director'),
       '[]'
   ) as directors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', g.id,
               'name', g.name
           )
       ) FILTER (WHERE g.id IS NOT NULL),
       '[]'
   ) as genres
FROM content.genre g
LEFT JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
LEFT JOIN content.film_work fw ON fw.id = gfw.film_work_id
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
WHERE g.modified > %(modified)s AND fw.id IS NOT NULL
GROUP BY fw.id
ORDER BY modified
"""

genre_query = """
SELECT
    g.id,
    g.name,
    g.description,
    g.created,
    g.modified
FROM content.genre g
WHERE g.modified > %(modified)s AND g.id IS NOT NULL
ORDER BY g.modified
"""

person_query = """
SELECT
    p.id,
    p.full_name as name,
    p.created,
    p.modified,
    array_agg(DISTINCT pfw.role) as role,
    array_agg(DISTINCT pfw.film_work_id) as film_ids
FROM content.person p
LEFT JOIN content.person_film_work pfw on p.id=pfw.person_id
GROUP BY p.id
HAVING p.modified > %(modified)s OR max(pfw.created) > %(modified)s
ORDER BY p.modified
"""
