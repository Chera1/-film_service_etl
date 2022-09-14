"""Список подготовленных SQL запросов."""

filmwork_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   fw.modified,
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
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > (%s)
GROUP BY fw.id
ORDER BY fw.modified
"""

person_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   min(p.created) as created,
   max(p.modified) as modified,
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
   array_agg(DISTINCT g.name) as genres
FROM content.person p
LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE p.modified > (%s) AND fw.id IS NOT NULL
GROUP BY fw.id
ORDER BY modified
"""

genre_query = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   min(g.created) as created,
   max(g.modified) as modified,
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
   array_agg(DISTINCT g.name) as genres
FROM content.genre g
LEFT JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
LEFT JOIN content.film_work fw ON fw.id = gfw.film_work_id
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
WHERE g.modified > (%s) AND fw.id IS NOT NULL
GROUP BY fw.id
ORDER BY modified
"""
