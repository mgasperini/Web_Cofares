from google.cloud import bigquery

def get_products(prompt):
    client = bigquery.Client()
    query = """
        SELECT codigo_web, informacion_producto->>'$.nombre' AS nombre,
               informacion_producto->>'$.descripcion' AS descripcion,
               informacion_producto->>'$.modo_implementacion' AS modo_implementacion,
               informacion_producto->>'$.imagen_url' AS imagen_url
        FROM `tu_proyecto.parafarmacia_data.productos`
        WHERE informacion_producto LIKE '%{}%'
        LIMIT 5
    """.format(prompt)
    query_job = client.query(query)
    results = query_job.result()

    products = []
    for row in results:
        products.append({
            "codigo_web": row.codigo_web,
            "nombre": row.nombre,
            "descripcion": row.descripcion,
            "modo_implementacion": row.modo_implementacion,
            "imagen_url": row.imagen_url
        })
    return products
