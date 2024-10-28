from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()  # Carga las variables desde .env al entorno

# Ahora puedes acceder a las variables de entorno
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

def get_products(prompt):
    client = bigquery.Client(project=project_id)
    query = """
    WITH QueryEmbedding AS (
      SELECT
        ml_generate_embedding_result AS query_embedding
      FROM
        ML.GENERATE_EMBEDDING(
          MODEL `dataton-2024-team-01-cofares.datos_cofares.text_embedding`,
          (SELECT @prompt AS content),  -- Aquí usamos el parámetro
          STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
        )
    )
    SELECT
      d.nombre_completo_material AS nombre,
      d.txt_mas_informacion_del_producto AS descripcion,
      d.txt_instrucciones_de_uso AS modo_implementacion,
      d.codigo_web,
      d.URI_primera_imagen,
      d.codigo_nacional,
      ML.DISTANCE(
        qe.query_embedding,
        e.ml_generate_embedding_result,
        'COSINE'
      ) AS distance_to_query
    FROM
      `dataton-2024-team-01-cofares.datos_cofares.data_final_temp` AS d
    INNER JOIN
      `dataton-2024-team-01-cofares.datos_cofares.SalidaEmbeddings_temp` AS e
      ON d.codigo_web = e.title
    INNER JOIN QueryEmbedding AS qe
      ON TRUE
    ORDER BY
      distance_to_query
    LIMIT 5;
    """.format(prompt)
    # Configura el parámetro para el prompt
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("prompt", "STRING", prompt)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    
    products = []
    for row in results:

        descripcion = row.descripcion
        if not row.descripcion:
            descripcion = '-'
        
        modo_implementacion = row.modo_implementacion
        if not row.modo_implementacion:
            modo_implementacion = '-'


        # Cambia la URL si es necesario
        imagen_url = row.URI_primera_imagen 
        if imagen_url and imagen_url.startswith('gs:/'):
            imagen_url = imagen_url.replace('gs://dataton-2024-team-01-cofares-datastore/imagenes/', 'https://storage.googleapis.com/dataton-2024-team-01-cofares-datastore/imagenes/reto_cofares/')
        products.append({
            "codigo_web": row.codigo_web,
            "nombre": row.nombre,
            "codigo_nacional": row.codigo_nacional,
            "descripcion": descripcion,
            "modo_implementacion": modo_implementacion,
            "imagen_url": imagen_url
        })
    return products
