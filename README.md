Repositorio creado para depositar los scripts y ficheros concernientes al trabajo de fin de máster.

Diseño e implementación de un sistema automático de
filtrado de variantes usando tecnologías Big Data.

Tabla de contenido:

# carga_parquets_ENF.R : Script de consulta vía API y carga de los ficheros parquets para el caso de validación I.

# carga_parquets_MUESTRAS.R : Script de consulta vía API y carga de los ficheros parquets para el caso de validación II.

# fisher_enfermedades.R : Script para el estudio estadísitco de la valdiación I y generación de las gráficas asociadas.

# heatmap_genes_muestra.R : Script para la generación del mapa de calor en la validación II.

# parquets_from_mongo_paralelo.py : Script principal de trabajo de la herramienta. Realiza desde la consulta a MongoDB hasta la generación de los archivos parquet en paralelo.

# parquets_from_mongo_secuencial.py : Script principal de trabajo de la herramienta. Realiza desde la consulta a MongoDB hasta la generación de los archivos parquet en secuencial (no se introdujo en el flujo de trabajo, se utilizó para comparar la eficiencia del paralelismo).

# script_api.py : Script de la API REST.

# parquet_corto/largo_AF_brugada/SADS.parquet : Parquets generados en la consulta de validación de presencia de variantes en distintas enfermedades.

# parquet_corto/largo_MU.parquet : Parquets generados en la consulta de validación de variantes por gen-muestra.
