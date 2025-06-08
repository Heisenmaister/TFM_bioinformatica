import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from pymongo import MongoClient
import polars as pl
from datetime import datetime

# === Config inicial ===
# Diccionario con filtros específicos para cada colección de MongoDB.
# Se usan para filtrar los documentos que se extraen de cada colección.
FILTROS = {
    "samples": {"patol_prin": "Hipertrofica"},
    "effects": {"IMPACT": {"$in": ["MODERATE", "HIGH"]}, "MAX_AF": {"$lte": 0.05}},
    "samples_variants": {"cov": {"$gte": 10}, "qual": {"$gte": 20}},
    "variants": {}
}

# === Mongo config ===
def get_client():
    # Crea y devuelve un cliente MongoDB para conectarse a la base local
    # Timeout de servidor configurado a 5000 ms para evitar esperas largas
    return MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)

# === Consulta y creación de Polars DataFrame en memoria ===
def consulta_polars(nombre_col, filtro, queue):
    """
    Ejecuta una consulta en MongoDB con un filtro dado para la colección nombre_col.
    Convierte los documentos obtenidos a un DataFrame de Polars.
    Envía el resultado y tiempos a una cola para su posterior procesamiento.

    Parámetros:
    - nombre_col: nombre de la colección en MongoDB
    - filtro: diccionario con condiciones para la consulta
    - queue: objeto Queue para enviar resultados entre hilos
    """
    logging.info(f"▶️ Iniciando consulta: {nombre_col}")
    client = get_client()           # Crear cliente Mongo
    db = client.MIRNAS              # Seleccionar base de datos MIRNAS

    t0 = time.time()                # Tiempo inicio consulta
    cursor = db[nombre_col].find(filtro, {"_id": 0})  # Ejecutar consulta sin incluir campo _id
    docs = list(cursor)             # Cargar resultados en lista
    t1 = time.time()                # Tiempo fin consulta

    # Limpieza de datos: reemplazar cadenas vacías por None para mejor manejo en Polars
    for doc in docs:
        for k, v in doc.items():
            if v == "":
                doc[k] = None

    # Crear DataFrame Polars con los datos limpios
    df = pl.DataFrame(docs)
    t2 = time.time()                # Tiempo fin creación DataFrame

    # Calcular tiempos parciales y totales
    tiempo_mongo = round(t1 - t0, 3)
    tiempo_polars = round(t2 - t1, 3)
    total = round(t2 - t0, 3)

    logging.info(f"✅ {nombre_col:<18} → {len(df)} registros en {total} s (Mongo: {tiempo_mongo}s | Polars: {tiempo_polars}s)")

    # Enviar resultados a la cola para recogerlos desde el hilo principal
    queue.put((nombre_col, df, {
        "registros": len(df),
        "tiempo_total": total,
        "tiempo_mongo": tiempo_mongo,
        "tiempo_polars": tiempo_polars
    }))

# === Unión y creación de Parquets en memoria ===
def generar_parquets(dfs, carpeta_salida):
    """
    Realiza joins entre los DataFrames cargados desde MongoDB para crear dos formatos distintos.
    Luego escribe ambos DataFrames resultantes en formato parquet en la carpeta especificada.

    Parámetros:
    - dfs: diccionario con DataFrames Polars indexados por nombre de colección
    - carpeta_salida: ruta donde guardar los archivos parquet

    Retorna:
    - tiempos_parquets: diccionario con tiempos de escritura para cada parquet
    """
    tiempos_parquets = {}

    # Extraer DataFrames individuales para comodidad
    df_samples = dfs["samples"]
    df_effects = dfs["effects"]
    df_sv = dfs["samples_variants"]
    df_variants = dfs["variants"].with_columns([
        # Asegurar que ciertas columnas tienen el tipo correcto para evitar errores en joins
        pl.col("chr").cast(pl.Utf8),
        pl.col("ref").cast(pl.Utf8),
        pl.col("alt").cast(pl.Utf8),
        pl.col("pos_start").cast(pl.Int64),
        pl.col("pos_end").cast(pl.Int64)
    ])

    # === Creación del DataFrame largo mediante joins encadenados ===
    df1 = df_samples.join(df_sv, left_on="id", right_on="sample", how="inner")
    df2 = df1.join(df_effects, left_on="variant", right_on="id", how="inner")
    df_final = df2.join(df_variants, left_on="variant", right_on="id", how="inner")

    # Guardar DataFrame largo como parquet y medir tiempo
    t_start = time.time()
    ruta_largo = os.path.join(carpeta_salida, "parquet_largo.parquet")
    df_final.write_parquet(ruta_largo)
    t_end = time.time()
    tiempos_parquets["parquet_largo"] = round(t_end - t_start, 3)

    # === Creación del DataFrame corto con formato pivot (genotipos por muestra) ===
    muestras = df_samples.select(["id", "name"])                # Seleccionar columnas necesarias
    variantes = df_variants.select(["id", "key"]).rename({"id": "variant_id"})  # Renombrar para join
    muestras_variantes = muestras.join(variantes, how="cross")   # Producto cartesiano muestras x variantes

    sv = df_sv.select(["sample", "variant", "homo"])             # Datos de variantes por muestra
    # Join para agregar información homo (homocigoto) si existe, sino null
    df_full = muestras_variantes.join(sv, left_on=["id", "variant_id"], right_on=["sample", "variant"], how="left")

    # Definir columna genotipo con reglas condicionales:
    # null -> "0/0", homo=True -> "1/1", homo=False -> "0/1"
    df_full = df_full.with_columns([
        pl.when(pl.col("homo").is_null()).then(pl.lit("0/0"))
          .when(pl.col("homo") == True).then(pl.lit("1/1"))
          .otherwise(pl.lit("0/1")).alias("genotype")
    ])

    # Pivot para que cada fila sea una variante (key) y las columnas sean los nombres de muestra
    df_corto = df_full.select(["key", "name", "genotype"]).pivot(
        values="genotype", index="key", on="name"
    )

    # Guardar DataFrame corto como parquet y medir tiempo
    t_start = time.time()
    ruta_corto = os.path.join(carpeta_salida, "parquet_corto.parquet")
    df_corto.write_parquet(ruta_corto)
    t_end = time.time()
    tiempos_parquets["parquet_corto"] = round(t_end - t_start, 3)
    return tiempos_parquets

# === Guardar informe con tiempos y registros ===
def guardar_informe_txt(mediciones, tiempos_parquets, total_time, carpeta_salida):
    """
    Genera un archivo de texto con el resumen de tiempos de consulta, conversión y escritura.

    Parámetros:
    - mediciones: dict con métricas (registros y tiempos) por colección
    - tiempos_parquets: dict con tiempos de escritura para parquets largo y corto
    - total_time: tiempo total de ejecución antes de la generación de parquets
    - carpeta_salida: ruta donde guardar el informe
    """
    path = os.path.join(carpeta_salida, "informe_tiempos.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("== MÉTRICAS DE TIEMPO EN EJECUCIÓN PARALELA ==\n\n")
        for nombre, info in mediciones.items():
            f.write(f"🗂️  {nombre}\n")
            f.write(f"    📊 Registros         : {info['registros']}\n")
            f.write(f"    ⏱️ MongoDB            : {info['tiempo_mongo']} s\n")
            f.write(f"    🧪 Conversión Polars   : {info['tiempo_polars']} s\n")
            f.write(f"    ⏲️ Tiempo total       : {info['tiempo_total']} s\n\n")

        f.write("== TIEMPOS GENERACIÓN PARQUETS ==\n\n")
        f.write(f"    🟦 Parquet largo : {tiempos_parquets['parquet_largo']} s\n")
        f.write(f"    🟩 Parquet corto : {tiempos_parquets['parquet_corto']} s\n\n")

        f.write(f"⏱️ Tiempo TOTAL de ejecución: {total_time} segundos\n")


# === Función para integrar en API ===
def ejecutar_consulta_y_generar_parquets(filtros_dict, carpeta_salida):
    """
    Función que ejecuta las consultas a MongoDB en paralelo y genera los archivos parquet.
    Pensada para ser llamada desde una API o interfaz externa.

    Parámetros:
    - filtros_dict: diccionario de filtros (igual estructura que FILTROS)
    - carpeta_salida: ruta para guardar los archivos parquet

    Retorna:
    - dfs: diccionario con DataFrames obtenidos
    - tiempos_parquets: tiempos de escritura de archivos parquet
    - mediciones: métricas de consulta y creación de DataFrames
    """
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue

    queue = Queue()
    dfs = {}
    mediciones = {}

    # Ejecutar consultas en paralelo con un pool de hilos (max 4)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(consulta_polars, col, filtro, queue) for col, filtro in filtros_dict.items()]
        # Recoger resultados de la cola en el orden que terminen
        for _ in futures:
            nombre, df, info = queue.get()
            dfs[nombre] = df
            mediciones[nombre] = info

    # Generar archivos parquet a partir de los DataFrames obtenidos
    tiempos_parquets = generar_parquets(dfs, carpeta_salida)

    return dfs, tiempos_parquets, mediciones


# === MAIN ===
def main():
    """
    Función principal que se ejecuta al iniciar el script.
    Configura logging, crea carpeta de salida con timestamp,
    ejecuta consultas en paralelo, genera parquets y guarda informe.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Crear carpeta de salida con nombre basado en fecha y hora actuales
    carpeta_salida = os.path.join(os.path.dirname(__file__), "output", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(carpeta_salida, exist_ok=True)

    inicio_total = time.time()  # Tiempo inicio global

    queue = Queue()
    dfs = {}
    mediciones = {}

    # Ejecutar consultas a MongoDB en paralelo usando ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(consulta_polars, col, filtro, queue) for col, filtro in FILTROS.items()]
        # Recoger resultados a medida que terminan
        for _ in futures:
            nombre, df, info = queue.get()
            dfs[nombre] = df
            mediciones[nombre] = info

    # Medir tiempo para generación de archivos parquet
    generar_parquets_start = time.time()
    tiempos_parquets = generar_parquets(dfs, carpeta_salida)
    generar_parquets_end = time.time()

    # Calcular tiempo total de ejecución hasta generación parquet (excluye tiempo de escritura parquet e informe)
    total = round(generar_parquets_start - inicio_total, 3)

    # Guardar informe con métricas y tiempos
    guardar_informe_txt(mediciones, tiempos_parquets, total, carpeta_salida)

    logging.info(f"⏱️  Tiempo total ejecución: {total} segundos")
    logging.info(f"⏱️  Informe y archivos Parquet generados en: {carpeta_salida}")

# Ejecutar main si el script se llama directamente
if __name__ == "__main__":
    main()

