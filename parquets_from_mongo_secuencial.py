import os
import time
import logging
from pymongo import MongoClient
import polars as pl
from datetime import datetime

# === Config inicial ===
FILTROS = {
    "samples": {"patol_prin": "Hipertrofica"},
    "effects": {"IMPACT": {"$in": ["MODERATE", "HIGH"]}, "MAX_AF": {"$lte": 0.05}},
    "samples_variants": {"cov": {"$gte": 10}, "qual": {"$gte": 20}},
    "variants": {}
}

# === Mongo config ===
def get_client():
    return MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)

# === Consulta y creación de Polars DataFrame en memoria ===
def consulta_polars(nombre_col, filtro):
    logging.info(f"▶️ Iniciando consulta: {nombre_col}")
    client = get_client()
    db = client.MIRNAS

    # Medir tiempo de consulta Mongo
    inicio_consulta = time.time()
    cursor = db[nombre_col].find(filtro, {"_id": 0})  # Proyección sin _id
    docs = list(cursor)
    fin_consulta = time.time()

    # Limpiar datos vacíos
    for doc in docs:
        for k, v in doc.items():
            if v == "":
                doc[k] = None

    # Medir tiempo de creación DataFrame Polars
    inicio_polars = time.time()
    df = pl.DataFrame(docs)
    fin_polars = time.time()

    logging.info(f"✅ {nombre_col:<18} → {len(df)} registros | Consulta: {round(fin_consulta - inicio_consulta, 3)} s | Polars: {round(fin_polars - inicio_polars, 3)} s")
    return df, fin_consulta - inicio_consulta, fin_polars - inicio_polars

# === Unión y creación de Parquets en memoria ===
def generar_parquets(dfs, carpeta_salida):
    tiempos_parquets = {}
    df_samples = dfs["samples"]
    df_effects = dfs["effects"]
    df_sv = dfs["samples_variants"]
    df_variants = dfs["variants"].with_columns([
        pl.col("chr").cast(pl.Utf8),
        pl.col("ref").cast(pl.Utf8),
        pl.col("alt").cast(pl.Utf8),
        pl.col("pos_start").cast(pl.Int64),
        pl.col("pos_end").cast(pl.Int64)
    ])

    # JOINs largos y guardar parquet largo
    inicio_largo = time.time()
    df1 = df_samples.join(df_sv, left_on="id", right_on="sample", how="inner")
    df2 = df1.join(df_effects, left_on="variant", right_on="id", how="inner")
    df_final = df2.join(df_variants, left_on="variant", right_on="id", how="inner")
    ruta_largo = os.path.join(carpeta_salida, "parquet_largo.parquet")
    df_final.write_parquet(ruta_largo)
    fin_largo = time.time()
    tiempos_parquets["parquet_largo"] = fin_largo - inicio_largo

    # Formato corto (genotipo pivot) y guardar parquet corto
    inicio_corto = time.time()
    muestras = df_samples.select(["id", "name"])
    variantes = df_variants.select(["id", "key"]).rename({"id": "variant_id"})
    muestras_variantes = muestras.join(variantes, how="cross")

    sv = df_sv.select(["sample", "variant", "homo"])
    df_full = muestras_variantes.join(sv, left_on=["id", "variant_id"], right_on=["sample", "variant"], how="left")

    df_full = df_full.with_columns([
        pl.when(pl.col("homo").is_null()).then(pl.lit("0/0"))
          .when(pl.col("homo") == True).then(pl.lit("1/1"))
          .otherwise(pl.lit("0/1")).alias("genotype")
    ])

    df_corto = df_full.select(["key", "name", "genotype"]).pivot(
        values="genotype", index="key", on="name"
    )

    ruta_corto = os.path.join(carpeta_salida, "parquet_corto.parquet")
    df_corto.write_parquet(ruta_corto)
    fin_corto = time.time()
    tiempos_parquets["parquet_corto"] = fin_corto - inicio_corto

    print(f"🟢 Parquet largo y corto generados en {carpeta_salida}")
    return tiempos_parquets

# === MAIN ===
def main():
    # Configura logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    carpeta_salida = os.path.join(os.path.dirname(__file__), "output", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(carpeta_salida, exist_ok=True)

    inicio_total = time.time()

    dfs = {}
    tiempos_consulta = {}
    tiempos_polars = {}

    # Ejecutar consultas + DataFrames en serie, sin paralelismo
    for col, filtro in FILTROS.items():
        df, t_consulta, t_polars = consulta_polars(col, filtro)
        dfs[col] = df
        tiempos_consulta[col] = t_consulta
        tiempos_polars[col] = t_polars

    # Generar parquets y obtener tiempos de escritura
    tiempos_parquets = generar_parquets(dfs, carpeta_salida)

    total = round(time.time() - inicio_total, 3)

    # Guardar informe de tiempos en archivo txt
    informe_path = os.path.join(carpeta_salida, "informe_tiempos.txt")
    with open(informe_path, "w") as f:
        f.write(f"Informe de tiempos - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for col in FILTROS.keys():
            f.write(f"{col}:\n")
            f.write(f"  Tiempo consulta MongoDB: {tiempos_consulta[col]:.4f} s\n")
            f.write(f"  Tiempo creación DataFrame Polars: {tiempos_polars[col]:.4f} s\n\n")
        f.write(f"Tiempo escritura parquet largo: {tiempos_parquets['parquet_largo']:.4f} s\n")
        f.write(f"Tiempo escritura parquet corto: {tiempos_parquets['parquet_corto']:.4f} s\n\n")
        f.write(f"Tiempo total ejecución (excluyendo generación del informe): {total:.4f} s\n")

    logging.info(f"⏱️ Tiempo total ejecución: {total} segundos")
    logging.info(f"📝 Informe de tiempos guardado en {informe_path}")

if __name__ == "__main__":
    main()
