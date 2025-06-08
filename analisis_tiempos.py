import os
import json
from statistics import mean, stdev
from collections import defaultdict
from datetime import datetime
import matplotlib.pyplot as plt

# Configura la ruta absoluta a la carpeta output
CARPETA_BASE_SALIDA = r"C:\Users\Yeray\Desktop\Bioinformática Máster\TFM\scripts\output"

def calcular_media_y_error(lista):
    if len(lista) < 2:
        return round(mean(lista), 5), 0.0
    return round(mean(lista), 5), round(stdev(lista) / (len(lista) ** 0.5), 5)

def generar_grafica(tiempos, carpeta_destino):
    for coleccion, valores in tiempos.items():
        if coleccion == "_tiempo_total_global":
            continue

        etiquetas = []
        medias = []
        errores = []

        for tipo, datos in valores.items():
            if tipo.startswith("tiempo_"):
                etiquetas.append(tipo.replace("tiempo_", ""))
                medias.append(datos["media"])
                errores.append(datos["error"])

        plt.figure(figsize=(8, 5))
        plt.bar(etiquetas, medias, yerr=errores, capsize=5, color='skyblue')
        plt.title(f"Tiempos medios - {coleccion}")
        plt.ylabel("Segundos")
        plt.xlabel("Etapa")
        plt.tight_layout()
        plt.savefig(os.path.join(carpeta_destino, f"{coleccion}_grafico_tiempos.png"))
        plt.close()

def main():
    datos_acumulados = defaultdict(lambda: {
        "tiempo_consulta": [],
        "tiempo_polars": [],
        "tiempo_total": []
    })

    # Parquets por separado
    tiempos_parquet_largo = []
    tiempos_parquet_corto = []

    tiempos_globales = []

    for entrada in os.listdir(CARPETA_BASE_SALIDA):
        ruta_carpeta = os.path.join(CARPETA_BASE_SALIDA, entrada)
        if os.path.isdir(ruta_carpeta):
            ruta_txt = os.path.join(ruta_carpeta, "informe_tiempos.txt")
            if os.path.isfile(ruta_txt):
                try:
                    with open(ruta_txt, "r", encoding="utf-8") as f:
                        lineas = f.readlines()

                    coleccion_actual = None
                    for linea in lineas:
                        linea = linea.strip()

                        if linea.startswith("🗂️"):
                            coleccion_actual = linea.split("🗂️")[1].strip()
                            continue

                        if coleccion_actual and "MongoDB" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("s", "").strip())
                            datos_acumulados[coleccion_actual]["tiempo_consulta"].append(tiempo)

                        elif coleccion_actual and "Conversión Polars" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("s", "").strip())
                            datos_acumulados[coleccion_actual]["tiempo_polars"].append(tiempo)

                        elif coleccion_actual and "Tiempo total" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("s", "").strip())
                            datos_acumulados[coleccion_actual]["tiempo_total"].append(tiempo)

                        elif "Parquet largo" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("s", "").strip())
                            tiempos_parquet_largo.append(tiempo)

                        elif "Parquet corto" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("s", "").strip())
                            tiempos_parquet_corto.append(tiempo)

                        elif "Tiempo TOTAL de ejecución" in linea:
                            tiempo = float(linea.split(":")[1].strip().replace("segundos", "").strip())
                            tiempos_globales.append(tiempo)

                except Exception as e:
                    print(f"[!] Error leyendo {ruta_txt}: {e}")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    carpeta_informe = os.path.join(CARPETA_BASE_SALIDA, f"analisis_{timestamp}")
    os.makedirs(carpeta_informe, exist_ok=True)

    informe_txt = os.path.join(carpeta_informe, "informe_estadistico_tiempos.txt")
    informe_json = os.path.join(carpeta_informe, "informe_estadistico_tiempos.json")

    resultado_json = {}

    with open(informe_txt, "w", encoding="utf-8") as f:
        f.write("============== INFORME DE TIEMPOS EJECUCIÓN (ESTADÍSTICOS) ==============\n\n")

        for coleccion, datos in datos_acumulados.items():
            f.write(f"[✔] Colección: {coleccion}\n")

            datos_coleccion = {}
            for tipo in ["tiempo_consulta", "tiempo_polars", "tiempo_total"]:
                lista_tiempos = datos[tipo]
                if lista_tiempos:
                    media, error = calcular_media_y_error(lista_tiempos)
                    f.write(f"→ {tipo.replace('_', ' ')}: {media} ± {error} segundos\n")
                    datos_coleccion[tipo] = {"media": media, "error": error}

            f.write("\n-----------------------------------------------------------\n\n")
            resultado_json[coleccion] = datos_coleccion

        if tiempos_parquet_largo:
            media, error = calcular_media_y_error(tiempos_parquet_largo)
            f.write("[✔] Tiempo generación Parquet largo\n")
            f.write(f"→ parquet largo: {media} ± {error} segundos\n\n")
            resultado_json["parquet_largo"] = {"tiempo_parquet": {"media": media, "error": error}}

        if tiempos_parquet_corto:
            media, error = calcular_media_y_error(tiempos_parquet_corto)
            f.write("[✔] Tiempo generación Parquet corto\n")
            f.write(f"→ parquet corto: {media} ± {error} segundos\n\n")
            resultado_json["parquet_corto"] = {"tiempo_parquet": {"media": media, "error": error}}

        if tiempos_globales:
            media_global, error_global = calcular_media_y_error(tiempos_globales)
            f.write("[✔] Tiempo total global\n\n")
            f.write(f"→ tiempo total: {media_global} ± {error_global} segundos\n")
            f.write("\n-----------------------------------------------------------\n\n")
            resultado_json["_tiempo_total_global"] = {
                "tiempo_total": {"media": media_global, "error": error_global}
            }

    with open(informe_json, "w", encoding="utf-8") as f:
        json.dump(resultado_json, f, indent=4)

    generar_grafica(resultado_json, carpeta_informe)

    print(f"\n✅ Informe estadístico generado en:\n{informe_txt}\n{informe_json}")
    print(f"✅ Gráficas guardadas en: {carpeta_informe}")

if __name__ == "__main__":
    main()
