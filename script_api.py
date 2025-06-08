from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import tempfile
import os
import uuid
import shutil
import zipfile
import io

# Importa la función principal que realiza la consulta a MongoDB y genera los parquet
from parquets_from_mongo_paralelo import ejecutar_consulta_y_generar_parquets

# Instancia la aplicación FastAPI
app = FastAPI()

# Endpoint raíz para comprobar que la API está funcionando
@app.get("/")
def root():
    return {
        "message": "Bienvenid@ a la API para generar parquets. Visita URL_actual/docs para usarla vía interfaz web."
    }

# Modelo Pydantic para validar un filtro individual (diccionario)
class Filtro(BaseModel):
    filtro: dict

# Modelo Pydantic que representa el esquema completo esperado en la petición POST
class FiltrosEntrada(BaseModel):
    variants: Filtro
    samples: Filtro
    samples_variants: Filtro
    effects: Filtro

# Endpoint POST para generar los archivos parquet a partir de filtros recibidos
@app.post("/generar-parquets")
def generar_parquets_endpoint(filtros: FiltrosEntrada):
    try:
        # Crear un directorio temporal único para almacenar los resultados
        carpeta_salida = os.path.join(tempfile.gettempdir(), f"parquets_{uuid.uuid4().hex}")
        os.makedirs(carpeta_salida, exist_ok=True)

        # Construir el diccionario de filtros que se pasará a la función principal
        filtros_dict = {
            "samples": filtros.samples.filtro,
            "effects": filtros.effects.filtro,
            "samples_variants": filtros.samples_variants.filtro,
            "variants": filtros.variants.filtro
        }

        # Ejecutar la función que hace la consulta paralela y genera los archivos parquet
        dfs, mediciones, tiempos_parquets = ejecutar_consulta_y_generar_parquets(filtros_dict, carpeta_salida)

        # Crear un archivo ZIP en memoria para empaquetar los parquet generados
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Añadir ambos archivos parquet al ZIP
            for filename in ["parquet_largo.parquet", "parquet_corto.parquet"]:
                path = os.path.join(carpeta_salida, filename)
                zip_file.write(path, arcname=filename)

        # Resetear el cursor de lectura del buffer
        zip_buffer.seek(0)

        # Eliminar el directorio temporal y su contenido para liberar espacio
        shutil.rmtree(carpeta_salida, ignore_errors=True)

        # Devolver el ZIP generado como una respuesta de streaming para descarga directa
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=parquets_generados.zip"}
        )

    except Exception as e:
        # Capturar cualquier error y devolver una excepción HTTP 500 con el mensaje del error
        raise HTTPException(status_code=500, detail=str(e))
