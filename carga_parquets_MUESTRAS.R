library(httr)
library(jsonlite)
library(arrow)
library(fs)
library(dplyr)

consulta_api <- function(filtros) {
  # Convierte filtros a JSON
  json_body <- toJSON(filtros, auto_unbox = TRUE)
  cat("Enviando filtro:\n", json_body, "\n")
  
  # POST a la API
  response <- tryCatch(
    POST(
      url = "http://localhost:8000/generar-parquets",
      body = json_body,
      encode = "json",
      content_type_json()
    ),
    error = function(e) {
      message("Error en la petición POST: ", e$message)
      return(NULL)
    }
  )
  
  if (is.null(response)) return(NULL)
  
  if (http_error(response)) {
    content_resp <- content(response, as = "text", encoding = "UTF-8")
    message("Error HTTP ", status_code(response), ":\n", content_resp)
    return(NULL)
  }
  
  # Guardar ZIP recibido
  zip_temp <- tempfile(fileext = ".zip")
  tryCatch(
    writeBin(content(response, "raw"), zip_temp),
    error = function(e) {
      message("Error al guardar ZIP: ", e$message)
      return(NULL)
    }
  )
  
  # Crear carpeta temp para descomprimir
  unzip_dir <- tempfile()
  dir_create(unzip_dir)
  
  tryCatch(
    unzip(zip_temp, exdir = unzip_dir),
    error = function(e) {
      message("Error al descomprimir ZIP: ", e$message)
      return(NULL)
    }
  )
  
  # Verifica qué Parquets hay
  parquet_files <- dir(unzip_dir, pattern = "\\.parquet$", full.names = TRUE)
  cat("Parquet(s) encontrados:\n")
  print(parquet_files)
  
  if (length(parquet_files) == 0) {
    message("No se encontraron archivos Parquet en el ZIP.")
    return(NULL)
  }
  
  # Ruta final fija
  parquet_dest_dir <- "F:/Bioinformatica/TFM/parquets_muestras"
  dir_create(parquet_dest_dir)  # Asegura que exista
  
  # Lista de nombres
  parquet_nombres <- c("parquet_largo.parquet", "parquet_corto.parquet")
  parquet_paths <- list()
  
  for (nombre in parquet_nombres) {
    parquet_file <- parquet_files[basename(parquet_files) == nombre]
    if (length(parquet_file) == 0) {
      message("No se encontró: ", nombre)
      next
    }
    final_parquet_path <- file.path(parquet_dest_dir, nombre)
    file.copy(parquet_file, final_parquet_path, overwrite = TRUE)
    parquet_paths[[nombre]] <- final_parquet_path
    cat("Parquet copiado en:\n", final_parquet_path, "\n")
  }
  
  # Limpia temporales
  unlink(zip_temp)
  unlink(unzip_dir, recursive = TRUE)
  
  # Devuelve rutas finales
  return(parquet_paths)
}

# === EJEMPLO DE USO ===

empty_named_list <- function() { structure(list(), names = character(0)) }

filtros_MUESTRAS <- list(
  samples = list(
    filtro = list(
      afec_patol_prin = "Afectado"
    )
  ),
  effects = list(
    filtro = list(
      SYMBOL = list(`$in` = c(
        "MIR663AHG", "ROCK1P1", "PVT1", "DMD", "TCF4",
        "FTX", "MIR99AHG", "MIR924HG", "HLA-B"
      )),
      AF = list(`$lte` = 0.05)
    )
  ),
  samples_variants = list(
    filtro = empty_named_list()
  ),
  variants = list(
    filtro = empty_named_list()
  )
)

# Ejecuta la consulta: guarda rutas
parquet_paths <- consulta_api(filtros_MUESTRAS)

# Abre cada uno por separado
if (!is.null(parquet_paths[["parquet_largo.parquet"]])) {
  ds_largo_MU <- open_dataset(parquet_paths[["parquet_largo.parquet"]])
  cat("Dataset largo cargado correctamente.\n")
}

if (!is.null(parquet_paths[["parquet_corto.parquet"]])) {
  ds_corto_MU <- open_dataset(parquet_paths[["parquet_corto.parquet"]])
  cat("Dataset corto cargado correctamente.\n")
}

# Usa normalmente
if (exists("ds_largo_MU")) {
  cat("\nPrimeras filas del dataset largo:\n")
  print(head(collect(ds_largo_MU)))
}

if (exists("ds_corto_MU")) {
  cat("\nPrimeras filas del dataset corto:\n")
  print(head(collect(ds_corto_MU)))
}
