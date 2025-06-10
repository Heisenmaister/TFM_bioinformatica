install.packages(c("arrow", "dplyr"))

library(arrow)
library(dplyr)

# Configurar rutas a los archivos específicos
ruta_archivo_ancho <- "F:/Bioinformatica/TFM/scripts/output/2025-06-05_19-51-36/parquet_corto.parquet"
ruta_archivo_largo <- "F:/Bioinformatica/TFM/scripts/output/2025-06-05_19-51-36/parquet_largo.parquet"

# Cargar y validar dataset
cargar_y_validar_parquet <- function(ruta, nombre = "Dataset") {
  cat("\n", rep("=", 40), "\n")
  cat("Cargando", nombre, "desde:", ruta, "\n")
  
  #Cargar el dataset
  ds <- open_dataset(sources = ruta, format = "parquet")
  
  cat("\n[Validación] Esquema de", nombre, ":\n")
  print(ds$schema)
  
  conteo <- ds %>% 
    count() %>% 
    collect() %>% 
    pull(n)  # Simplificado sin as_vector
  
  cat("\nTotal de filas:", conteo, "\n")
  
  cat("\n[Validación]", nombre, "- Total de filas:", conteo, "\n")
  
  #Muestra de datos
  cat("\n[Validación]", nombre, "- Muestra de 5 filas:\n")
  muestra <- ds %>% 
    head(5) %>% 
    collect()
  
  print(muestra)
  
  #Estructura básica
  cat("\n[Validación]", nombre, "- Resumen:\n")
  cat("- Columnas:", paste(names(ds$schema), collapse = ", "), "\n")
  cat("- Tipos de datos:", paste(sapply(ds$schema$types, toString), collapse = ", "), "\n")
  
  cat(rep("=", 40), "\n")
  return(ds)
}

# Cargar ambos datasets por separado
dataset_ancho <- cargar_y_validar_parquet(
  ruta_archivo_ancho,
  "Parquet Ancho"
)

dataset_largo <- cargar_y_validar_parquet(
  ruta_archivo_largo,
  "Parquet largo"
)

