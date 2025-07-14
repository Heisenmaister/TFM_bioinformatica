library(dplyr)
library(tidyr)
library(ggplot2)
library(viridis)
library(arrow)

# Dataset parquet ya cargado como arrow::Dataset:
ds_largo_MU <- open_dataset("F:/Bioinformatica/TFM/parquets_muestras/parquet_largo_MU.parquet")

# 1️⃣ Colectar muestras, genes y patología
df_local <- ds_largo_MU %>%
  select(name, SYMBOL, patol_prin) %>%
  collect()

# 2️⃣ Contar combinaciones reales
df_counts <- df_local %>%
  group_by(name, SYMBOL) %>%
  summarise(variant_count = n(), .groups = "drop")

# 3️⃣ Crear grid completo name × SYMBOL
muestras <- df_local %>% distinct(name, patol_prin)
genes <- df_local %>% distinct(SYMBOL)
grid_completo <- expand_grid(muestras, genes)

# 4️⃣ Unir grid con conteos y rellenar vacíos con 0
df_counts_full <- grid_completo %>%
  left_join(df_counts, by = c("name", "SYMBOL")) %>%
  mutate(variant_count = ifelse(is.na(variant_count), 0, variant_count))

# 5️⃣ Calcular posición x máxima para el heatmap
num_genes <- nrow(genes)

# 6️⃣ Plot heatmap + texto de patología
ggplot(df_counts_full, aes(x = SYMBOL, y = name, fill = variant_count)) +
  geom_tile(color = "grey") +
  scale_fill_viridis_c(option = "plasma", name = "Nº variantes") +
  labs(title = "Número de variantes por gen y muestra",
       x = "GENES (SYMBOL)", y = "Muestras") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  
  # Añadir texto de patología justo a la derecha del heatmap
  geom_text(data = muestras,
            aes(x = num_genes + 0.5, y = name, label = patol_prin),
            inherit.aes = FALSE,
            hjust = 0,
            size = 3) +
  
  # Expandir límites para que quepa el texto
  scale_x_discrete(expand = expansion(mult = c(0.02, 0.3))) +
  
  # Opcional: ocultar ticks y etiquetas extra en el eje X para la zona texto
  theme(
    axis.ticks.x = element_line(),
    axis.text.x = element_text(),
    panel.grid = element_blank()
  )
