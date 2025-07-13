library(dplyr)
library(tidyr)
library(ggplot2)
library(viridis)
library(arrow)

# Dataset parquet ya cargado como arrow::Dataset:
ds_largo_MU <- open_dataset("F:/Bioinformatica/TFM/parquets_muestras/parquet_largo.parquet")

# 1️⃣ Colectar
df_local <- ds_largo_MU %>%
  select(name, SYMBOL) %>%
  collect()

# 2️⃣ Contar combinaciones reales
df_counts <- df_local %>%
  group_by(name, SYMBOL) %>%
  summarise(variant_count = n(), .groups = "drop")

# 3️⃣ Crear grid completo name × SYMBOL
muestras <- df_local %>% distinct(name)
genes <- df_local %>% distinct(SYMBOL)
grid_completo <- expand_grid(muestras, genes)

# 4️⃣ Unir grid con conteos y rellenar vacíos con 0
df_counts_full <- grid_completo %>%
  left_join(df_counts, by = c("name", "SYMBOL")) %>%
  mutate(variant_count = ifelse(is.na(variant_count), 0, variant_count))

# 5️⃣ Heatmap sin huecos
ggplot(df_counts_full, aes(x = SYMBOL, y = name, fill = variant_count)) +
  geom_tile(color = "grey") +
  scale_fill_viridis_c(
    option = "plasma",
    name = "Nº variantes"
  ) +
  labs(
    title = "Frecuencia de variantes por gen y muestra",
    x = "GENES (SYMBOL)",
    y = "Muestras"
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)
  )
