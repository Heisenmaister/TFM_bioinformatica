# -------------------------------------
# Librerías
# -------------------------------------
library(arrow)
library(dplyr)
library(ggplot2)
library(scales)
library(gtools)

# -------------------------------------
# 1️⃣ Leer el parquet largo de la ruta fija
# -------------------------------------
ds_largo_AF <- open_dataset("F:/Bioinformatica/TFM/parquets_AF/parquet_largo.parquet")

# Pasar a dataframe para ggplot
df <- collect(ds_largo_AF)

# -------------------------------------
# 2️⃣ Histograma de distribución de frecuencias alélicas
# -------------------------------------
p1 <- ggplot(df, aes(x = AF)) +
  geom_histogram(bins = 30, fill = "steelblue", color = "black", alpha = 0.8) +
  labs(
    title = "Distribución de frecuencias alélicas",
    x = "Frecuencia alélica (AF)",
    y = "Número de variantes"
  ) +
  scale_x_continuous(
    breaks = seq(0, max(df$AF, na.rm = TRUE), by = 0.002)
  ) +
  scale_y_continuous(
    breaks = seq(0, max(table(cut(df$AF, breaks = 30))), by = 2)
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5)
  )

p1 <- p1 + theme(axis.text.x = element_text(angle = 45, hjust = 1))

print(p1)


# -------------------------------------
# 3️⃣ Manhattan plot (AF por posición genómica)
# -------------------------------------
# -------------------------------------
# Preparar dataframe Manhattan
# -------------------------------------
df_manhattan <- df %>%
  filter(!is.na(chr) & !is.na(pos_start) & !is.na(AF))

# Asegurar orden natural de cromosomas
df_manhattan$chr <- factor(df_manhattan$chr, 
                           levels = mixedsort(unique(df_manhattan$chr)))

p2 <- ggplot(df_manhattan, aes(x = pos_start, y = AF, color = chr)) +
  geom_point(alpha = 0.7, size = 2) +
  facet_wrap(~ chr, scales = "free_x", ncol = 4) +
  labs(
    title = "Frecuencia alélica por posición cromosómica",
    x = "Posición en cromosoma",
    y = "Frecuencia alélica (AF)",
    color = "Cromosoma"
  ) +
  scale_x_continuous(labels = comma) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.text.x = element_text(size = 6)  # 👈 Cambia solo el eje X
  )
    
print(p2)


