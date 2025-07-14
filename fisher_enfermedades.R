library(dplyr)
library(ggplot2)
library(ggrepel)

df_brug <- read.csv("F:/Bioinformatica/TFM/parquets_AF/df_brug.csv")
df_SADS <- read.csv("F:/Bioinformatica/TFM/parquets_AF/df_SADS.csv")

# Paso 1
counts_brug <- df_brug %>%
  filter(afec_patol_prin == "Afectado") %>%
  group_by(key) %>%
  summarise(brug_count = n_distinct(id))

total_brug <- df_brug %>%
  filter(afec_patol_prin == "Afectado") %>%
  summarise(n = n_distinct(id)) %>%
  pull(n)

counts_SADS <- df_SADS %>%
  filter(afec_patol_prin == "Afectado") %>%
  group_by(key) %>%
  summarise(SADS_count = n_distinct(id))

total_SADS <- df_SADS %>%
  filter(afec_patol_prin == "Afectado") %>%
  summarise(n = n_distinct(id)) %>%
  pull(n)

# Paso 2
conteos <- full_join(counts_brug, counts_SADS, by = "key") %>%
  replace_na(list(brug_count = 0, SADS_count = 0)) %>%
  mutate(
    prop_brug = brug_count / total_brug,
    prop_SADS = SADS_count / total_SADS
  )

# SIN FILTRO — usamos todos
conteos_filtered <- conteos

# Paso 4: Fisher solo si tiene datos válidos
conteos_filtered <- conteos_filtered %>%
  rowwise() %>%
  mutate(
    p_value = ifelse(
      brug_count == 0 & SADS_count == 0,
      NA,  # No tiene datos → NA
      fisher.test(
        matrix(
          c(
            brug_count, total_brug - brug_count,
            SADS_count, total_SADS - SADS_count
          ),
          nrow = 2
        )
      )$p.value
    )
  ) %>%
  ungroup() %>%
  mutate(
    p_adj = p.adjust(p_value, method = "BH"),
    diff_prop = prop_brug - prop_SADS,
    label = ifelse(!is.na(p_value) & p_value < 0.05, key, NA)
  )

# Volcano plot
volcano <- ggplot(conteos_filtered, aes(x = diff_prop, y = -log10(p_value))) +
  geom_point(
    aes(color = diff_prop > 0),
    alpha = 0.9, size = 4,
    position = position_jitter(width = 0.005, height = 0)
  ) +
  geom_text_repel(aes(label = label), size = 4, max.overlaps = 15) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  geom_hline(yintercept = -log10(0.05), linetype = "dashed", color = "red") +
  scale_color_manual(
    values = c("TRUE" = "#1b9e77", "FALSE" = "#d95f02"),
    labels = c("TRUE" = "Más en Brugada", "FALSE" = "Más en SADS"),
    name = "Grupo con mayor proporción"
  ) +
  labs(
    x = "Diferencia de proporciones (Brugada - SADS)",
    y = "-log10 p-valor",
    title = "Proporción de presencia de variantes por enfermedad"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5, margin = margin(b = 15))
  ) +
  guides(color = "none")

volcano
