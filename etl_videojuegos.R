#!/usr/bin/env Rscript

# etl_videojuegos.R
# ETL completo en un solo archivo para analisis de videojuegos Backloggd.
# Genera reporte textual y graficas en la carpeta output/.

log_info <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("%s - INFO - %s\n", timestamp, message))
}

convert_plays_to_numeric <- function(x) {
  if (is.na(x) || trimws(as.character(x)) == "") {
    return(0)
  }

  value <- toupper(gsub(",", "", trimws(as.character(x))))

  if (grepl("K$", value)) {
    return(as.numeric(sub("K$", "", value)) * 1000)
  }

  if (grepl("M$", value)) {
    return(as.numeric(sub("M$", "", value)) * 1000000)
  }

  parsed <- suppressWarnings(as.numeric(value))
  if (is.na(parsed)) {
    return(0)
  }

  parsed
}

parse_genres <- function(genres_str) {
  if (is.na(genres_str) || trimws(genres_str) == "") {
    return(character(0))
  }

  clean <- gsub("^\\[|\\]$", "", genres_str)
  if (trimws(clean) == "") {
    return(character(0))
  }

  parts <- unlist(strsplit(clean, ",", fixed = TRUE))
  genres <- trimws(gsub("^'|'$", "", parts))
  genres[genres != ""]
}

extract_data <- function(csv_path) {
  log_info(sprintf("Extrayendo datos desde: %s", csv_path))

  if (!file.exists(csv_path)) {
    stop(sprintf("Archivo no encontrado: %s", csv_path), call. = FALSE)
  }

  df <- read.csv(
    csv_path,
    row.names = 1,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  log_info(sprintf("Datos extraidos exitosamente. Registros: %s", format(nrow(df), big.mark = ",")))
  log_info(sprintf("Columnas: %s", paste(names(df), collapse = ", ")))
  df
}

transform_data <- function(df) {
  log_info("Transformando datos...")

  clean_df <- df
  clean_df$Plays_numeric <- vapply(clean_df$Plays, convert_plays_to_numeric, numeric(1))
  clean_df$Rating <- suppressWarnings(as.numeric(clean_df$Rating))
  clean_df$Genres_list <- I(lapply(clean_df$Genres, parse_genres))

  clean_df
}

get_most_played_game <- function(df) {
  idx <- which.max(df$Plays_numeric)
  row <- df[idx, ]

  list(
    title = row$Title,
    plays = row$Plays,
    plays_numeric = row$Plays_numeric,
    rating = row$Rating,
    genres = row$Genres,
    platforms = row$Platforms,
    release_date = row$Release_Date
  )
}

get_top_genres <- function(df, top_n = 20) {
  lengths_vec <- lengths(df$Genres_list)
  if (sum(lengths_vec) == 0) {
    return(data.frame(Genre = character(0), Total_Plays = numeric(0)))
  }

  genres <- unlist(df$Genres_list)
  plays <- rep(df$Plays_numeric, lengths_vec)
  genre_df <- data.frame(Genre = genres, Plays = plays, stringsAsFactors = FALSE)

  agg <- aggregate(Plays ~ Genre, data = genre_df, FUN = sum)
  names(agg)[2] <- "Total_Plays"

  ordered <- agg[order(agg$Total_Plays, decreasing = TRUE), ]
  head(ordered, top_n)
}

get_genre_rating_summary <- function(df, genres_df) {
  genres <- genres_df$Genre

  rows <- lapply(genres, function(genre) {
    mask <- vapply(df$Genres_list, function(x) genre %in% x, logical(1))
    genre_games <- df[mask, ]

    if (nrow(genre_games) == 0) {
      return(NULL)
    }

    valid <- genre_games[!is.na(genre_games$Rating), ]
    if (nrow(valid) == 0) {
      return(NULL)
    }

    data.frame(
      Genre = genre,
      Average_Rating = mean(valid$Rating),
      Game_Count = nrow(genre_games),
      stringsAsFactors = FALSE
    )
  })

  summary_df <- do.call(rbind, rows)
  if (is.null(summary_df) || nrow(summary_df) == 0) {
    return(data.frame(Genre = character(0), Average_Rating = numeric(0), Game_Count = integer(0)))
  }

  summary_df[order(summary_df$Average_Rating, decreasing = TRUE), ]
}

get_summary_statistics <- function(df) {
  unique_genres <- unique(unlist(df$Genres_list))

  list(
    total_games = nrow(df),
    total_plays = sum(df$Plays_numeric, na.rm = TRUE),
    average_rating = mean(df$Rating, na.rm = TRUE),
    max_rating = max(df$Rating, na.rm = TRUE),
    min_rating = min(df$Rating, na.rm = TRUE),
    unique_genres = length(unique_genres)
  )
}

get_top_games_by_genre <- function(df, genre, top_n = 5) {
  mask <- vapply(df$Genres_list, function(x) genre %in% x, logical(1))
  genre_games <- df[mask, c("Title", "Plays", "Plays_numeric", "Rating")]

  if (nrow(genre_games) == 0) {
    return(genre_games)
  }

  ordered <- genre_games[order(genre_games$Plays_numeric, decreasing = TRUE), ]
  head(ordered, top_n)
}

generate_text_report <- function(output_dir, most_played, stats, genres_df, ratings_summary) {
  report_path <- file.path(output_dir, "analisis_videojuegos_r.txt")

  lines <- c(
    strrep("=", 80),
    "ANALISIS DE VIDEOJUEGOS - BACKLOGGD (R)",
    strrep("=", 80),
    sprintf("Fecha de generacion: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "ESTADISTICAS GENERALES",
    strrep("-", 80),
    sprintf("Total de juegos: %s", format(stats$total_games, big.mark = ",")),
    sprintf("Total de jugadas: %s", format(round(stats$total_plays), big.mark = ",")),
    sprintf("Rating promedio: %.2f", stats$average_rating),
    sprintf("Rating maximo: %.2f", stats$max_rating),
    sprintf("Rating minimo: %.2f", stats$min_rating),
    sprintf("Generos unicos: %s", stats$unique_genres),
    "",
    "JUEGO MAS JUGADO",
    strrep("-", 80),
    sprintf("Titulo: %s", most_played$title),
    sprintf("Numero de jugadas: %s", most_played$plays),
    sprintf("Rating: %.2f", most_played$rating),
    sprintf("Generos: %s", most_played$genres),
    sprintf("Plataformas: %s", most_played$platforms),
    sprintf("Fecha de lanzamiento: %s", most_played$release_date),
    "",
    "TOP 20 GENEROS MAS JUGADOS",
    strrep("-", 80)
  )

  for (i in seq_len(nrow(genres_df))) {
    lines <- c(lines, sprintf("%d. %s: %s jugadas", i, genres_df$Genre[i], format(round(genres_df$Total_Plays[i]), big.mark = ",")))
  }

  lines <- c(lines, "", "RATINGS PROMEDIO POR GENERO", strrep("-", 80))

  for (i in seq_len(nrow(ratings_summary))) {
    lines <- c(lines, sprintf("%s: %.2f (%d juegos)", ratings_summary$Genre[i], ratings_summary$Average_Rating[i], ratings_summary$Game_Count[i]))
  }

  lines <- c(lines, "", strrep("=", 80), "Fin del reporte", strrep("=", 80))
  writeLines(lines, report_path, useBytes = TRUE)

  report_path
}

plot_most_played_game <- function(output_dir, most_played) {
  output_path <- file.path(output_dir, "juego_mas_jugado_r.png")

  png(output_path, width = 1400, height = 900, res = 140)
  par(mar = c(5, 10, 4, 2))

  barplot(
    most_played$plays_numeric,
    horiz = TRUE,
    names.arg = most_played$title,
    col = "#1f77b4",
    border = NA,
    xlab = "Numero de Jugadas",
    main = "JUEGO MAS JUGADO"
  )

  text(
    x = most_played$plays_numeric,
    y = 0.7,
    labels = sprintf("%s jugadas | Rating: %.1f/5.0", most_played$plays, most_played$rating),
    pos = 4,
    cex = 1
  )

  dev.off()
  output_path
}

plot_top_genres <- function(output_dir, genres_df) {
  output_path <- file.path(output_dir, "top_20_generos_r.png")
  ordered <- genres_df[order(genres_df$Total_Plays), ]

  png(output_path, width = 1600, height = 1100, res = 140)
  par(mar = c(5, 12, 4, 2))

  colors <- colorRampPalette(c("#4f81bd", "#9bbb59"))(nrow(ordered))
  bp <- barplot(
    ordered$Total_Plays,
    horiz = TRUE,
    names.arg = ordered$Genre,
    las = 1,
    col = colors,
    border = NA,
    xlab = "Total de Jugadas",
    main = "TOP 20 GENEROS MAS JUGADOS"
  )

  text(
    x = ordered$Total_Plays,
    y = bp,
    labels = format(round(ordered$Total_Plays), big.mark = ","),
    pos = 4,
    cex = 0.8
  )

  dev.off()
  output_path
}

plot_genre_ratings_pie <- function(output_dir, ratings_summary, top_n = 15) {
  output_path <- file.path(output_dir, "ratings_por_genero_torta_r.png")
  top_ratings <- head(ratings_summary, top_n)

  png(output_path, width = 1800, height = 900, res = 140)
  par(mfrow = c(1, 2), mar = c(5, 4, 4, 2))

  colors <- rainbow(nrow(top_ratings), s = 0.6, v = 0.9)

  pie(
    top_ratings$Average_Rating,
    labels = top_ratings$Genre,
    col = colors,
    main = sprintf("Top %d generos mejor valorados", top_n)
  )

  bp <- barplot(
    rev(top_ratings$Average_Rating),
    horiz = TRUE,
    names.arg = rev(top_ratings$Genre),
    col = rev(colors),
    xlim = c(0, 5),
    border = NA,
    xlab = "Rating Promedio",
    main = "Rating promedio por genero"
  )

  text(x = rev(top_ratings$Average_Rating), y = bp, labels = sprintf("%.2f", rev(top_ratings$Average_Rating)), pos = 4, cex = 0.8)

  dev.off()
  output_path
}

plot_comprehensive_analysis <- function(output_dir, genres_df, ratings_summary) {
  output_path <- file.path(output_dir, "analisis_combinado_r.png")

  combined <- merge(genres_df, ratings_summary, by = "Genre", all = FALSE)
  combined <- head(combined[order(combined$Total_Plays, decreasing = TRUE), ], 15)

  png(output_path, width = 1700, height = 1000, res = 140)
  par(mar = c(8, 5, 4, 5))

  bp <- barplot(
    combined$Total_Plays,
    names.arg = combined$Genre,
    las = 2,
    col = "#3498db",
    border = NA,
    main = "ANALISIS COMBINADO: JUGADAS VS RATING",
    ylab = "Total de Jugadas"
  )

  par(new = TRUE)
  plot(
    x = bp,
    y = combined$Average_Rating,
    type = "o",
    pch = 16,
    col = "#e74c3c",
    axes = FALSE,
    xlab = "",
    ylab = "",
    ylim = c(0, 5)
  )

  axis(side = 4, col = "#e74c3c", col.axis = "#e74c3c")
  mtext("Rating Promedio", side = 4, line = 3, col = "#e74c3c")
  legend("topright", legend = c("Total Jugadas", "Rating Promedio"), fill = c("#3498db", NA), border = NA, lty = c(0, 1), pch = c(NA, 16), col = c("#3498db", "#e74c3c"))

  dev.off()
  output_path
}

plot_top5_for_top6_genres <- function(output_dir, df, top_genres) {
  generated_files <- c()

  for (genre in top_genres) {
    top_games_df <- get_top_games_by_genre(df, genre, top_n = 5)
    safe_genre_name <- tolower(gsub("[^a-zA-Z0-9]+", "_", genre))
    output_path <- file.path(output_dir, sprintf("top_5_juegos_%s_r.png", safe_genre_name))

    if (nrow(top_games_df) > 0) {
      ordered <- top_games_df[order(top_games_df$Plays_numeric), ]

      png(output_path, width = 1500, height = 900, res = 140)
      par(mar = c(5, 12, 4, 2))

      colors <- colorRampPalette(c("#5e4fa2", "#fdae61"))(nrow(ordered))
      bp <- barplot(
        ordered$Plays_numeric,
        horiz = TRUE,
        names.arg = ordered$Title,
        las = 1,
        col = colors,
        border = NA,
        xlab = "Numero de Jugadas",
        main = sprintf("TOP 5 JUEGOS MAS JUGADOS - %s", toupper(genre))
      )

      text(
        x = ordered$Plays_numeric,
        y = bp,
        labels = sprintf("%s | %.1f", ordered$Plays, ordered$Rating),
        pos = 4,
        cex = 0.8
      )

      dev.off()
      generated_files <- c(generated_files, output_path)
    }
  }

  generated_files
}

run_pipeline <- function(csv_path = "backloggd_games.csv", output_dir = "output") {
  start_time <- Sys.time()

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    log_info(sprintf("Directorio de salida creado: %s", output_dir))
  }

  log_info("Iniciando ETL completo...")

  df_raw <- extract_data(csv_path)
  df <- transform_data(df_raw)

  most_played <- get_most_played_game(df)
  genres_df <- get_top_genres(df, top_n = 20)
  ratings_summary <- get_genre_rating_summary(df, genres_df)
  stats <- get_summary_statistics(df)

  report_path <- generate_text_report(output_dir, most_played, stats, genres_df, ratings_summary)
  chart1 <- plot_most_played_game(output_dir, most_played)
  chart2 <- plot_top_genres(output_dir, genres_df)
  chart3 <- plot_genre_ratings_pie(output_dir, ratings_summary, top_n = 15)
  chart4 <- plot_comprehensive_analysis(output_dir, genres_df, ratings_summary)

  top6 <- head(genres_df$Genre, 6)
  top5_charts <- plot_top5_for_top6_genres(output_dir, df, top6)

  end_time <- Sys.time()
  duration <- as.numeric(difftime(end_time, start_time, units = "secs"))

  log_info("ETL completado exitosamente")
  cat("\nResumen:\n")
  cat(sprintf("- Tiempo de ejecucion: %.2f segundos\n", duration))
  cat(sprintf("- Reporte: %s\n", report_path))
  cat(sprintf("- Graficos principales: %s, %s, %s, %s\n", chart1, chart2, chart3, chart4))
  cat(sprintf("- Graficos top5 por genero generados: %d\n", length(top5_charts)))

  invisible(list(
    report = report_path,
    charts = c(chart1, chart2, chart3, chart4),
    top5_charts = top5_charts,
    duration = duration
  ))
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)

  csv_path <- if (length(args) >= 1) args[[1]] else "backloggd_games.csv"
  output_dir <- if (length(args) >= 2) args[[2]] else "output"

  tryCatch(
    run_pipeline(csv_path = csv_path, output_dir = output_dir),
    error = function(e) {
      cat(sprintf("\nERROR: %s\n", e$message))
      quit(status = 1)
    }
  )
}
