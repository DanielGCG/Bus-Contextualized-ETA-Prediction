import pandas as pd

# Configuração
LINHA = "913"

STOP_TIMES = "dados brutos/GTFS/stop_times.txt"
STOPS      = "dados brutos/GTFS/stops.txt"
TRIPS      = "dados brutos/GTFS/trips.txt"
OUTPUT     = f"dados brutos/paradas_{LINHA}_final.csv"

# Carrega
stop_times = pd.read_csv(
    STOP_TIMES,
    usecols=["trip_id", "stop_sequence", "stop_id", "shape_dist_traveled"],
    dtype={"stop_id": str, "trip_id": str},
)

trips = pd.read_csv(
    TRIPS,
    usecols=["trip_id", "route_id", "direction_id"],
    dtype={"trip_id": str, "route_id": str},
)

stops = pd.read_csv(
    STOPS,
    usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
    dtype={"stop_id": str},
)

# Filtra pela linha
trips_linha = trips[trips["route_id"].str.contains(LINHA, na=False)]

if trips_linha.empty:
    raise ValueError(f"Nenhuma trip encontrada para a linha '{LINHA}'. "
                     f"Exemplos de route_id: {trips['route_id'].head().tolist()}")

print(f"Linha {LINHA}: {len(trips_linha)} trips encontradas "
      f"(directions: {sorted(trips_linha['direction_id'].unique())})")

# Joins
df = (
    stop_times
    .merge(trips_linha[["trip_id", "direction_id"]], on="trip_id", how="inner")
    .merge(stops, on="stop_id", how="left")
)

# Seleciona colunas
COLS = ["stop_sequence", "stop_id", "stop_name", "stop_lat", "stop_lon", "shape_dist_traveled", "direction_id"]

df = df[COLS].drop_duplicates().sort_values(["direction_id", "stop_sequence"])

print(f"Paradas únicas: {df.shape[0]}  "
      f"(direction 0: {(df['direction_id']==0).sum()}, "
      f"direction 1: {(df['direction_id']==1).sum()})")

# Salva
df.to_csv(OUTPUT, index=False)
print(f"\nSalvo em: {OUTPUT}")
print("\nAmostra:")
print(df.head(10).to_string(index=False))