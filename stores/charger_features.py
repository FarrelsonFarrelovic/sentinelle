"""J2 — Features « profil » d'un compte dans Valkey + mesure de la latence de lecture.
Clé : feat:<compte_id> (hash)     Usage : python3 stores/charger_features.py
"""
import os, random, statistics, time
import duckdb, redis

HIST = "data/historique/*.parquet"
r = redis.Redis.from_url(os.getenv("VALKEY_URL", "redis://localhost:6379/0"), decode_responses=True)

con = duckdb.connect()
con.execute("SET TimeZone='UTC'")
rows = con.execute(f"""
    WITH h AS (SELECT * FROM read_parquet('{HIST}')),
         fin AS (SELECT max(ts) AS t FROM h)
    SELECT compte_id,
           count(*)                                       AS nb_tx_30j,
           round(avg(montant), 2)                         AS montant_moyen_30j,
           max(montant)                                   AS montant_max_30j,
           count(*) FILTER (WHERE type = 'TRANSFERT')     AS nb_transferts_30j,
           count(DISTINCT device_id)                      AS nb_devices_30j
    FROM h, fin
    WHERE h.ts >= fin.t - INTERVAL 30 DAY
    GROUP BY compte_id
""").fetchall()
print(f"{len(rows)} comptes avec features")

t0 = time.time()
pipe = r.pipeline(transaction=False)
for i, (cid, n, moy, mx, ntr, ndev) in enumerate(rows, 1):
    pipe.hset(f"feat:{cid}", mapping={"nb_tx_30j": n, "montant_moyen_30j": float(moy),
                                      "montant_max_30j": float(mx),
                                      "nb_transferts_30j": ntr, "nb_devices_30j": ndev})
    if i % 5000 == 0:
        pipe.execute()
pipe.execute()
print(f"écriture : {time.time() - t0:.1f} s")

ids = [row[0] for row in rows]
random.seed(1)
lat = []
for cid in random.choices(ids, k=10_000):
    t = time.perf_counter()
    r.hgetall(f"feat:{cid}")
    lat.append((time.perf_counter() - t) * 1000)
lat.sort()
p = lambda q: lat[int(q * len(lat)) - 1]
print(f"latence lecture (ms) : médiane={statistics.median(lat):.2f}  p95={p(.95):.2f}  p99={p(.99):.2f}  max={lat[-1]:.2f}")
print("objectif < 5 ms :", "OK" if p(.95) < 5 else "NON ATTEINT")
