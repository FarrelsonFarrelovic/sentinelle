"""Exporte les TRANSFERT de l'historique vers data/virements.csv (lu par Neo4j)."""
import duckdb
con = duckdb.connect()
con.execute("SET TimeZone='UTC'")
con.execute("""COPY (
    SELECT tx_id, strftime(ts, '%Y-%m-%dT%H:%M:%SZ') AS ts, compte_id, compte_dest, montant
    FROM read_parquet('data/historique/*.parquet')
    WHERE type = 'TRANSFERT' AND compte_dest IS NOT NULL ORDER BY ts)
    TO 'data/virements.csv' (HEADER, DELIMITER ',')""")
print(con.execute("SELECT count(*) FROM read_csv('data/virements.csv')").fetchone()[0], "virements exportés")
