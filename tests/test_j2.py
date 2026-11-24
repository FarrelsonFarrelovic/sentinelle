"""J2 — contrôles du graphe des virements (sans Docker : DuckDB + Python sur l'historique)."""
import collections
from pathlib import Path

import duckdb
import pytest

RACINE = Path(__file__).resolve().parent.parent
HIST = RACINE / "data" / "historique"
pytestmark = pytest.mark.skipif(not HIST.exists(), reason="lancer make donnees d'abord")


def _virements(debut, fin):
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con.execute(
        f"SELECT compte_id, compte_dest, ts, montant FROM read_parquet('{HIST}/*.parquet') "
        "WHERE type='TRANSFERT' AND compte_dest IS NOT NULL AND ts >= ? AND ts < ? ORDER BY ts",
        [debut, fin]).fetchall()


def anneaux(virements, min_len=3, max_len=5, bas=0.94, haut=0.995):
    """Même règle que neo4j/02_anneaux.cypher : cycle de 3 à 5 comptes, dates croissantes,
    montant qui baisse de 0,5 à 6 % à chaque saut."""
    sortants = collections.defaultdict(list)
    for a, b, ts, m in virements:
        sortants[a].append((b, ts, float(m)))
    trouves = set()

    def explorer(depart, chemin, ts, m):
        for b, ts2, m2 in sortants.get(chemin[-1], []):
            if ts2 <= ts or not (bas * m <= m2 <= haut * m):
                continue
            if b == depart and min_len <= len(chemin) <= max_len:
                trouves.add(frozenset(chemin))
            elif b not in chemin and len(chemin) < max_len:
                explorer(depart, chemin + [b], ts2, m2)

    for a, sorts in list(sortants.items()):
        for b, ts, m in sorts:
            explorer(a, [a, b], ts, m)
    return trouves


def test_anneau_connu_detecte():
    res = anneaux(_virements("2026-09-01", "2026-09-08"))
    assert frozenset({"CM093256", "TD007213", "GA001646"}) in res


def test_pas_de_fausse_alerte_massive():
    # quelques anneaux par semaine, pas des centaines
    assert len(anneaux(_virements("2026-09-01", "2026-09-08"))) < 20


def test_requete_cypher_fournie():
    q = (RACINE / "neo4j" / "02_anneaux.cypher").read_text(encoding="utf-8")
    assert "{3,5}" in q and "VIREMENT" in q
