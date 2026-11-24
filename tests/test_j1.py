"""J1 — contrôles qualité de l'historique avant chargement dans le lakehouse."""
from pathlib import Path
import duckdb
import pytest

HIST = Path(__file__).resolve().parent.parent / "data" / "historique"
pytestmark = pytest.mark.skipif(not HIST.exists(), reason="lancer make donnees d'abord")


def q(sql):
    return duckdb.execute(sql.format(h=f"read_parquet('{HIST}/*.parquet')")).fetchone()[0]


def test_tx_id_uniques():
    assert q("SELECT count(*) - count(DISTINCT tx_id) FROM {h}") == 0


def test_montants_positifs():
    assert q("SELECT count(*) FROM {h} WHERE montant <= 0") == 0


def test_types_connus():
    assert q("SELECT count(*) FROM {h} WHERE type NOT IN "
             "('PAIEMENT','TRANSFERT','RETRAIT','DEPOT','FACTURE','RECHARGE')") == 0
