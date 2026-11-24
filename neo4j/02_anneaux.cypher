// Anneaux de 3 à 5 comptes sur 7 jours : dates croissantes,
// montant qui baisse de 0,5 à 6 % à chaque saut (commission + tolérance d'arrondi).
MATCH p = (a:Compte)
  ((:Compte)-[v:VIREMENT WHERE v.ts >= datetime('2026-09-01T00:00:00Z')
                          AND v.ts <  datetime('2026-09-08T00:00:00Z')]->(:Compte)){3,5}
  (a)
WITH a, v, [n IN nodes(p) | n.id] AS ids
WHERE all(x IN ids WHERE a.id <= x)
  AND size(ids) - 1 = size(reduce(s = [], x IN ids[0..-1] |
        CASE WHEN x IN s THEN s ELSE s + x END))
  AND all(i IN range(0, size(v) - 2) WHERE
        v[i].ts < v[i+1].ts
    AND v[i+1].montant <= v[i].montant * 0.995
    AND v[i+1].montant >= v[i].montant * 0.94)
RETURN ids[0..-1] AS anneau, size(v) AS longueur, count(*) AS passages,
       min(v[0].ts) AS debut, max(v[0].montant) AS montant_depart
ORDER BY debut;
