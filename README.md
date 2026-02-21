# KV Engine

Kratak opis: konzolni Key-Value engine sa WAL + Memtable + SSTable + LSM, cache-om, TTL, backup/checkpoint i skeniranjem.

Konfiguracija se cita iz `config.json`.

## Osnovne komande

- `PUT(key,value)`
- `GET(key)`
- `DELETE(key)`
- `DELETE_RANGE(startKey,endKey)`
- `PREFIX_SCAN(prefix,pageNumber,pageSize)`
- `RANGE_SCAN(minKey,maxKey,pageNumber,pageSize)`
- `PREFIX_ITERATE(prefix)`
- `RANGE_ITERATE(minKey,maxKey)`
- `ITER_NEXT(iteratorId)`
- `ITER_STOP(iteratorId)`

## Kako rade scan algoritmi

Implementacija je u `internal/engine/scan_iterators.go`.

### `RANGE_SCAN(min,max,page,size)`

1. Formira filter `min <= key <= max`.
2. Iz Memtable uzima snapshot i filtrira kljuceve po opsegu.
3. Iz SSTable/LSM sloja trazi samo range podatke preko `CollectKVRangeRecords`.
4. Spaja izvore, uzima najnoviji zapis po kljucu (`Seq`), izbacuje tombstone/expired.
5. Sortira po kljucu i vraca trazenu stranicu.

Napomena: za SSTable putanju ne cita se "sve naslepo". Prvo se suzava pretraga (summary min/max + index), pa se skeniraju samo relevantni data opsezi/blokovi za taj range.

### `PREFIX_SCAN(prefix,page,size)`

1. Filtrira kljuceve po prefiksu.
2. Spajanje izvora + deduplikacija + TTL/tombstone filtriranje.
3. Sortiranje i paginacija kao kod range skena.

## Kako rade iteratori

- `PREFIX_ITERATE` i `RANGE_ITERATE` koriste isti algoritam kao scan da prvo izgrade sortiranu listu rezultata.
- Zatim se cuva iterator stanje u memoriji (`id`, lista, pozicija).
- `ITER_NEXT(id)` vraca sledeci element O(1), `ITER_STOP(id)` zatvara iterator.

To znaci da iterator radi nad stabilnim snapshot-om rezultata u trenutku kreiranja.
