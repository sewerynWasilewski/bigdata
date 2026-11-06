# Projekt 1 – MapReduce, Hive, Apache Airflow

| | |
|---|---|
| **Numer projektu** | 160128 |
| **Zestaw** | Zestaw 11 – flights-airports |
| **Wariant MapReduce** | MapReduce Classic (Java) |

## Źródła

- Opis celu przetwarzania dla wszystkich zestawów:
  https://github.com/BigDataBatchProcessing/ProjectMapReduceHive
  - mój zestaw: [Zestaw 11 - flights-airports](https://github.com/BigDataBatchProcessing/ProjectMapReduceHive/blob/main/Zestaw%2011%20-%20flights-airports/README.md)
  - szablon przepływu Airflow: `projekt1.py`
  - wyniki kontrolne: `expected_results-2026-10.md`
- Opis zbiorów danych:
  https://github.com/BigDataBatchProcessing/ProjectDataSets
  - mój zestaw: [Zestaw 11 - flights-airports](https://github.com/BigDataBatchProcessing/ProjectDataSets/blob/main/Zestaw%2011%20-%20flights-airports/README.md)
- Strona z danymi do pobrania:
  https://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekty/index.html
  - dane: `zestaw11.zip`
  - zbiór testowy: `test2026-10/zestaw11.zip`

## Dokumenty w repozytorium

| Plik | Zawartość |
|---|---|
| [OPIS_PROJEKTU.md](OPIS_PROJEKTU.md) | Ogólny opis projektu i wymagania (z platformy kursu) |
| [KRYTERIA_OCENY.md](KRYTERIA_OCENY.md) | Wszystkie kryteria oceny (MapReduce, Hive, Airflow) |
| [NOTATKI_DO_OBRONY.md](NOTATKI_DO_OBRONY.md) | Co robi każdy element i dlaczego – pytania prowadzącego |
| [KROKI_WYKONANIA.md](KROKI_WYKONANIA.md) | Kolejne kroki wykonania projektu (lokalnie i na GCP) |

## Struktura

```
mapreduce/          źródła programu MapReduce (Maven, pakiet flights)
project_files/      pliki projektu: flights-airports.jar, run_mr.sh, hive.hql, run_hive.sh
projekt1.py         przepływ Apache Airflow
wyniki/             wynik końcowy (output6.json) i log tasku get_output
```

## Cel przetwarzania – Zestaw 11 flights-airports

### Program MapReduce (2)

Działając na zbiorze `datasource1` (1) należy wyznaczyć statystyki lotów wychodzących
z poszczególnych lotnisk w podziale na miesiąc wylotu.

Grupowanie według:
- `departure_airport_id` – identyfikator lotniska wylotu,
- `departure_month` – miesiąc wylotu (wyodrębniony z `scheduled_departure`).

Wynik (3):
- `departure_airport_id` – identyfikator lotniska wylotu,
- `departure_month` – miesiąc wylotu w formacie `YYYY-MM`,
- `flight_count` – liczba lotów w danej grupie,
- `avg_ticket_price` – średnia cena biletu w USD,
- `avg_delay_min` – średnie opóźnienie lotów ze statusem `Delayed` (brak takich lotów → `0`).

### Program Hive (5)

Działając na wyniku MapReduce (3) oraz `datasource4` (4) należy powiązać loty z lotniskami
i obliczyć zestawienia na poziomie krajów: łączną liczbę lotów wychodzących i średnią cenę biletu,
a następnie w ramach każdego kontynentu nadać krajom pozycję w rankingu według liczby lotów
oraz obliczyć odchylenie liczby lotów kraju od średniej dla krajów tego kontynentu.

Wynik (6), JSON:
- `continent` – kontynent,
- `country` – kraj,
- `total_flights` – liczba lotów wychodzących z kraju,
- `avg_ticket_price` – średnia cena biletu (po wszystkich lotach wychodzących z kraju),
- `rank_in_continent` – pozycja w rankingu w ramach kontynentu (1 = najwięcej lotów; remisy: 1, 2, 2, 4),
- `flights_deviation` – różnica między liczbą lotów kraju a średnią dla krajów kontynentu.

## Zbiory danych

Dane sztuczne wygenerowane biblioteką [DataFaker](https://www.datafaker.net/).

### `datasource1` – loty (flights)

CSV z nagłówkiem. Pola:

0. `flight_id` – unikalny identyfikator lotu (UUID)
1. `flight_number` – numer lotu (np. LO123)
2. `departure_airport_id` – lotnisko wylotu (`airport_id` ze słownika lotnisk)
3. `arrival_airport_id` – lotnisko przylotu (`airport_id` ze słownika lotnisk)
4. `scheduled_departure` – planowany wylot (`yyyy-MM-dd'T'HH:mm`)
5. `scheduled_arrival` – planowany przylot (`yyyy-MM-dd'T'HH:mm`)
6. `delay_min` – opóźnienie w minutach
7. `ticket_price_usd` – cena biletu w USD
8. `airline` – linia lotnicza (`LOT`, `Lufthansa`, `Ryanair`, `Emirates`, `Delta`)
9. `status` – status lotu (`On time`, `Delayed`, `Cancelled`)

### `datasource4` – lotniska (airports)

CSV z nagłówkiem. Pola:

0. `airport_id` – unikalny identyfikator lotniska (kod IATA/ICAO)
1. `airport_name` – nazwa lotniska
2. `city` – miasto
3. `country` – kraj
4. `continent` – kontynent (`Europe`, `Asia`, `North America`, `South America`, `Africa`, `Oceania`)
5. `type` – typ lotniska (`International`, `Regional`, `Domestic`)

## Uruchomienie

```bash
run_mr.sh   input_dir1 output_dir3
run_hive.sh input_dir3 input_dir4 output_dir6
```

Szczegóły (budowa jara, klaster Dataproc, Airflow): [KROKI_WYKONANIA.md](KROKI_WYKONANIA.md).
