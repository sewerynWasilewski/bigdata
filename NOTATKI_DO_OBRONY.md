# Projekt 1 – Zestaw 11 flights-airports (MapReduce Classic) – notatki do obrony

## 1. Co robi projekt (całość w jednym zdaniu)

Z miliona lotów (datasource1) liczę statystyki **lotnisko × miesiąc** w MapReduce,
a potem w Hive łączę je ze słownikiem lotnisk (datasource4), sumuję do poziomu **kraju**
i robię **ranking krajów w obrębie kontynentu** + **odchylenie od średniej kontynentu**.
Wynik zapisuję jako JSON (jeden rekord = jedna linia). Całość orkiestruje Airflow.

```
(1) datasource1: flights CSV ──► (2) MapReduce (Java) ──► (3) HDFS: airport,month,stats
                                                               │
(4) datasource4: airports CSV ─────────────────────────────────┤
                                                               ▼
                                                     (5) Hive (hive.hql)
                                                               ▼
                                                     (6) HDFS: JSONL wynik
Airflow: clean dirs → MapReduce → Hive → get_output
```

## 2. Dane

**datasource1 – loty** (CSV z nagłówkiem, 100 plików, 1 000 000 rekordów):
`flight_id, flight_number, departure_airport_id, arrival_airport_id, scheduled_departure
(yyyy-MM-ddTHH:mm), scheduled_arrival, delay_min, ticket_price_usd, airline, status`
– status: `On time`, `Delayed`, `Cancelled` (każdy ~1/3).

**datasource4 – lotniska** (CSV z nagłówkiem, 100 lotnisk):
`airport_id, airport_name, city, country, continent, type`
– **nazwy lotnisk i krajów są w cudzysłowach i zawierają przecinki** (np. `"Kunde, Champlin and Rice Airport"`),
dlatego w Hive używam `OpenCSVSerde`, a nie zwykłego `FIELDS TERMINATED BY ','`.
– dane są sztuczne (DataFaker), więc np. „American Samoa” może leżeć w „Africa” – to normalne.

## 3. Część MapReduce (Classic, Java)

**Cel:** dla każdej pary (`departure_airport_id`, `departure_month` = YYYY-MM):
- `flight_count` – liczba lotów (wszystkich, również Cancelled),
- `avg_ticket_price` – średnia cena biletu,
- `avg_delay_min` – średnie opóźnienie **tylko lotów Delayed** (brak takich → 0).

**Klasy:**

| Klasa | Rola |
|---|---|
| `FlightsAirports` | Driver (`Tool` + `ToolRunner`): konfiguruje Job, typy, combiner, separator `,` wyniku |
| `FlightsMapper` | Parsuje CSV, waliduje, emituje klucz `"LOTNISKO,YYYY-MM"` → `FlightStatsWritable` |
| `FlightStatsWritable` | Własny typ `Writable`: `flightCount, priceSum, delayedCount, delaySum` |
| `FlightStatsCombiner` | Sumuje składowe; wejście = wyjście (Text, FlightStatsWritable) |
| `FlightsReducer` | Sumuje składowe i dopiero na końcu liczy średnie; emituje tekst CSV |

**Kluczowe pytania i odpowiedzi:**

- *Dlaczego mapper nie emituje średniej?* – Średnia nie jest dystrybutywna: średnia ze średnich ≠ średnia.
  Dlatego emituję **sumę i liczność** osobno (`priceSum`, `flightCount`, `delaySum`, `delayedCount`).
  Dzięki temu combiner może łączyć dane wielokrotnie, a reducer liczy `suma/liczność` na końcu.
- *Dlaczego osobny `delayedCount`?* – Bo średnie opóźnienie liczymy po innym zbiorze (tylko Delayed)
  niż `flight_count` (wszystkie loty). To jest „agregacja warunkowa” (kryterium M04).
- *Po co combiner?* – Częściowa agregacja po stronie mappera zmniejsza ruch sieciowy (shuffle).
  W teście: 1 000 000 rekordów z mapperów → ok. 122 000 po combinerze → 1300 wyników.
  Combiner używa tylko dodawania (przemienne i łączne), więc Hadoop może go uruchomić 0, 1 lub wiele razy.
- *Dlaczego combiner to inna klasa niż reducer?* – Reducer zwraca `Text` (gotowe średnie),
  a combiner musi zwracać ten sam typ co mapper (`FlightStatsWritable`).
- *Jak obsługujesz błędne dane?* – Nagłówek (`flight_id`) jest pomijany; sprawdzam liczbę pól (10),
  format daty regexem, parsowanie liczb w `try/catch`. Błędne rekordy są pomijane i liczone
  w licznikach (`Records.MALFORMED`, `HEADER`, `VALID`) – widać je w logu zadania.
- *Przetwarzanie strumieniowe?* – Mapper emituje od razu każdy rekord, nic nie trzyma w pamięci;
  reducer/combiner trzymają tylko sumy dla bieżącego klucza. Obiekty `Text`/`Writable` są reużywane.
- *Skąd miesiąc?* – `scheduled_departure.substring(0, 7)` po walidacji formatu `yyyy-MM-ddTHH:mm`.
- *Format wyniku (3)?* – `TextOutputFormat` z separatorem `,` (`mapreduce.output.textoutputformat.separator`):
  `departure_airport_id,departure_month,flight_count,avg_ticket_price,avg_delay_min`.
- *Uruchomienie:* `run_mr.sh input_dir1 output_dir3` → najpierw `hadoop fs -rm -r -f output_dir3`
  (powtarzalność), potem `hadoop jar flights-airports.jar flights.FlightsAirports in out`.
- *Wersja Javy?* – Kompilacja z `--release 8` przez Maven, Hadoop jako `provided` (nie pakowany do jara,
  dostarcza go klaster).

## 4. Część Hive

**Cel:** dla każdego (kontynent, kraj): `total_flights`, `avg_ticket_price`, `rank_in_continent`,
`flights_deviation`.

**Struktury (`hive.hql`):**
- `flights_monthly_stats` – `EXTERNAL TABLE` na wyniku MR (`LOCATION '${hivevar:input_dir3}'`),
  kolumny o nazwach dokładnie jak w specyfikacji.
- `airports` – `EXTERNAL TABLE` z `OpenCSVSerde` + `skip.header.line.count=1`.
- `country_flights_ranking` – `EXTERNAL TABLE` z `JsonSerDe` na `output_dir6` – wynik końcowy.
- Każde `CREATE` poprzedzone `DROP TABLE IF EXISTS` (powtarzalność); tabele źródłowe zostają po skrypcie.

**Zapytanie – jedno `INSERT OVERWRITE`, bez tabel pośrednich:**
1. podzapytanie: `JOIN` po `departure_airport_id = airport_id`, `GROUP BY continent, country`,
   `SUM(flight_count)`, średnia cena ważona:
   `SUM(avg_ticket_price * flight_count) / SUM(flight_count)`;
2. zewnętrzny SELECT – funkcje okienkowe:
   - `RANK() OVER (PARTITION BY continent ORDER BY total_flights DESC)` – ranking 1,2,2,4,
   - `total_flights - AVG(total_flights) OVER (PARTITION BY continent)` – odchylenie.

**Kluczowe pytania:**
- *Dlaczego średnia ważona, a nie `AVG(avg_ticket_price)`?* – Bo grupy (lotnisko×miesiąc) mają różną
  liczbę lotów; średnia ze średnich dałaby zły wynik. `avg × count` odtwarza sumę cen.
- *Dlaczego `GROUP BY continent, country`, a nie samo `country`?* – Ranking jest w ramach kontynentu,
  a w danych ten sam kraj może występować na różnych kontynentach.
- *Dlaczego `RANK`, a nie `ROW_NUMBER`/`DENSE_RANK`?* – Specyfikacja: równe wartości mają tę samą
  pozycję, a kolejna jest pomijana (1, 2, 2, 4) – to dokładnie `RANK()`.
- *Dlaczego `EXTERNAL TABLE ... LOCATION`, a nie `LOAD DATA`?* – `LOAD DATA` przenosi pliki; external
  table tylko wskazuje dane tam, gdzie leżą (szybciej, nie niszczy źródła, `DROP` nie usuwa danych).
- *JOIN wewnętrzny?* – Tak; lot z lotniska spoza słownika nie ma kraju/kontynentu, więc nie może
  trafić do zestawienia krajów.
- *JSON:* tworzy go `org.apache.hadoop.hive.serde2.JsonSerDe` (wbudowany w Hive 3/4), nie ręczne `CONCAT`.
- *Uruchomienie:* `run_hive.sh input_dir3 input_dir4 output_dir6` →
  `beeline -u jdbc:hive2://localhost:10000/default --hivevar ... -f hive.hql`
  (beeline, bo stary klient `hive` jest przestarzały / usunięty w Hive 4).

## 5. Airflow (`projekt1.py`)

Zmieniłem tylko to, co trzeba:
- `classic_or_streaming` domyślnie = `classic`,
- operator `mapreduce_classic`: `hadoop jar {{dags_home}}/project_files/flights-airports.jar flights.FlightsAirports {{input_dir}}/datasource1 {{output_mr_dir}}`,
- operator `hive`: `beeline ... --hivevar input_dir3={{output_mr_dir}} input_dir4={{input_dir}}/datasource4 output_dir6={{output_dir}} -f .../hive.hql`.

Operatory czyszczące, wybór wariantu, `get_output` i zależności – bez zmian.
Operator nie woła `run_mr.sh`, tylko zawiera to samo polecenie (wymóg kryterium W02).
Parametr `input_dir` wskazuje katalog `input` zawierający `datasource1/` i `datasource4/`.

## 6. Wyniki kontrolne

- MapReduce: **1300** wierszy; na zbiorze testowym `test2026-10`:
  `BGKA,2026-10,12,960.279,107.333` – zgodne z `expected_results-2026-10.md`.
- Hive: zbiór testowy → **96** wierszy, np.
  `Africa, American Samoa, 526, 780.188, rank 5, deviation 20.25` – zgodne z oczekiwanym.
  Na pełnym zbiorze z kursu: 95 wierszy.

## 7. Środowisko GCP

- Projekt `big-data-2026-10-sw`, bucket `gs://pbd-26-10-sw/projekt1/` (dane + pliki projektu).
- Klaster Dataproc `pbd-cluster`, europe-west4, obraz `3.0-debian13` (Hadoop 3.5, Hive 4 na Tez),
  `--max-age=3h` (auto-usunięcie).
- Dane z bucketu kopiowane do HDFS: `hadoop fs -cp gs://pbd-26-10-sw/projekt1/input /tmp/`
  → w Airflow `input_dir=/tmp/input`.
