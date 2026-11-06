# Projekt 1 – Zestaw 11 flights-airports (MapReduce Classic) – kroki wykonania

Dokument opisuje po kolei wszystko, co zostało zrobione, tak aby dało się to odtworzyć.
Wyjaśnienie „co i dlaczego” jest w `NOTATKI_DO_OBRONY.md`.

## 0. Struktura katalogu projektu

```
bigdata/
├── mapreduce/                      # źródła programu MapReduce (Maven)
│   ├── pom.xml
│   └── src/main/java/flights/
│       ├── FlightsAirports.java    # driver (konfiguracja Job)
│       ├── FlightsMapper.java
│       ├── FlightStatsWritable.java
│       ├── FlightStatsCombiner.java
│       └── FlightsReducer.java
├── project_files/                  # to, co trafia do ~/airflow/dags/project_files
│   ├── flights-airports.jar
│   ├── run_mr.sh
│   ├── hive.hql
│   └── run_hive.sh
├── projekt1.py                     # DAG Airflow (trafia do ~/airflow/dags)
├── wyniki/
│   ├── output6.json                # pełny wynik końcowy (95 rekordów JSON)
│   └── airflow_get_output_run1.log # log tasku get_output z Airflow
├── NOTATKI_DO_OBRONY.md
└── KROKI_WYKONANIA.md              # ten plik
```

## 1. Zebranie wymagań

Źródła:
- opis celu przetwarzania: https://github.com/BigDataBatchProcessing/ProjectMapReduceHive → `Zestaw 11 - flights-airports/README.md`
- opis danych: https://github.com/BigDataBatchProcessing/ProjectDataSets → `Zestaw 11 - flights-airports/README.md`
- szablon DAG: `projekt1.py` z repo ProjectMapReduceHive
- wyniki kontrolne: `expected_results-2026-10.md` z repo ProjectMapReduceHive (1300 wierszy MR, 96 wierszy Hive dla zbioru testowego)
- instrukcja Airflow na GCP: `PBD06_w1_25-Apache-Airflow-projekt1-zadania-gcp.pdf`

## 2. Pobranie danych (lokalnie)

```bash
mkdir data && cd data
curl -O https://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekty/zestaw11.zip
unzip zestaw11.zip          # -> input/datasource1/flights00..99.csv, input/datasource4/airports.csv

# zbiór testowy do porównania z expected_results-2026-10.md
mkdir ../testdata && cd ../testdata
curl -O https://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekty/test2026-10/zestaw11.zip
unzip zestaw11.zip
```

Obserwacje z analizy danych:
- flights: 100 plików, 1 000 000 rekordów, każdy plik ma nagłówek, brak cudzysłowów, zawsze 10 pól;
- statusy: On time / Delayed / Cancelled (~po 1/3);
- airports: 100 lotnisk, **pola w cudzysłowach zawierają przecinki** → w Hive `OpenCSVSerde`.

## 3. Program MapReduce – budowa i test lokalny

Wymagane: JDK (≥ 8) i Maven.

```bash
cd mapreduce
mvn -q -B package                         # -> target/flights-airports.jar
cp target/flights-airports.jar ../project_files/
```

Test lokalny (Hadoop w trybie standalone, np. `brew install hadoop`):

```bash
project_files/run_mr.sh data/input/datasource1 out_mr3
project_files/run_mr.sh data/input/datasource1 out_mr3     # 2. raz – test powtarzalności
cat out_mr3/part-r-* | wc -l                                # 1300

project_files/run_mr.sh testdata/input/datasource1 test_out_mr3
grep '^BGKA,2026-10' test_out_mr3/part-r-*                  # 12, 960.279..., 107.333... = expected
```

Logika Hive została sprawdzona skryptem w Pythonie na zbiorze testowym:
96 wierszy, `Africa / American Samoa / 526 / 780.188 / 5 / 20.25` – zgodne z expected.

## 4. Środowisko GCP

Projekt GCP: **`big-data-2026-10-sw`** (konto `seweryn.wasilewski@gmail.com`), region **europe-west4**,
bucket **`gs://pbd-26-10-sw`**.

> Uwaga: domyślny projekt w lokalnym `gcloud` to `n-family` – dlatego w poleceniach
> zawsze podawany jest jawnie `--project big-data-2026-10-sw`
> (albo najpierw `gcloud config set project big-data-2026-10-sw`).

### 4.1 Logowanie

```bash
gcloud auth login
gcloud projects list
```

### 4.2 Wgranie danych i plików projektu do bucketu

```bash
gcloud storage cp -r data/input gs://pbd-26-10-sw/projekt1/ --project big-data-2026-10-sw
gcloud storage cp project_files/* projekt1.py gs://pbd-26-10-sw/projekt1/project_files/ --project big-data-2026-10-sw
```

### 4.3 Utworzenie klastra Dataproc (polecenie z kursu)

```bash
export REGION=europe-west4
export CLUSTER_NAME=pbd-cluster
export PROJECT_ID=big-data-2026-10-sw

gcloud dataproc clusters create ${CLUSTER_NAME} --enable-component-gateway \
  --region ${REGION} --subnet default --public-ip-address \
  --master-machine-type n2-standard-4 --master-boot-disk-size 50 \
  --num-workers 2 --worker-machine-type n2-standard-2 --worker-boot-disk-size 50 \
  --initialization-actions=gs://pbd-init/v1/fix-deprecated-props.sh \
  --image-version 3.0-debian13 \
  --project ${PROJECT_ID} --max-age=3h
```

`--max-age=3h` → klaster sam się usuwa po 3 godzinach (sprawdzenie:
`gcloud dataproc clusters describe pbd-cluster --region europe-west4 --project big-data-2026-10-sw --format="value(config.lifecycleConfig.autoDeleteTime)"`).

Obraz 3.0-debian13: Hadoop 3.5, Hive 4 (silnik Tez), Python 3.13.

### 4.4 Połączenie z masterem

```bash
gcloud compute ssh pbd-cluster-m --zone europe-west4-a --project big-data-2026-10-sw
```

(lub przycisk SSH w konsoli GCP → Compute Engine → VM instances → `pbd-cluster-m`)

## 5. Na masterze: pliki projektu i dane w HDFS

```bash
mkdir -p ~/project_files
gsutil cp gs://pbd-26-10-sw/projekt1/project_files/* ~/project_files/
chmod +x ~/project_files/*.sh

# dane źródłowe do HDFS (przygotowanie danych NIE jest częścią przepływu Airflow)
hadoop fs -mkdir -p /tmp
hadoop fs -cp gs://pbd-26-10-sw/projekt1/input /tmp/
hadoop fs -ls /tmp/input          # datasource1, datasource4
```

## 6. Test skryptów na klastrze

```bash
# MapReduce (2x – powtarzalność), ~6 min na uruchomienie
~/project_files/run_mr.sh /tmp/input/datasource1 /project1/output_mr3
~/project_files/run_mr.sh /tmp/input/datasource1 /project1/output_mr3
hadoop fs -cat /project1/output_mr3/part-r-* | wc -l     # 1300

# Hive (2x – powtarzalność)
~/project_files/run_hive.sh /project1/output_mr3 /tmp/input/datasource4 /project1/output6
~/project_files/run_hive.sh /project1/output_mr3 /tmp/input/datasource4 /project1/output6
hadoop fs -cat /project1/output6/* | wc -l               # 95
hadoop fs -cat /project1/output6/* | head
```

Wynik Hive porównany z obliczeniem referencyjnym w Pythonie: 95/95 wierszy zgodnych.

## 7. Apache Airflow na masterze

### 7.1 Instalacja (wg warsztatu)

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=3.1.0
export AIRFLOW__CORE__LOAD_EXAMPLES=False
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
export PATH=$PATH:~/.local/bin

curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
source ~/.venv/bin/activate
airflow db migrate
sed -i 's/^refresh_interval\s*=.*/refresh_interval = 15/' ~/airflow/airflow.cfg
```

### 7.2 Wgranie przepływu i plików projektu

```bash
mkdir -p ~/airflow/dags/project_files
cp ~/project_files/projekt1.py ~/airflow/dags/
cp ~/project_files/flights-airports.jar ~/project_files/hive.hql \
   ~/project_files/run_mr.sh ~/project_files/run_hive.sh ~/airflow/dags/project_files/
```

Zmiany w `projekt1.py` względem szablonu:
- `classic_or_streaming` → domyślnie `"classic"`,
- `mapreduce_classic`:
  `hadoop jar {{ params.dags_home }}/project_files/flights-airports.jar flights.FlightsAirports {{ params.input_dir }}/datasource1 {{ params.output_mr_dir }}`
- `hive`:
  `beeline -u jdbc:hive2://localhost:10000/default --hivevar input_dir3={{ params.output_mr_dir }} --hivevar input_dir4={{ params.input_dir }}/datasource4 --hivevar output_dir6={{ params.output_dir }} -f {{ params.dags_home }}/project_files/hive.hql`
- reszta (operatory czyszczące, branch, get_output, zależności) bez zmian.

### 7.3 Uruchomienie Airflow

W warsztacie: `airflow standalone` w otwartym terminalu (hasło admina w logu lub
`head ~/airflow/simple_auth_manager_passwords.json.generated`).
Tutaj uruchomione w tle:

```bash
nohup bash -c "source ~/.venv/bin/activate && AIRFLOW_HOME=~/airflow AIRFLOW__CORE__LOAD_EXAMPLES=False airflow standalone" \
  > ~/airflow_standalone.log 2>&1 &
```

### 7.4 Dostęp do interfejsu webowego (z własnego komputera)

Tunel SSH do portu 8080 mastera:

```bash
gcloud compute ssh pbd-cluster-m --zone europe-west4-a --project big-data-2026-10-sw -- -L 8080:localhost:8080
```

potem w przeglądarce: http://localhost:8080 (login `admin`, hasło z pliku powyżej).

### 7.5 Uruchomienie przepływu

W UI: DAG `project1-workflow` → **Trigger** → ustawić parametry:
- `dags_home` = `/home/<użytkownik>/airflow/dags` (u mnie `/home/tensw062/airflow/dags`)
- `input_dir` = `/tmp/input`
- `output_mr_dir` = `/project1/output_mr3` (domyślne)
- `output_dir` = `/project1/output6` (domyślne)
- `classic_or_streaming` = `classic` (domyślne)

Alternatywnie z CLI (tak zrobiono test):

```bash
airflow dags unpause project1-workflow
airflow dags trigger project1-workflow \
  --conf '{"dags_home": "/home/tensw062/airflow/dags", "input_dir": "/tmp/input"}'
airflow dags list-runs project1-workflow
```

Wynik: 2 uruchomienia, oba **success** (clean_* → pick → mapreduce_classic → hive → get_output;
`hadoop_streaming` = skipped). Czas jednego przebiegu ~7 min (MR ~6 min, Hive ~30 s).

Log tasku `get_output`:
`~/airflow/logs/dag_id=project1-workflow/run_id=<run>/task_id=get_output/attempt=1.log`
(kopia lokalnie: `wyniki/airflow_get_output_run1.log`).

## 8. Pobranie wyników na swój komputer

```bash
gcloud compute ssh pbd-cluster-m --zone europe-west4-a --project big-data-2026-10-sw \
  --command 'hadoop fs -cat /project1/output6/*' > wyniki/output6.json
```

## 9. Sprzątanie

Klaster usuwa się sam po 3 h (`--max-age`). Ręcznie:

```bash
gcloud dataproc clusters delete pbd-cluster --region europe-west4 --project big-data-2026-10-sw
```

Dane w `gs://pbd-26-10-sw/projekt1/` zostają (koszt pomijalny) – przy kolejnym klastrze
wystarczy wykonać kroki 4.3 → 5 → 7.
