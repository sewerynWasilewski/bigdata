#!/bin/bash
# Użycie: run_mr.sh input_dir1 output_dir3
set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 input_dir1 output_dir3" >&2
    exit 1
fi

INPUT_DIR1="$1"
OUTPUT_DIR3="$2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# usunięcie wyniku poprzedniego uruchomienia (jeśli istnieje)
hadoop fs -rm -r -f "$OUTPUT_DIR3"

hadoop jar "$SCRIPT_DIR/flights-airports.jar" flights.FlightsAirports "$INPUT_DIR1" "$OUTPUT_DIR3"
