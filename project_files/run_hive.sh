#!/bin/bash
# Użycie: run_hive.sh input_dir3 input_dir4 output_dir6
set -e

if [ $# -ne 3 ]; then
    echo "Usage: $0 input_dir3 input_dir4 output_dir6" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

beeline -u jdbc:hive2://localhost:10000/default \
    --hivevar input_dir3="$1" \
    --hivevar input_dir4="$2" \
    --hivevar output_dir6="$3" \
    -f "$SCRIPT_DIR/hive.hql"
