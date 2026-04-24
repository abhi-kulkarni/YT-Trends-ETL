#!/bin/bash

set -euo pipefail

BUCKET="s3://de-youtubedata-raw-useast1-dev/youtube"
RAW_PATH="$BUCKET/raw_statistics"
REF_PATH="$BUCKET/raw_statistics_reference_data"

REGIONS=(ca de fr gb in jp kr mx ru us)

PARALLEL_JOBS=4 
DRY_RUN=false     # set to true for testing


log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

run_cmd() {
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY-RUN] $*"
    else
        eval "$@"
    fi
}


upload_reference_data() {
    log "Uploading JSON reference data..."

    run_cmd aws s3 cp ./reference_data "$REF_PATH/" \
        --recursive \
        --exclude "*" \
        --include "*.json"

    log "Reference data upload complete"
}


upload_csvs() {
    log "Uploading regional CSV files..."

    for region in "${REGIONS[@]}"; do
        (
        FILE="$(echo $region | tr '[:lower:]' '[:upper:]')videos.csv"

        if [[ -f "$FILE" ]]; then
            log "Uploading $FILE → region=$region"
            run_cmd aws s3 cp "$FILE" "$RAW_PATH/region=$region/"
        else
            log "File not found: $FILE"
        fi
        ) &

        # Limit parallel jobs
        if (( $(jobs -r | wc -l) >= PARALLEL_JOBS )); then
        wait -n
        fi
    done

    wait
    log "CSV upload complete"
}

main() {
    log "Starting upload process..."

    upload_reference_data
    upload_csvs

    log "All uploads completed successfully"
}

main "$@"
