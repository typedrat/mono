#!/bin/bash

set -e

# Get the number of files to download, default to 6 if not specified
NUM_FILES=${1:-6}

# Ensure NUM_FILES is not greater than 400
if [ "$NUM_FILES" -gt 400 ]; then
    echo "Number of files cannot exceed 400. Setting to 400."
    NUM_FILES=400
fi

BASE_URL="https://rocinante-dev.s3.us-east-1.amazonaws.com"

# Define base files that should always be downloaded
BASE_FILES=(
    "comments.csv"
    "issue_labels.csv"
    "issues.csv"
    "labels.csv"
    "users.csv"
)

# Generate numbered file lists based on NUM_FILES
NUMBERED_FILES=()
for i in $(seq 1 $NUM_FILES); do
    NUMBERED_FILES+=("comments_${i}.csv")
    NUMBERED_FILES+=("issue_labels_${i}.csv")
    NUMBERED_FILES+=("issues_${i}.csv")
done

# Combine all files into one array
FILES=("${BASE_FILES[@]}" "${NUMBERED_FILES[@]}")

mkdir -p data/1gb

for file in "${FILES[@]}"; do
    echo "Downloading $file..."
    curl -L -o "data/1gb/$file" "$BASE_URL/$file"
done

echo "All files downloaded successfully!"
