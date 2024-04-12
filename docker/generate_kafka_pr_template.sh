#!/usr/bin/env bash

# Ensure script exits on error or unset variable
set -eu

# Define the 'self' variable with the script's basename
self="$(basename "$BASH_SOURCE")"

# Navigate to the script's directory and then to docker_official_images
cd "$(dirname "$(readlink -f "$BASH_SOURCE")")/docker_official_images"

# Source common utilities
source ../common.sh

# Initialize an empty variable for the highest version
highest_version=""

# Output header information
cat <<-EOH
# This file is generated via https://github.com/apache/kafka/blob/$(fileCommit "../$self")/docker/generate_kafka_pr_template.sh

Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)
GitRepo: https://github.com/apache/kafka.git
EOH

# Find all versions, excluding -rc, sort them, and determine the globally highest version
versions=$(find . -mindepth 1 -maxdepth 1 -type d ! -name "*-rc" | sort -V)
for dir in $versions; do
    version=$(basename "$dir")
    highest_version="$version" # Continuously update to ensure the last is the highest
done

# Process each non-RC version
for dir in $versions; do
    version=$(basename "$dir")
    # Determine the major.minor for the current and highest version
    major_minor_version=$(echo "$version" | cut -d'.' -f1-2)
    highest_major_minor=$(echo "$highest_version" | cut -d'.' -f1-2)

    # Check if the current version is the latest in its major.minor series
    latest_in_series=$(find . -mindepth 1 -maxdepth 1 -type d -name "$major_minor_version.*" ! -name "*-rc" | sort -V | tail -n 1)
    latest_in_series_basename=$(basename "$latest_in_series")

    # Tags and latest tag determination
    tags="$version"
    if [[ "$version" == "$latest_in_series_basename" && "$major_minor_version" == "$highest_major_minor" ]]; then
        tags+=", latest"
    fi

    commit="$(dirCommit "$dir/jvm")"

    # Output image information
    echo
    cat <<-EOE
Tags: $tags
Architectures: amd64,arm64v8
GitCommit: $commit
Directory: ./docker/docker_official_images/$version/jvm
EOE
done

