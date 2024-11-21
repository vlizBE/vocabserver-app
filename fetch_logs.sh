#!/bin/bash

stack_name="vocabserver-app"
output_dir="container_logs"

mkdir -p $output_dir

for container in $(docker ps --filter "label=com.docker.compose.project=$stack_name" --format "{{.Names}}"); do
    docker logs --timestamps $container > $output_dir/$container-$(date +%Y-%m-%d).log 2>&1
done
