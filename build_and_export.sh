#!/bin/bash
echo "Building image..."
docker build -t frlbot:latest .
echo "Exporting image..."
docker save frlbot:latest | gzip > frlbot_latest.tar.gz