#!/bin/bash

echo "Iniciando container Airflow MySQL..."
docker-compose -f /WX2TB/Documentos/fontes/PMO/scripts_unificados/server_configuration/containers/airflow_mysql/docker-compose.yaml up -d

echo "Iniciando container Nginx..."
docker-compose -f /WX2TB/Documentos/fontes/PMO/scripts_unificados/server_configuration/containers/nginx/docker-compose-nginx.yaml up -d

echo "Iniciando container OpenTelemetry Collector..."
docker-compose -f /WX2TB/Documentos/fontes/PMO/scripts_unificados/server_configuration/containers/openTelemetryCollector/docker-compose.yml up -d

echo "Iniciando container SFTP..."
docker-compose -f /WX2TB/Documentos/fontes/PMO/scripts_unificados/server_configuration/containers/sftp/docker-compose.yml up -d

echo "Iniciando container do aplicativo personalizado..."
docker-compose -f /home/admin/jose/app/docker-compose.yaml up -d

echo "Iniciando container API v2..."
docker-compose -f /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2/docker-compose.yaml up -d