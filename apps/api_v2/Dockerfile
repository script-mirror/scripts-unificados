FROM python:3.10

WORKDIR /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2

COPY . .

RUN pip install --no-cache-dir --upgrade -r  /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2/requirements.txt

EXPOSE 8000

CMD ["python3", "./main.py"]
