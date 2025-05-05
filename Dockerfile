# Usa uma imagem base com Python
FROM python:3.10-slim

# Define diretório de trabalho no container
WORKDIR /app

# Copia arquivos para o container
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto do código
COPY ./src ./src
COPY ./data ./data

# Define o comando padrão
CMD ["python", "src/etl.py"]
