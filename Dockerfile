# Imagem base do Python
FROM python:3.11-slim

# Define diretório de trabalho
WORKDIR /app

# Copia os arquivos de dependências
COPY requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Define o comando padrão: DummyApp + JSON server
CMD ["sh", "-c", "python application.py & python json_server.py & tail -f /dev/null"]