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

# Define o comando padrão (para debug, você pode mudar depois)
CMD ["python", "producer.py"]