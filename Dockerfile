FROM node:20-slim

# Install Python + pip
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip3 install --break-system-packages -r requirements.txt

# Node deps
COPY package.json .
RUN npm install --production

# App files
COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
