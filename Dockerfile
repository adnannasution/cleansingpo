FROM node:20

# Install Python + venv
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip

WORKDIR /app
COPY . .

# Python venv
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Node deps
RUN npm install

# Railway uses PORT env
ENV PYTHON_PORT=5001

# Expose Railway port
EXPOSE 3000
EXPOSE 5001

# Start Python first, then Node
CMD sh -c "python python/worker.py & sleep 5 && node server.js"