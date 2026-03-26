FROM node:20

# Install Python + venv
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip

WORKDIR /app
COPY . .

# Create virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Node deps
RUN npm install

# Set env
ENV PYTHON_PORT=5001

# Expose port Railway
EXPOSE 3000
EXPOSE 5001

# Start Python + Node
CMD sh -c "python python/worker.py & node server.js"