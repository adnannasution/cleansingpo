FROM node:20

# Install Python + venv
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip

WORKDIR /app
COPY . .

# Create virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Node dependencies
RUN npm install

# Environment
ENV PYTHON_PORT=5001
ENV NODE_PORT=3000

# Expose ports
EXPOSE 3000
EXPOSE 5001

# Run Python + Node
CMD python python/worker.py & node server.js