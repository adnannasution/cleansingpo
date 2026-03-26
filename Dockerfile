FROM node:20

# Install Python
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip

WORKDIR /app
COPY . .

# Python virtual env
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Node deps
RUN npm install

# Expose Railway port
EXPOSE 3000

# Start Python (gunicorn) + Node
CMD sh -c "gunicorn -w 2 -b 0.0.0.0:5001 python.worker:app & node server.js"