FROM node:20

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app
COPY . .

# Install dependencies
RUN pip install -r requirements.txt
RUN npm install

# Set environment
ENV PYTHON_PORT=5001
ENV NODE_PORT=3000

# Expose ports
EXPOSE 3000
EXPOSE 5001

# Run Python worker + Node server
CMD python3 python/worker.py & node server.js