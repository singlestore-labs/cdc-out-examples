FROM python:3.11-bookworm

RUN mkdir /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD}

# Install poetry
RUN pip3 install poetry
RUN poetry config virtualenvs.create false

# Copy the code
COPY . .

# Install deps
RUN poetry install --no-dev --no-root

# Proxy commands to main.py within poetry environment
ENTRYPOINT ["poetry", "run", "python", "main.py"]