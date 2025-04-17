# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

FROM 015137877991.dkr.ecr.us-east-1.amazonaws.com/python:slim as runtime

RUN set -eux; \
    apt-get update; \
    apt-get install -y bash \
    libpq-dev \
    ; \
    rm -rf /var/lib/apt/lists/*

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

#SAIC SSL Decryption (local build only)
ENV CA_CERTIFICATES_DIR=/usr/local/share/ca-certificates
COPY pypi.pem $CA_CERTIFICATES_DIR/
COPY pip.conf /etc/pip.conf
ENV NODE_EXTRA_CA_CERTS=$CA_CERTIFICATES_DIR/pypi.pem
ENV REQUESTS_CA_BUNDLE=$CA_CERTIFICATES_DIR/pypi.pem

# Configure Poetry
ENV POETRY_VERSION=1.7.1
ENV POETRY_HOME=/app/poetry
ENV POETRY_VENV=/app/poetry-venv
ENV POETRY_CACHE_DIR=/app/.cache
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Install poetry separated from system interpreter
RUN python3 -m venv $POETRY_VENV && \
    $POETRY_VENV/bin/pip install -U pip setuptools && \
    $POETRY_VENV/bin/pip install poetry==$POETRY_VERSION

WORKDIR /app

COPY poetry.lock pyproject.toml ./

#RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR
RUN poetry install --without dev --no-root

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
#RUN --mount=type=cache,target=/root/.cache/pip \
#    --mount=type=bind,source=chalicelib/requirements.txt,target=requirements.txt \
#    python -m pip install -r requirements.txt

# Copy the source code into the container.
COPY . ./

# Expose the port that the application listens on.
EXPOSE 5000

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

RUN chown -R 10001:10001 ./

# Switch to the non-privileged user to run the application.
USER appuser

# Run the application.
CMD ["poetry", "run", "python3", "__init__.py", "--host", "0.0.0.0", "--port", "5000"]
