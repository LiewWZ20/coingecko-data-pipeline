FROM apache/airflow:3.2.1

USER root

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# set working directory
WORKDIR /opt/airflow

# copy dependency files
COPY pyproject.toml uv.lock ./

# install dependencies into the system python (not a venv)
# --frozen = use exact versions from uv.lock
# --no-dev = skip dev dependencies like pytest, sqlfluff
# --system = install into system python, not a new venv
# RUN uv sync --frozen --no-dev --system --python $(which python)
RUN uv export --no-dev --frozen --format requirements-txt > requirements.txt && \
    uv pip install --system -r requirements.txt

USER airflow