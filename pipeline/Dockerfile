#Bước đóng gói (containerize): Containerize = code + environment + dependencies
#File Dockerfile này dùng uv

# base Docker image that we will build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# set up our image by installing prerequisites; pandas in this case
RUN pip install pandas pyarrow

# set up the working directory inside the container
WORKDIR /code
# copy the script to the container. 1st name is source file, 2nd is destination
COPY pipeline.py pipeline.py

# Set entrypoint
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]