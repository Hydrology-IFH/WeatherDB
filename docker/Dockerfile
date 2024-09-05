# Use the official Python base image
FROM --platform=linux/amd64 python:3.9-rc-slim-buster as pythonbaseimage

# update pip
RUN python -m pip install --upgrade pip

WORKDIR /src

# get the pyproject.toml file
COPY ../pyproject.toml ./pyproject.toml

# Create a requirements.txt file from pyproject.toml
RUN pip install toml
RUN python -c "import toml; pyproject = toml.load('pyproject.toml'); deps = pyproject['project']['dependencies'];\
open('/src/requirements.txt', 'w').write('\\n'.join(deps))"

# Install the dependencies
RUN pip install -r requirements.txt

# Copy the source code
COPY ../README.md ./README.md
COPY ../MANIFEST.in ./MANIFEST.in
COPY ../weatherDB ./weatherDB
RUN pip install . --upgrade --upgrade-strategy only-if-needed --no-deps

# Copy the tests
COPY ../tests ./tests
