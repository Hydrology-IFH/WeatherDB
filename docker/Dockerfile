# Use the official Python base image
FROM --platform=linux/amd64 python:3.10-rc-slim-buster as pythonbaseimage

# update pip and install git
RUN pip install pip --upgrade && \
    apt-get update && \
    apt-get install git -y

WORKDIR /src

# get the pyproject.toml file
COPY ../pyproject.toml ./pyproject.toml

# install dependencies
RUN pip install toml && \
    python -c "import toml; pyproject = toml.load('pyproject.toml');\
        deps = pyproject['project']['dependencies'] \
            + pyproject['project']['optional-dependencies']['optionals'] \
            + pyproject['build-system']['requires'];\
        open('/src/requirements.txt', 'w').write('\\n'.join(deps))" &&\
    pip install -r requirements.txt --root-user-action=ignore

# Copy the source code
COPY ../README.md ../MANIFEST.in  ./
COPY ../weatherDB ./weatherDB
RUN --mount=source=../.git,target=/src/.git,type=bind \
    pip install . --upgrade --upgrade-strategy only-if-needed --no-deps --no-cache-dir --root-user-action=ignore

# Copy the tests
COPY ../tests ./tests
