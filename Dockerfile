FROM lyft/pythonlibrary:aade5cd71241b7e490cd9d576154a013298a0892
RUN : \
    && apt-get update \
    && apt-get install -y \
        python3.6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN : \
    && virtualenv -ppython2.7 /code/venvs/venv2 \
    && virtualenv -ppython3.6 /code/venvs/venv3 \
    && /code/venvs/venv2/bin/pip install "pip==9.0.0" "setuptools==39.0.1" "cython==0.28.1" "wheel" \
    && /code/venvs/venv3/bin/pip install "pip==9.0.3" "setuptools==39.0.1" "cython==0.28.1" "wheel"
ENV PATH=/code/venvs/venv2/bin:/code/venvs/venv3/bin:$PATH
# add source to a different directory, the code root and the setup.py root
# are at different locations than lyft conventions expect
WORKDIR /src/beam
COPY . .
# Add a symlink to the setup.py code root at the lyft conventional root
RUN ln -sf /src/beam/sdks/python /code/beam
RUN \
    cp \
        /src/beam/manifest.yaml \
        /code/beam/
