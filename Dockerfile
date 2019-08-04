FROM lyft/pythonlibrary:66d815e6ba03c9515c0c2273ecca13036015715d
# add source to a different directory, the code root and the setup.py root
# are at different locations than lyft conventions expect
WORKDIR /src/beam
COPY . .
# Add a symlink to the setup.py code root at the lyft conventional root
RUN ln -sf /src/beam/sdks/python /code/beam
# Install the same version of cython required by beam build
RUN pip install "cython==0.28.1"
RUN \
    cp \
        /code/containers/pythonlibrary/Makefile \
        /src/beam/manifest.yaml \
        /code/beam/
