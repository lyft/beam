FROM lyft/pythonlibrary:da49cb1d41a1bce9c284e5c8238eb3af0872c433
# add source to a different directory, the code root and the setup.py root
# are at different locations than lyft conventions expect
WORKDIR /src/beam
COPY . .
# Add a symlink to the setup.py code root at the lyft conventional root
RUN ln -sf /src/beam/sdks/python /code/beam
RUN \
    cp \
        /code/containers/pythonlibrary/Makefile \
        /src/beam/manifest.yaml \
        /code/beam/
