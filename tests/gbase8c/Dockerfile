# The local base supplies only an ARM64 Linux userland and libraries.
# Actual database executables below are the separately verified GBase vendor package.
FROM opengauss/opengauss-server:latest
USER root
COPY --chown=70:70 kernel/ /opt/gbase8c/
RUN mkdir -p /var/lib/gbase8c && chown 70:70 /var/lib/gbase8c
ENV GAUSSHOME=/opt/gbase8c GBASEHOME=/opt/gbase8c GAUSSLOG=/var/lib/gbase8c/log
ENV PATH=/opt/gbase8c/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/gbase8c/lib:/opt/gbase8c/lib/3rd
USER 70:70
ENTRYPOINT ["/bin/bash", "/fixture/start.sh"]
