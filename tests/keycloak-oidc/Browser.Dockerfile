FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends chromium libnss3-tools \
    && rm -rf /var/lib/apt/lists/*
RUN printf '%s\n' '#!/bin/sh' 'exec /usr/bin/chromium --no-sandbox --disable-dev-shm-usage "$@"' > /usr/local/bin/fixture-chromium \
    && chmod 0755 /usr/local/bin/fixture-chromium
USER node
