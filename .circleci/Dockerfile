FROM jaredallard/triton-base:ubuntu

COPY --chown=999:999 package.json /stack
RUN yarn --production=true

COPY --chown=999:999 . /stack
