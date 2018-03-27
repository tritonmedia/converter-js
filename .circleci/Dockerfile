FROM jaredallard/triton-base:ubuntu

ENTRYPOINT [ "/trickle-me.sh" ]
COPY --chown=999:999 .circleci/trickle-me.sh /trickle-me.sh
RUN chmod +x /trickle-me.sh

COPY --chown=999:999 package.json /stack
RUN yarn

COPY --chown=999:999 . /stack
