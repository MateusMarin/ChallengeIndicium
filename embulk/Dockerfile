FROM alpine:edge as yaml

RUN apk update & apk add yq jq
FROM fablic/embulk

COPY --from=yaml /usr/bin/yq /usr/bin/yq
RUN --mount=type=bind,src=./requirements.txt,dst=/tmp/requirements.txt \
    embulk gem install $(cat /tmp/requirements.txt|tr '\n' ' ')

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x entrypoint.sh \
    && mkdir /embulk

WORKDIR /embulk

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["--version"]