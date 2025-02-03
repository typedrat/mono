FROM litestream/litestream:0.3.13 as litestream
FROM node:22.11.0-alpine3.20

# No ZERO_VERSION arg needed since we're using local tarball

RUN apk add --update curl

RUN mkdir -p /opt/app
WORKDIR /opt/app

# Copy the tarball from the build context
COPY ./prod/zbugs/rocicorp-zero-*.tgz .
# Install from local tarball instead of npm
RUN npm install -g ./rocicorp-zero-*.tgz

# Copy litestream executable and config.yml
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
RUN cp /usr/local/lib/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_LOG_LEVEL=debug
ENV ZERO_LOG_FORMAT=json

EXPOSE 4848 4849
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["npx zero-cache"]