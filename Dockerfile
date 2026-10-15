# syntax=docker/dockerfile:1
# (heredocs + --mount=type=secret need the BuildKit dockerfile frontend)
FROM python:3.14.5-bookworm AS usage_client_downloader
# usage_client (shared lib, private repo deepriver-ai/usage_client): fetched with its own
# read-only deploy key passed as a BuildKit secret, so the key never lands in the image:
#   docker build --secret id=gh-ssh-usage-client,src=$HOME/.ssh/usage_client_deploy \
#                --build-arg USAGE_CLIENT_VERSION=<tag> ...
# (rate_limiting.md §3.4 step 3, Q26). Keep the tag the same across services.
ARG USAGE_CLIENT_VERSION="v0.2.1"
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client && rm -rf /var/lib/apt/lists/*
COPY <<EOF /root/.ssh/config
Host github.com-usage_client
        HostName github.com
        User git
        IdentitiesOnly yes
        IdentityFile /run/secrets/gh-ssh-usage-client
EOF
COPY <<EOF /root/.ssh/known_hosts
github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
EOF
WORKDIR /usage_client
RUN --mount=type=secret,required=true,id=gh-ssh-usage-client \
    test -n "$USAGE_CLIENT_VERSION" && \
    pip download --no-deps --dest /usage_client "usage-client[apify] @ git+ssh://git@github.com-usage_client/deepriver-ai/usage_client.git@${USAGE_CLIENT_VERSION}"

FROM python:3.14.5-bookworm AS elastic_client_downloader
# elastic_client (private repo deepriver-ai/elastic_client), same deploy key and pin as
# social_tags: docker build --secret id=gh-ssh-priv,src=<elastic_client deploy key> ...
# ELASTIC_CLIENT_VERSION: keep in sync with social_tags' Dockerfile.
ARG ELASTIC_CLIENT_VERSION="fdd01166a603080f466f2be9bab8dfc2a2765ee9"
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client && rm -rf /var/lib/apt/lists/*
COPY <<EOF /root/.ssh/config
Host github.com-elastic_client
        HostName github.com
        User git
        IdentitiesOnly yes
        IdentityFile /run/secrets/gh-ssh-priv
EOF
COPY <<EOF /root/.ssh/known_hosts
github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
EOF
WORKDIR /elastic_client
RUN --mount=type=secret,required=true,id=gh-ssh-priv pip download --no-deps --dest /elastic_client git+ssh://git@github.com-elastic_client/deepriver-ai/elastic_client.git@${ELASTIC_CLIENT_VERSION}

FROM python:3.14.5-bookworm
RUN apt update && apt install -y python3-lxml && rm -rf /var/lib/apt/lists/*
RUN addgroup --gid 1000 ejecutor && adduser --ingroup ejecutor --uid 1000 --disabled-login ejecutor
USER 1000:1000
WORKDIR /apify_client
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY --from=usage_client_downloader /usage_client /usage_client
RUN pip install --find-links=/usage_client "usage-client[apify]"
COPY --from=elastic_client_downloader /elastic_client /elastic_client
RUN pip install --no-deps --find-links=/elastic_client/ elastic_client
# usage_client's JSONL spool: mount a persistent volume here in k8s (rate_limiting.md §3.4 step 5).
ENV USAGE_SPOOL_DIR=/apify_client/cache/usage_spool
ADD ./src ./src
ADD tasks.xlsx tasks.xlsx
# The deployed service is the crawl-run queue worker (CRAWL_RUNS_QUEUE). Hand runs of the
# xlsx crawler: docker run --entrypoint python <image> -u -m src.run_searches ...
ENTRYPOINT [ "python", "-u", "-m", "src.run_crawl_worker" ]