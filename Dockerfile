# Keep in sync with .python-version
FROM python:3.11.5-slim

WORKDIR /dbt-bouncer

COPY dist/dbt-bouncer.pex ./dbt-bouncer.pex

ENTRYPOINT [ "/dbt-bouncer/dbt-bouncer.pex" ]
