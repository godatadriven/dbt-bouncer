# Keep in sync with .python-version
FROM python:3.11.5-slim

COPY dist/dbt-bouncer.pex dbt-bouncer.pex

CMD ["/bin/bash", "-c", "echo 'Expecting commands to be passed in.' && exit 1"]
