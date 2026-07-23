FROM python:3.14.5-bookworm
RUN apt update && apt install -y python3-lxml && rm -rf /var/lib/apt/lists/*
RUN addgroup --gid 1000 ejecutor && adduser --ingroup ejecutor --uid 1000 --disabled-login ejecutor
USER 1000:1000
WORKDIR /apify_client
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
ADD ./src ./src
ADD tasks.xlsx tasks.xlsx
ENTRYPOINT [ "python", "-u", "-m", "src.run_searches" ]