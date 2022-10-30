FROM python:3.10-slim

COPY ./main.py /app/main.py
COPY ./player.py /app/player.py
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN apt-get update \
    && python3 -m pip install -r requirements.txt \
    && apt-get autoremove -y

ENV PORT=8000
EXPOSE 8000

CMD ["python", "main.py"]