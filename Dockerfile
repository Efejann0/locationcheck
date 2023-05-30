FROM python:3.9-slim

WORKDIR /home/app

ENV TZ="Europe/Istanbul"
ENV ACCEPT_EULA=Y

COPY main.py /home/app
COPY apicollect.py /home/app
COPY database.py /home/app
COPY locationcheck.py /home/app
COPY .env /home/app
COPY config.ini /home/app
COPY requirements.txt ./
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY . . 

CMD ["python3", "/home/app/main.py"]