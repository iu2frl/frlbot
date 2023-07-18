FROM python:3.11.2
WORKDIR /home/frlbot
COPY ./ .
RUN pip install -r /home/frlbot/requirements.txt
CMD ["python3", "./frlbot.py"]