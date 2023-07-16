FROM python:3.11.2
WORKDIR /home/frlbot
COPY g4f/* /home/frlbot/g4f/
COPY *.py /home/frlbot/
COPY requirements.txt /home/frlbot/
RUN pip install -r /home/frlbot/requirements.txt
CMD ["python3", "/home/frlbot/frlbot.py"]