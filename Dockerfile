FROM python:3.12-slim

WORKDIR /opt/CookieLeveling

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY cookieleveling ./cookieleveling

CMD ["python", "-m", "cookieleveling"]
