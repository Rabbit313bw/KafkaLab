FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/frontend /app/frontend

EXPOSE 8501

CMD ["streamlit", "run", "frontend/app.py"] 