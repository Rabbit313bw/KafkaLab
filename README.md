
Этот проект представляет собой систему анализа настроений твитов в реальном времени, использующую Apache Kafka для обработки данных и современные методы анализа естественного языка.

запуск

- docker-compose up --build -d

host для streamlit

http://localhost:8501




Система использует комбинацию двух подходов для анализа настроений:
- TextBlob для определения полярности текста
- Модель эмоций на основе DistilBERT для определения доминирующих эмоций


вывод логов kafka

- docker-compose logs -f backend

вывод логов для streamlit
- docker-compose logs -f frontend

выключение
- docker-compose down
