import requests
from datetime import datetime, timezone
import csv
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

_queue = queue.Queue()

user_agent_key = "User-Agent"
user_agent_value = "Mozilla/5.0"
headers = {user_agent_key: user_agent_value}
TICKERS = ["AAPL", "MSFT", "GOOGL"]  # Пример нескольких тикеров

def get_history_data(ticker: str, start_date: str, end_date: str, interval: str = "1wk"):
    """Получает данные с исправленным URL (без пробелов!)."""
    per1 = int(datetime.strptime(start_date, "%d.%m.%y").replace(tzinfo=timezone.utc).timestamp())
    per2 = int(datetime.strptime(end_date, "%d.%m.%y").replace(tzinfo=timezone.utc).timestamp())
    
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    
    params = {
        "period1": per1,
        "period2": per2,
        "interval": interval,
        "includeAdjustedClose": "true"
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def fetch_and_enqueue(ticker: str, start_date: str, end_date: str, interval: str = "1wk"):
    """Получает данные и помещает их в очередь для записи."""
    try:
        data = get_history_data(ticker, start_date, end_date, interval)
        result = data["chart"]["result"][0]
        symbol = result["meta"]["symbol"]
        closes = result["indicators"]["quote"][0]["close"]
        _queue.put((symbol, closes))
    except Exception as e:
        print(f"✗ Ошибка для {ticker}: {e}")

def writer_worker():
    """Поток-писатель: добавляет данные в CSV без перезаписи."""
    os.makedirs("data_csv", exist_ok=True)
    filepath = os.path.join("data_csv", "result.csv")
    
    # Записываем заголовок только при первом создании файла
    write_header = not os.path.exists(filepath)
    
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["Symbol", "Close_Values"])  # Простой заголовок
        
        while True:
            item = _queue.get()
            if item is None:  # Сигнал завершения
                _queue.task_done()
                break
            
            symbol, closes = item
            # ✅ ПРАВИЛЬНАЯ ЗАПИСЬ: тикер + цены как отдельные ячейки
            # Вариант 1: одна строка = тикер + все цены (рекомендуется)
            writer.writerow([symbol] + closes)
            
            # Вариант 2 (альтернатива): каждая цена в отдельной строке
            # for price in closes:
            #     writer.writerow([symbol, price])
            
            f.flush()  # Немедленная запись на диск
            _queue.task_done()

if __name__ == "__main__":
    # Запускаем поток-писатель
    writer_thread = threading.Thread(target=writer_worker, daemon=False)
    writer_thread.start()
    
    # Загружаем данные параллельно
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(fetch_and_enqueue, ticker, "02.02.25", "20.03.25", "1wk")
            for ticker in TICKERS
        ]
        # Ждём завершения всех загрузок
        for future in futures:
            future.result()
    
    # Завершаем поток-писатель
    _queue.put(None)  # Сигнал остановки
    _queue.join()     # Ждём обработки всех элементов очереди
    writer_thread.join()
    