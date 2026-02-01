import requests
import json
from datetime import datetime, timezone

user_agent_key = "User-Agent"
user_agent_value = "Mozilla/5.0"
headers = {user_agent_key: user_agent_value}

def get_history_data(ticker: str, start_date: str, end_date: str, interval: str = "1wk"):
    """
    Получает исторические данные для указанного тикера актива.

    :param ticker: str, тикер актива.
    :param start_date: str, дата начала периода в формате 'дд.мм.гг'.
    :param end_date: str, дата окончания периода в формате 'дд.мм.гг'.
    :param interval: str, интервал времени (неделя, день и т.д.) (необязательный, по умолчанию '1wk' - одна неделя).
    :return: str, JSON-строка с историческими данными.
    """
    per2 = int(datetime.strptime(end_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())
    per1 = int(datetime.strptime(start_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())
    params = {"period1": str(per1), "period2": str(per2),
              "interval": interval, "includeAdjustedClose": "true"}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    response = requests.get(url, headers=headers, params=params)
    return response.json()

print(get_history_data('AAPL',"02.02.24","02.02.25","1wk"))