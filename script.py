import os
import sys
import pandas as pd
from datetime import datetime, timedelta

def load_data_for_last_7_days(date_str):
    # Парсим входную дату
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Собираем все даты за последние 7 дней
    dates = [(target_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    
    # Собираем данные за каждый день
    data_frames = []
    for date in dates:
        file_path = f'input/{date}.csv'
        if os.path.exists(file_path):
            # Читаем файл без заголовков
            df = pd.read_csv(file_path, header=None, names=['email', 'action', 'date'])
            data_frames.append(df)
        else:
            print(f"Файл не найден: {file_path}")

    # Объединяем все данные в один DataFrame
    if data_frames:
        return pd.concat(data_frames)
    else:
        return pd.DataFrame(columns=['email', 'action', 'date'])

def count_actions(df):
    # Сначала создадим таблицу подсчёта всех действий
    action_counts = df.groupby(['email', 'action']).size().unstack(fill_value=0)
    
    # Добавляем отсутствующие столбцы действий, если их нет
    for action in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
        if action not in action_counts:
            action_counts[action] = 0
    
    # Возвращаем результат в нужном формате
    result_df = action_counts.reset_index().rename(columns={
        'CREATE': 'create_count',
        'READ': 'read_count',
        'UPDATE': 'update_count',
        'DELETE': 'delete_count'
    })
    
    return result_df

def save_result(date_str, result_df):
    output_file_path = f'output/{date_str}.csv'
    os.makedirs('output', exist_ok=True)
    result_df.to_csv(output_file_path, index=False)
    print(f"Результат сохранён в файл: {output_file_path}")

def main():
    if len(sys.argv) != 2:
        print("Использование: python script.py <yyyy-mm-dd>")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    # Загружаем данные за последние 7 дней
    df = load_data_for_last_7_days(date_str)
    
    if df.empty:
        print("Нет данных для обработки.")
        sys.exit(1)
    
    # Считаем количество действий
    result_df = count_actions(df)
    
    # Сохраняем результат
    save_result(date_str, result_df)

if __name__ == "__main__":
    main()