import sqlite3
#ТУТ МОГУТ БУТЬ ГОНКИ ДАННЫХ, НУ И ЛАДНО ПОКА

#Инициализирует SQLite базу данных для хранения вакансий
def init_db(db_file: str):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vacancies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            yoe INTEGER
        )
    ''')

    conn.commit()
    conn.close()
    print(f"Database initialized in file: {db_file}")

#Добавление новой вакансии в базу
def post_vacancy(db_file: str, name: str, description: str, yoe: int) -> int:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO vacancies (name, description, yoe)
        VALUES (?, ?, ?)
    ''', (name, description, yoe))

    last_id = cursor.lastrowid
    conn.commit()
    conn.close()
    print(f"Vacancy posted in file: {db_file} with name {name}, id: {last_id}")
    return last_id

#Получение всех вакансий, требуемый опыт которых ниже заданного
def get_vacancies_by_yoe(db_file: str, yoe: int):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
        
    cursor.execute('''
        SELECT id, name, description, yoe FROM vacancies
        WHERE yoe <= ?
        ORDER BY id ASC
    ''', (yoe,))

    result = cursor.fetchall()
        
    conn.close()
    print(f"Found {len(result)} results for yoe <= {yoe} in {db_file}")
    if result: return result

#Получение данных вакансии по id
def get_vacancy_by_id(db_file: str, vacancy_id: int):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
        
    cursor.execute('''
        SELECT id, name, description, yoe FROM vacancies
        WHERE id <= ?
        ORDER BY id ASC
        LIMIT 1
    ''', (vacancy_id,))

    result = cursor.fetchone()
        
    conn.close()
    print(f"Found {len(result)} results for yoe <= {vacancy_id} in {db_file}")
    if result: return result

#Обновить данные вакансии
def put_vacancy(db_file:str, id: int, name: str, description: str, yoe: int) -> int:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
        
    cursor.execute('''
        UPDATE vacancies
        SET name = ?, description = ?, yoe = ?
        WHERE id = ?
    ''', (name, description, yoe, id))

    rows_affected = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Vacancy {id} updated in file: {db_file}, rows affected: {rows_affected}")
    return rows_affected