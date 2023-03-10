import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="mysecretpassword",
)

cur = conn.cursor()

cur.execute(
    """
    CREATE TABLE member (
        member_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        date_of_birth DATE,
        mobile_no VARCHAR(20),
        email VARCHAR(50)
    )
    """
)

cur.execute(
    """
    CREATE TABLE items (
        item_id SERIAL PRIMARY KEY,
        item_name VARCHAR(50),
        manufacture_name VARCHAR(50),
        cost DECIMAL(10,2),
        weight_kg DECIMAL(5,2)
    )
    """
)

cur.execute(
    """
    CREATE TABLE transaction (
        id SERIAL PRIMARY KEY,
        member_id INTEGER REFERENCES member(member_id),
        item_id INTEGER REFERENCES items(item_id),
        qty INTEGER,
        total_items_price DECIMAL(10,2),
        total_items_weight DECIMAL(5,2)
    )
    """
)

conn.commit()
cur.close()
conn.close()
