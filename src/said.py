import pandas as pd
from sqlalchemy import create_engine, text

# lire le fichier CSV
df = pd.read_csv("data/said.csv", sep=";")

# connexion à MySQL (serveur local)
# عدّل اسم قاعدة البيانات وبيانات الدخول إذا لزم
engine = create_engine("mysql+pymysql://root:@localhost/student_performance")

# تجهيز الجدول إن لم يكن موجودًا، وإضافة الأعمدة الناقصة بدون حذف البيانات
with engine.begin() as conn:
    conn.execute(
        text(
            "CREATE TABLE IF NOT EXISTS etudiants ("
            "ID INT, NAME VARCHAR(255), NOTE INT)"
        )
    )
    conn.execute(text("ALTER TABLE etudiants ADD COLUMN IF NOT EXISTS ID INT"))
    conn.execute(text("ALTER TABLE etudiants ADD COLUMN IF NOT EXISTS NAME VARCHAR(255)"))
    conn.execute(text("ALTER TABLE etudiants ADD COLUMN IF NOT EXISTS NOTE INT"))
    pk_exists = conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            "WHERE table_schema = DATABASE() "
            "AND table_name = 'etudiants' "
            "AND constraint_type = 'PRIMARY KEY'"
        )
    ).scalar()
    if pk_exists == 0:
        conn.execute(text("ALTER TABLE etudiants ADD PRIMARY KEY (ID)"))

# إرسال البيانات إلى MySQL دون حذف ما سبق مع التحديث عند التكرار
upsert_sql = text(
    "INSERT INTO etudiants (ID, NAME, NOTE) "
    "VALUES (:ID, :NAME, :NOTE) "
    "ON DUPLICATE KEY UPDATE "
    "NAME = VALUES(NAME), NOTE = VALUES(NOTE)"
)
rows = df.to_dict(orient="records")
with engine.begin() as conn:
    if rows:
        conn.execute(upsert_sql, rows)

# استخراج الطلاب الذين لديهم G3 > 15 في DataFrame
query = text("SELECT * FROM etudiants WHERE G3 > 15")
df_g3 = pd.read_sql_query(query, con=engine)

print("Importation réussie")
print(df_g3.head())
