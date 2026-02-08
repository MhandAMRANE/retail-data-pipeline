import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import os
import traceback


def main():
    # Detect the directory of the script
    base_dir = Path(__file__).resolve().parent
    
    # Path logic: 
    # - If running in Docker (scripts/ dir), data is in ../data/
    # - If running locally (scripts/ dir), data is in ../data/
    csv_path = base_dir.parent / "data" / "online_retail.csv"

    # Charger le dataset
    try:
        df = pd.read_csv(csv_path, encoding="ISO-8859-1")
    except FileNotFoundError:
        # Fallback for different CWD or relative path issues
        print(f"Erreur : Fichier {csv_path} non trouve. Tentative avec chemin relatif direct...")
        csv_path = Path("data/online_retail.csv")
        df = pd.read_csv(csv_path, encoding="ISO-8859-1")

    print("Taille initiale :", df.shape)
    
    # Nettoyage des données
    df = df.dropna(subset=["CustomerID"])
    df = df[df["Quantity"] > 0]
    df = df[df["UnitPrice"] > 0]
    
    # Conversions et nouvelles colonnes
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["TotalAmount"] = df["Quantity"] * df["UnitPrice"]

    # Mapping des colonnes pour correspondre a la base de donnees (snake_case)
    column_mapping = {
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
        "TotalAmount": "total_amount"
    }
    df = df.rename(columns=column_mapping)

    # Configuration de la base de donnees (Utilise les variables d'environnement)
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASS", "postgres")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "retail_db")

    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    try:
        engine = create_engine(db_url)

        # Charger les données dans PostgreSQL
        df.to_sql(
            name="online_retail",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )

        print(f"Donnees chargees avec succes dans PostgreSQL ({db_host})")
    except Exception as e:
        print("Erreur : Impossible de charger les donnees dans PostgreSQL")
        traceback.print_exc()


if __name__ == "__main__":
    main()