from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
from sqlalchemy import create_engine, exc
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# Configure logging
logger = logging.getLogger(_name_)

load_dotenv()

default_args = {
    'owner': 'soukaina',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

def run_scraper():
    """Exécute le script de scraping et gère les erreurs proprement."""
    try:
        base_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project"
        script_path = os.path.join(base_dir, "scripts", "reviews_collection.py")
        
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found at {script_path}")
        
        # Convertir les fins de ligne au cas où (pour Windows/Linux)
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        with open(script_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(content)
        
        result = subprocess.run(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        logger.info("Script output:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Script warnings:\n%s", result.stderr)
            
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error("Script failed with exit code %d", e.returncode)
        logger.error("Error output:\n%s", e.stderr)
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise

def insert_into_postgresql(df_reviews):
    """Insère les données dans PostgreSQL avec gestion des erreurs."""
    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
            pool_pre_ping=True
        )
        
        with engine.connect() as conn:
            df_reviews.to_sql(
                "staging_reviews",
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
        logger.info("Data inserted successfully")
        
    except exc.SQLAlchemyError as e:
        logger.error("Database error: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise

def insert_reviews(**context):
    """Lit le CSV et insère les données, vérifiant d'abord son existence."""
    try:
        data_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project"
        csv_path = os.path.join(data_dir, "data", "bank_reviews.csv")
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        # Validation basique des données
        if df.empty:
            logger.warning("Le fichier CSV est vide")
        
        insert_into_postgresql(df)
        
    except pd.errors.EmptyDataError:
        logger.error("Le fichier CSV est vide ou corrompu")
        raise
    except Exception as e:
        logger.error("Error processing CSV: %s", str(e))
        raise

def run_complete_transformation():
    """Transformation complète avec sentiment + topics (Phase 2)"""
    try:
        base_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform"
        
        commands = [
            f"cd {base_dir}",
            "source ~/bank_reviews_project/dbt_env/bin/activate",
            "python scripts/sentiment_analysis.py",
            "python scripts/topic_extraction.py"
        ]
        
        result = subprocess.run(" && ".join(commands), shell=True, check=True)
        logger.info("Phase 2 transformation completed successfully")
        
    except subprocess.CalledProcessError as e:
        logger.error("Phase 2 transformation failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in transformation: %s", str(e))
        raise

def create_star_schema():
    """Crée le schéma en étoile - Phase 3"""
    try:
        base_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform"
        
        commands = [
            f"cd {base_dir}",
            "source ~/bank_reviews_project/dbt_env/bin/activate",
            "dbt run --models tag:dimensions",  # Exécute d'abord les dimensions
            "dbt test --models tag:dimensions",  # Teste les dimensions
            "dbt run --models tag:facts",       # Puis les faits
            "dbt test --models tag:facts"       # Teste les faits
        ]
        
        result = subprocess.run(" && ".join(commands), shell=True, check=True)
        logger.info("Star schema creation completed successfully")
        
    except subprocess.CalledProcessError as e:
        logger.error("Star schema creation failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in star schema creation: %s", str(e))
        raise

def validate_data_quality():
    """Valide la qualité des données du data warehouse"""
    try:
        base_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform"
        
        commands = [
            f"cd {base_dir}",
            "source ~/bank_reviews_project/dbt_env/bin/activate",
            "dbt run --models tag:data_quality",  # Modèles de validation
            "dbt test --select tag:critical"      # Tests critiques uniquement
        ]
        
        result = subprocess.run(" && ".join(commands), shell=True, check=True)
        logger.info("Data quality validation completed successfully")
        
    except subprocess.CalledProcessError as e:
        logger.error("Data quality validation failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in data quality validation: %s", str(e))
        raise

def generate_documentation():
    """Génère la documentation DBT"""
    try:
        base_dir = "/mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform"
        
        commands = [
            f"cd {base_dir}",
            "source ~/bank_reviews_project/dbt_env/bin/activate",
            "dbt docs generate"
        ]
        
        result = subprocess.run(" && ".join(commands), shell=True, check=True)
        logger.info("DBT documentation generated successfully")
        
    except subprocess.CalledProcessError as e:
        logger.error("Documentation generation failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in documentation generation: %s", str(e))
        raise

# Définition du DAG
with DAG(
    dag_id='bank_reviews_complete_pipeline',
    default_args=default_args,
    description='Pipeline complet: Scraping → Transformation → Star Schema → BI',
    schedule='@daily',
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=['scraping', 'postgresql', 'dbt', 'star-schema', 'data-warehouse'],
) as dag:
    
    # Phase 1: Data Collection
    t1_scraper = PythonOperator(
        task_id='collect_google_maps_reviews',
        python_callable=run_scraper,
    )

    t2_insert = PythonOperator(
        task_id='insert_reviews_to_postgresql',
        python_callable=insert_reviews,
    )
    
    # Phase 2: Data Cleaning & Transformation
    t3_transformation = PythonOperator(
        task_id='sentiment_and_topic_analysis',
        python_callable=run_complete_transformation,
    )
    
    # Phase 3: Data Modeling (Star Schema)
    t4_star_schema = PythonOperator(
        task_id='create_star_schema_warehouse',
        python_callable=create_star_schema,
    )
    
    # Phase 3: Data Quality Validation
    t5_data_quality = PythonOperator(
        task_id='validate_data_warehouse_quality',
        python_callable=validate_data_quality,
    )
    
    # Documentation Generation
    t6_documentation = PythonOperator(
        task_id='generate_dbt_documentation',
        python_callable=generate_documentation,
    )
    
    # Alternative: Utiliser BashOperator pour plus de simplicité
    dbt_run_dimensions = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command="""
        cd /mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform && \
        source ~/bank_reviews_project/dbt_env/bin/activate && \
        dbt run --models tag:dimensions
        """,
    )
    
    dbt_run_facts = BashOperator(
        task_id='dbt_run_facts',
        bash_command="""
        cd /mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform && \
        source ~/bank_reviews_project/dbt_env/bin/activate && \
        dbt run --models tag:facts
        """,
    )
    
    dbt_test_all = BashOperator(
        task_id='dbt_test_complete_warehouse',
        bash_command="""
        cd /mnt/c/Users/hp/Downloads/bank_reviews_project/bank_reviews_transform && \
        source ~/bank_reviews_project/dbt_env/bin/activate && \
        dbt test
        """,
    )

    # Définition des dépendances du pipeline
    # Phase 1: Collection
    t1_scraper >> t2_insert
    
    # Phase 2: Transformation
    t2_insert >> t3_transformation
    
    # Phase 3: Star Schema (deux approches - choisissez l'une)
    # Approche 1: Fonctions Python
    t3_transformation >> t4_star_schema >> t5_data_quality >> t6_documentation
    
    # Approche 2: BashOperator (plus simple et fiable)
    # t3_transformation >> dbt_run_dimensions >> dbt_run_facts >> dbt_test_all >> t6_documentation