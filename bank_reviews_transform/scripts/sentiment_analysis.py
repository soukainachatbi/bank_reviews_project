import pandas as pd
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from langdetect import detect, DetectorFactory
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Fixer la graine pour la détection de langue
DetectorFactory.seed = 0

load_dotenv()

def analyze_sentiment(text):
    """Analyse de sentiment avec VADER et TextBlob"""
    # VADER (bon pour texte informel)
    analyzer = SentimentIntensityAnalyzer()
    vader_scores = analyzer.polarity_scores(text)
    
    # TextBlob
    blob = TextBlob(text)
    textblob_polarity = blob.sentiment.polarity
    
    # Classification finale
    if vader_scores['compound'] >= 0.05:
        sentiment = 'positive'
    elif vader_scores['compound'] <= -0.05:
        sentiment = 'negative'
    else:
        sentiment = 'neutral'
    
    return {
        'sentiment': sentiment,
        'vader_compound': vader_scores['compound'],
        'textblob_polarity': textblob_polarity,
        'confidence': abs(vader_scores['compound'])
    }

def detect_language_advanced(text):
    """Détection de langue améliorée"""
    try:
        return detect(text)
    except:
        # Fallback avec des mots-clés
        if any(word in text.lower() for word in ['والله', 'الله', 'جيد', 'سيء']):
            return 'ar'
        elif any(word in text.lower() for word in ['très', 'bon', 'mauvais', 'service']):
            return 'fr'
        elif any(word in text.lower() for word in ['good', 'bad', 'service', 'staff']):
            return 'en'
        return 'unknown'

def process_reviews():
    """Traite les avis avec analyse de sentiment"""
    # Connexion à la base
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DB_URL)
    
    # Lire les données nettoyées
    query = "SELECT * FROM stg_reviews"
    df = pd.read_sql(query, engine)
    
    # Analyse de sentiment
    sentiment_results = []
    for _, row in df.iterrows():
        result = analyze_sentiment(row['clean_text'])
        result['review_id'] = row['review_id']
        sentiment_results.append(result)
    
    # Créer DataFrame des résultats
    sentiment_df = pd.DataFrame(sentiment_results)
    
    # Sauvegarder dans PostgreSQL
    sentiment_df.to_sql('sentiment_analysis', engine, if_exists='replace', index=False)
    
    print(f"✅ Analyse de sentiment terminée pour {len(sentiment_df)} avis")
    return sentiment_df

if __name__ == "__main__":
    process_reviews()