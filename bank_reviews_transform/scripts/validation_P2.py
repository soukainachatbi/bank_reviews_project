import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()

def comprehensive_validation():
    """Validation complète de la Phase 2"""
    
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DB_URL)
    
    print("🔍 VALIDATION COMPLÈTE DE LA PHASE 2")
    print("=" * 50)
    
    # 1. Vérification du nettoyage des données
    print("\n1️⃣ NETTOYAGE DES DONNÉES")
    print("-" * 30)
    
    # Compter les doublons éliminés
    original_count = pd.read_sql("SELECT COUNT(*) as count FROM staging_reviews", engine).iloc[0]['count']
    cleaned_count = pd.read_sql("SELECT COUNT(*) as count FROM stg_reviews", engine).iloc[0]['count']
    
    print(f"📊 Avis originaux: {original_count}")
    print(f"📊 Avis après nettoyage: {cleaned_count}")
    print(f"📊 Doublons supprimés: {original_count - cleaned_count} ({((original_count - cleaned_count)/original_count*100):.1f}%)")
    
    # Vérifier la qualité du texte
    text_quality = pd.read_sql("""
        SELECT 
            text_quality,
            COUNT(*) as count,
            ROUND(AVG(text_length)::numeric, 0) as avg_length
        FROM stg_reviews 
        GROUP BY text_quality
    """, engine)
    print(f"\n📝 Distribution qualité du texte:")
    print(text_quality.to_string(index=False))
    
    # 2. Vérification de l'analyse de sentiment
    print("\n\n2️⃣ ANALYSE DE SENTIMENT")
    print("-" * 30)
    
    sentiment_stats = pd.read_sql("""
        SELECT 
            sentiment,
            COUNT(*) as count,
            ROUND(AVG(rating)::numeric, 2) as avg_rating,
            ROUND(AVG(confidence)::numeric, 3) as avg_confidence
        FROM reviews_enriched 
        WHERE sentiment IS NOT NULL
        GROUP BY sentiment
        ORDER BY count DESC
    """, engine)
    print(f"📊 Distribution des sentiments:")
    print(sentiment_stats.to_string(index=False))
    
    # Cohérence sentiment/rating
    consistency = pd.read_sql("""
        SELECT 
            sentiment_rating_consistency,
            COUNT(*) as count,
            ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 1) as percentage
        FROM reviews_enriched 
        WHERE sentiment IS NOT NULL
        GROUP BY sentiment_rating_consistency
    """, engine)
    print(f"\n🎯 Cohérence sentiment/note:")
    print(consistency.to_string(index=False))
    
    # 3. Vérification de l'extraction de langues
    print("\n\n3️⃣ DÉTECTION DE LANGUES")
    print("-" * 30)
    
    language_stats = pd.read_sql("""
        SELECT 
            detected_language,
            COUNT(*) as count,
            ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 1) as percentage
        FROM stg_reviews 
        GROUP BY detected_language
        ORDER BY count DESC
    """, engine)
    print(f"🌍 Distribution des langues:")
    print(language_stats.to_string(index=False))
    
    # 4. Vérification de l'analyse de topics
    print("\n\n4️⃣ ANALYSE DE TOPICS")
    print("-" * 30)
    
    try:
        topic_stats = pd.read_sql("""
            SELECT 
                topic_category,
                COUNT(*) as count,
                ROUND(AVG(topic_confidence)::numeric, 3) as avg_confidence,
                STRING_AGG(DISTINCT LEFT(topic_keywords, 50), ' | ') as sample_keywords
            FROM reviews_enriched 
            WHERE topic_category IS NOT NULL
            GROUP BY topic_category
            ORDER BY count DESC
        """, engine)
        print(f"🏷️  Distribution des catégories de topics:")
        print(topic_stats.to_string(index=False))
        
        # Topics par sentiment
        topic_sentiment = pd.read_sql("""
            SELECT 
                topic_category,
                sentiment,
                COUNT(*) as count
            FROM reviews_enriched 
            WHERE topic_category IS NOT NULL AND sentiment IS NOT NULL
            GROUP BY topic_category, sentiment
            ORDER BY topic_category, sentiment
        """, engine)
        print(f"\n🔗 Topics par sentiment:")
        pivot_table = topic_sentiment.pivot(index='topic_category', columns='sentiment', values='count').fillna(0)
        print(pivot_table.to_string())
        
    except Exception as e:
        print(f"⚠️  Analyse de topics non disponible: {e}")
    
    # 5. Vérification par banque
    print("\n\n5️⃣ ANALYSE PAR BANQUE")
    print("-" * 30)
    
    bank_stats = pd.read_sql("""
        SELECT 
            bank,
            COUNT(*) as total_reviews,
            ROUND(AVG(rating)::numeric, 2) as avg_rating,
            COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) as positive_reviews,
            COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) as negative_reviews
        FROM reviews_enriched 
        WHERE sentiment IS NOT NULL
        GROUP BY bank
        ORDER BY total_reviews DESC
    """, engine)
    print(f"🏦 Statistiques par banque:")
    print(bank_stats.to_string(index=False))
    
    # 6. Métriques de qualité globales
    print("\n\n6️⃣ MÉTRIQUES DE QUALITÉ")
    print("-" * 30)
    
    quality_metrics = pd.read_sql("""
        SELECT 
            COUNT(*) as total_reviews,
            COUNT(CASE WHEN sentiment IS NOT NULL THEN 1 END) as with_sentiment,
            COUNT(CASE WHEN topic_category IS NOT NULL THEN 1 END) as with_topics,
            COUNT(CASE WHEN detected_language != 'unknown' THEN 1 END) as with_language,
            ROUND(AVG(CASE WHEN confidence IS NOT NULL THEN confidence END)::numeric, 3) as avg_sentiment_confidence,
            COUNT(CASE WHEN review_quality = 'high' THEN 1 END) as high_quality,
            COUNT(CASE WHEN sentiment_rating_consistency = 'coherent' THEN 1 END) as coherent_reviews
        FROM reviews_enriched
    """, engine)
    
    metrics = quality_metrics.iloc[0]
    total = metrics['total_reviews']
    
    print(f"📊 Total des avis traités: {total}")
    print(f"📊 Avec analyse de sentiment: {metrics['with_sentiment']} ({metrics['with_sentiment']/total*100:.1f}%)")
    print(f"📊 Avec extraction de topics: {metrics['with_topics']} ({metrics['with_topics']/total*100:.1f}%)")
    print(f"📊 Avec détection de langue: {metrics['with_language']} ({metrics['with_language']/total*100:.1f}%)")
    print(f"📊 Confiance sentiment moyenne: {metrics['avg_sentiment_confidence']}")
    print(f"📊 Avis haute qualité: {metrics['high_quality']} ({metrics['high_quality']/total*100:.1f}%)")
    print(f"📊 Avis cohérents: {metrics['coherent_reviews']} ({metrics['coherent_reviews']/total*100:.1f}%)")
    
    # 7. Recommandations
    print("\n\n7️⃣ RECOMMANDATIONS")
    print("-" * 30)
    
    # Taux de couverture
    sentiment_coverage = metrics['with_sentiment'] / total
    topic_coverage = metrics['with_topics'] / total
    coherence_rate = metrics['coherent_reviews'] / total
    
    if sentiment_coverage < 0.9:
        print("⚠️  Couverture sentiment faible - vérifier la configuration VADER/TextBlob")
    
    if topic_coverage < 0.7:
        print("⚠️  Couverture topics faible - ajuster les paramètres LDA")
    
    if coherence_rate < 0.7:
        print("⚠️  Cohérence sentiment/note faible - vérifier la qualité des données")
    
    if metrics['avg_sentiment_confidence'] and metrics['avg_sentiment_confidence'] < 0.3:
        print("⚠️  Confiance sentiment faible - revoir les seuils de classification")
    
    print(f"\n✅ PHASE 2 TERMINÉE AVEC SUCCÈS")
    print(f"📈 Données prêtes pour la Phase 3 (Modélisation en étoile)")
    
    return {
        'total_reviews': total,
        'sentiment_coverage': sentiment_coverage,
        'topic_coverage': topic_coverage,
        'coherence_rate': coherence_rate,
        'avg_confidence': metrics['avg_sentiment_confidence']
    }

if __name__ == "__main__":
    results = comprehensive_validation()