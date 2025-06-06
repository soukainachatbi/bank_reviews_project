import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import re
from collections import Counter

# Stopwords multilingues
FRENCH_STOPWORDS = {
    'le', 'de', 'et', 'à', 'un', 'il', 'être', 'et', 'en', 'avoir', 'que', 'pour',
    'dans', 'ce', 'son', 'une', 'sur', 'avec', 'ne', 'se', 'pas', 'tout', 'plus',
    'par', 'grand', 'en', 'le', 'bien', 'autre', 'pour', 'ce', 'tout', 'mais',
    'très', 'bon', 'service', 'banque', 'agence', 'personnel', 'client'
}

ARABIC_STOPWORDS = {
    'في', 'من', 'إلى', 'على', 'مع', 'هذا', 'هذه', 'التي', 'الذي', 'كان', 'كانت',
    'يكون', 'تكون', 'لا', 'ما', 'إن', 'أن', 'قد', 'لقد', 'كل', 'بعض', 'جميع'
}

ALL_STOPWORDS = ENGLISH_STOP_WORDS.union(FRENCH_STOPWORDS).union(ARABIC_STOPWORDS)

load_dotenv()

def preprocess_text(text, language='fr'):
    """Prétraitement du texte pour LDA"""
    # Convertir en minuscules
    text = text.lower()
    
    # Supprimer la ponctuation et caractères spéciaux
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Supprimer les chiffres
    text = re.sub(r'\d+', '', text)
    
    # Supprimer les espaces multiples
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Tokeniser et supprimer les mots courts
    words = [word for word in text.split() if len(word) > 2]
    
    # Supprimer les stopwords
    words = [word for word in words if word not in ALL_STOPWORDS]
    
    return ' '.join(words)

def extract_topics_lda(texts, n_topics=5, language='fr'):
    """Extraction de topics avec LDA"""
    
    # Prétraitement
    processed_texts = [preprocess_text(text, language) for text in texts]
    
    # Filtrer les textes vides
    processed_texts = [text for text in processed_texts if text.strip()]
    
    if len(processed_texts) < 10:
        print("⚠️  Pas assez de textes pour l'analyse LDA")
        return None, None, None
    
    # Vectorisation TF-IDF
    vectorizer = TfidfVectorizer(
        max_features=100,
        min_df=2,
        max_df=0.8,
        ngram_range=(1, 2),
        stop_words=list(ALL_STOPWORDS)
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform(processed_texts)
        
        # LDA
        lda = LatentDirichletAllocation(
            n_components=n_topics,
            random_state=42,
            max_iter=100,
            learning_method='batch'
        )
        
        lda.fit(tfidf_matrix)
        
        # Extraire les mots-clés pour chaque topic
        feature_names = vectorizer.get_feature_names_out()
        topics = []
        
        for topic_idx, topic in enumerate(lda.components_):
            top_words_idx = topic.argsort()[-10:][::-1]
            top_words = [feature_names[i] for i in top_words_idx]
            topics.append({
                'topic_id': topic_idx,
                'keywords': top_words,
                'weight_sum': topic[top_words_idx].sum()
            })
        
        # Assigner des topics aux documents
        doc_topic_dist = lda.transform(tfidf_matrix)
        dominant_topics = np.argmax(doc_topic_dist, axis=1)
        
        return topics, dominant_topics, doc_topic_dist
        
    except Exception as e:
        print(f"❌ Erreur LDA: {e}")
        return None, None, None

def categorize_topics(topics):
    """Catégorise automatiquement les topics basés sur les mots-clés"""
    
    # Dictionnaire de catégories avec mots-clés
    categories = {
        'service_client': ['service', 'personnel', 'accueil', 'staff', 'équipe', 'conseiller'],
        'attente_rapidite': ['attente', 'rapide', 'lent', 'temps', 'queue', 'file'],
        'frais_tarifs': ['frais', 'cher', 'prix', 'tarif', 'coût', 'gratuit'],
        'digital_technologie': ['application', 'app', 'site', 'internet', 'digital', 'technologie'],
        'localisation_acces': ['parking', 'accès', 'location', 'proche', 'loin', 'centre']
    }
    
    categorized_topics = []
    
    for topic in topics:
        topic_keywords = [word.lower() for word in topic['keywords']]
        
        # Calculer le score pour chaque catégorie
        category_scores = {}
        for category, category_words in categories.items():
            score = sum(1 for word in topic_keywords if any(cat_word in word for cat_word in category_words))
            category_scores[category] = score
        
        # Assigner la catégorie avec le meilleur score
        best_category = max(category_scores.items(), key=lambda x: x[1])
        
        categorized_topics.append({
            **topic,
            'category': best_category[0] if best_category[1] > 0 else 'autre',
            'category_confidence': best_category[1] / len(topic_keywords)
        })
    
    return categorized_topics

def process_topic_extraction():
    """Traite l'extraction de topics pour tous les avis"""
    
    # Connexion à la base
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DB_URL)
    
    # Lire les données nettoyées
    query = """
    SELECT review_id, clean_text, detected_language, sentiment, bank
    FROM reviews_enriched 
    WHERE text_length > 20
    """
    
    df = pd.read_sql(query, engine)
    print(f"📊 Analyse de {len(df)} avis pour extraction de topics")
    
    # Grouper par langue pour une meilleure analyse
    results = []
    
    for language in df['detected_language'].unique():
        if language == 'unknown':
            continue
            
        lang_df = df[df['detected_language'] == language]
        if len(lang_df) < 10:
            continue
            
        print(f"\n🔍 Analyse pour langue: {language} ({len(lang_df)} avis)")
        
        # Extraction de topics
        topics, dominant_topics, doc_topic_dist = extract_topics_lda(
            lang_df['clean_text'].tolist(), 
            n_topics=5, 
            language=language
        )
        
        if topics is None:
            continue
            
        # Catégoriser les topics  
        categorized_topics = categorize_topics(topics)
        
        # Assigner les topics aux avis
        for idx, (_, row) in enumerate(lang_df.iterrows()):
            if idx < len(dominant_topics):
                topic_id = dominant_topics[idx]
                topic_scores = doc_topic_dist[idx] if doc_topic_dist is not None else [0] * 5
                
                results.append({
                    'review_id': row['review_id'],
                    'dominant_topic': topic_id,
                    'topic_category': categorized_topics[topic_id]['category'],
                    'topic_keywords': ', '.join(categorized_topics[topic_id]['keywords'][:5]),
                    'topic_confidence': float(max(topic_scores)),
                    'language': language
                })
        
        # Afficher les topics trouvés
        print(f"📋 Topics identifiés pour {language}:")
        for topic in categorized_topics:
            print(f"  Topic {topic['topic_id']} ({topic['category']}): {', '.join(topic['keywords'][:5])}")
    
    # Sauvegarder les résultats
    if results:
        topics_df = pd.DataFrame(results)
        topics_df.to_sql('topic_analysis', engine, if_exists='replace', index=False)
        print(f"\n✅ Analyse de topics terminée - {len(topics_df)} avis traités")
        
        # Statistiques
        print("\n📊 Distribution des catégories de topics:")
        category_dist = topics_df['topic_category'].value_counts()
        for category, count in category_dist.items():
            print(f"  {category}: {count} avis ({count/len(topics_df)*100:.1f}%)")
    
    else:
        print("❌ Aucun résultat d'analyse de topics")

if __name__ == "__main__":
    process_topic_extraction()