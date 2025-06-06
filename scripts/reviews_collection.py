import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def main():
    # Configuration du navigateur headless
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)

    # Recherche Google Maps : banque + ville
    search_queries = [
        # Attijariwafa Bank
        "Attijariwafa bank Casablanca",
        "Attijariwafa bank Rabat",
        "Attijariwafa bank Marrakech",
        "Attijariwafa bank Fès",
        "Attijariwafa bank Tanger",
        "Attijariwafa bank Agadir",
        "Attijariwafa bank Meknès",
        "Attijariwafa bank Oujda",
        "Attijariwafa bank Kénitra",
        "Attijariwafa bank Laâyoune",

        # Banque Populaire
        "Banque Populaire Casablanca",
        "Banque Populaire Rabat",
        "Banque Populaire Marrakech",
        "Banque Populaire Fès",
        "Banque Populaire Tanger",
        "Banque Populaire Agadir",
        "Banque Populaire Meknès",
        "Banque Populaire Oujda",
        "Banque Populaire Kénitra",
        "Banque Populaire Laâyoune",

        # CIH Bank
        "CIH Bank Casablanca",
        "CIH Bank Rabat",
        "CIH Bank Marrakech",
        "CIH Bank Fès",
        "CIH Bank Tanger",
        "CIH Bank Agadir",
        "CIH Bank Meknès",
        "CIH Bank Oujda",
        "CIH Bank Kénitra",
        "CIH Bank Laâyoune",

        # BMCE / Bank of Africa
        "BMCE Casablanca",
        "BMCE Rabat",
        "BMCE Marrakech",
        "BMCE Fès",
        "BMCE Tanger",
        "BMCE Agadir",
        "BMCE Meknès",
        "BMCE Oujda",
        "BMCE Kénitra",
        "BMCE Laâyoune",

        # Crédit du Maroc
        "Crédit du Maroc Casablanca",
        "Crédit du Maroc Rabat",
        "Crédit du Maroc Marrakech",
        "Crédit du Maroc Fès",
        "Crédit du Maroc Tanger",
        "Crédit du Maroc Agadir",
        "Crédit du Maroc Meknès",
        "Crédit du Maroc Oujda",
        "Crédit du Maroc Kénitra",
        "Crédit du Maroc Laâyoune",

        # Al Barid Bank
        "Al Barid Bank Casablanca",
        "Al Barid Bank Rabat",
        "Al Barid Bank Marrakech",
        "Al Barid Bank Fès",
        "Al Barid Bank Tanger",
        "Al Barid Bank Agadir",
        "Al Barid Bank Meknès",
        "Al Barid Bank Oujda",
        "Al Barid Bank Kénitra",
        "Al Barid Bank Laâyoune",

        # Société Générale Maroc
        "Société Générale Casablanca",
        "Société Générale Rabat",
        "Société Générale Marrakech",
        "Société Générale Fès",
        "Société Générale Tanger",
        "Société Générale Agadir",
        "Société Générale Meknès",
        "Société Générale Oujda",
        "Société Générale Kénitra",
        "Société Générale Laâyoune",

        # Bank Al-Maghrib (Banque centrale)
        "Bank Al-Maghrib Casablanca",
        "Bank Al-Maghrib Rabat",
        "Bank Al-Maghrib Marrakech",
        "Bank Al-Maghrib Fès",
        "Bank Al-Maghrib Tanger",
        "Bank Al-Maghrib Agadir",
        "Bank Al-Maghrib Meknès",
        "Bank Al-Maghrib Oujda",
        "Bank Al-Maghrib Kénitra",
        "Bank Al-Maghrib Laâyoune"
    ]

    urls_data = []
    print("Récupération des URLs d'agences...")
    for query in search_queries:
        search_url = f"https://www.google.com/maps/search/ {query.replace(' ', '+')}"
        driver.get(search_url)
        time.sleep(5)

        # Scroll pour charger plus de résultats
        scroll_area_selector = 'div[role="main"]'
        for _ in range(20):  
            driver.execute_script(f"document.querySelector('{scroll_area_selector}').scrollTop += 1000")
            time.sleep(1.5)

        # Extraire les liens des agences
        places = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
        for place in places:
            name = place.get_attribute('aria-label')
            url = place.get_attribute('href')
            if name and url:
                urls_data.append({'bank': query.split()[0], 'query': query, 'agency': name, 'url': url})

    urls_df = pd.DataFrame(urls_data).drop_duplicates(subset="url")
    urls_df.to_csv("bank_agency_urls.csv", index=False)
    print(f"{len(urls_df)} URLs enregistrées.")

    all_reviews = []
    agency_details = []
    total_reviews = 0
    print("\nRécupération des avis clients et informations des agences...")

    for _, row in urls_df.iterrows():
        print(f"\n{row['agency']}")
        driver.get(row['url'])
        time.sleep(5)

        location = "Non disponible"
        try:
            location_buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='adresse' i], button[aria-label*='Adresse' i]")
            if location_buttons:
                location = location_buttons[0].text
            else:
                location_elements = driver.find_elements(By.CSS_SELECTOR, "div.Io6YTe fontsize-normal, div.rogA2c")
                if location_elements:
                    location = location_elements[0].text
                else:
                    all_buttons = driver.find_elements(By.CSS_SELECTOR, "button.CsEnBe")
                    for button in all_buttons:
                        text = button.text
                        if any(c.isdigit() for c in text) and len(text.split()) > 2:
                            location = text
                            break
            # Nettoyage de l'adresse
            if location != "Non disponible":
                location = location.replace("Adresse : ", "").replace("Adresse:", "").strip()
                location = ", ".join([l.strip() for l in location.split("\n") if l.strip()])
        except Exception as e:
            print(f"Erreur lors de la récupération de l'adresse: {e}")

        agency_details.append({
            "bank": row["bank"],
            "agency": row["agency"],
            "url": row["url"],
            "location": location
        })
        print(f"Adresse trouvée: {location}")

        # Cliquer sur le bouton d'avis
        try:
            try:
                button = driver.find_element(By.CLASS_NAME, "hh2c6")
                button.click()
            except NoSuchElementException:
                buttons = driver.find_elements(By.CSS_SELECTOR, "button.fontBodyMedium")
                for btn in buttons:
                    if "avis" in btn.text.lower():
                        btn.click()
                        break
                else:
                    driver.find_element(By.CSS_SELECTOR, "button[data-item-id*='review']").click()
            print("Bouton d'avis cliqué avec succès")
            time.sleep(5)
        except Exception as e:
            print(f"Bouton d'avis introuvable: {e}")
            continue

        try:
            all_review_buttons = driver.find_elements(By.CSS_SELECTOR, "button.fontBodyMedium")
            for btn in all_review_buttons:
                if "tous les avis" in btn.text.lower() or "voir plus" in btn.text.lower():
                    print("Bouton 'Tous les avis' trouvé et cliqué")
                    btn.click()
                    time.sleep(3)
                    break
        except:
            pass

        scroll_count = 0
        max_scrolls = 50 
        last_review_count = 0
        stagnation_count = 0

        try:
            scrollable = None
            possible_scrollables = [
                "//div[@aria-label='Avis']", 
                "//div[contains(@aria-label, 'avis')]",
                "//div[@role='dialog']//div[contains(@class, 'section-scrollbox')]"
            ]
            for selector in possible_scrollables:
                try:
                    scrollable = driver.find_element(By.XPATH, selector)
                    print(f"Conteneur de scroll trouvé avec: {selector}")
                    break
                except:
                    continue
            if not scrollable:
                print("Aucun conteneur de scroll trouvé")
                scrollable = driver.find_element(By.TAG_NAME, "body")  

            while scroll_count < max_scrolls:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                time.sleep(2)  
                current_reviews = driver.find_elements(By.CLASS_NAME, "jftiEf")
                print(f"Scroll {scroll_count+1}/{max_scrolls}: {len(current_reviews)} avis trouvés")
                if len(current_reviews) == last_review_count:
                    stagnation_count += 1
                    if stagnation_count >= 7:  
                        print("Aucun nouvel avis chargé après plusieurs tentatives, arrêt du scroll")
                        break
                else:
                    stagnation_count = 0  
                last_review_count = len(current_reviews)
                scroll_count += 1
        except Exception as e:
            print(f"Erreur pendant le scroll: {e}")

        reviews = []
        for selector in ["jftiEf", "gws-localreviews__google-review"]:
            try:
                reviews = driver.find_elements(By.CLASS_NAME, selector)
                if reviews:
                    print(f"Avis trouvés avec le sélecteur: {selector}")
                    break
            except:
                continue

        agency_review_count = 0
        print(f"Tentative d'extraction de {len(reviews)} avis")

        for review in reviews:
            try:
                author = "Anonyme"
                rating = "Non spécifié"
                date = "Non spécifié"
                text = "Non spécifié"

                try:
                    author_selectors = ["d4r55", "review__author", "section-review-title"]
                    for selector in author_selectors:
                        try:
                            author_element = review.find_element(By.CLASS_NAME, selector)
                            author = author_element.text.strip()
                            if author:
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"Erreur extraction auteur: {type(e).__name__}")

                try:
                    rating_selectors = [
                        (By.CLASS_NAME, "kvMYJc"),
                        (By.CSS_SELECTOR, "[aria-label*='étoile']"),
                        (By.CSS_SELECTOR, "[aria-label*='star']")
                    ]
                    for by, selector in rating_selectors:
                        try:
                            rating_element = review.find_element(by, selector)
                            rating = rating_element.get_attribute("aria-label")
                            if rating:
                                break
                        except:
                            continue
                    if rating != "Non spécifié":
                        import re
                        rating_match = re.search(r"(\d+[.,]?\d*)", rating)
                        if rating_match:
                            rating = rating_match.group(1).replace(",", ".")
                except Exception as e:
                    print(f"Erreur extraction note: {type(e).__name__}")

                try:
                    date_selectors = ["rsqaWe", "section-review-publish-date"]  
                    for selector in date_selectors:
                        try:
                            date_element = review.find_element(By.CLASS_NAME, selector)
                            date = date_element.text.strip()
                            if date:
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"Erreur extraction date: {type(e).__name__}")

                try:
                    text_selectors = ["wiI7pd", "section-review-text", "review-full-text"]
                    for selector in text_selectors:
                        try:
                            # Vérifier d'abord s'il y a un bouton "Plus"
                            try:
                                more_buttons = review.find_elements(By.CSS_SELECTOR, "button.w8nwRe, button.review-more-link")
                                for btn in more_buttons:
                                    if "plus" in btn.text.lower() or "more" in btn.text.lower():
                                        btn.click()
                                        time.sleep(0.5)
                                        break
                            except:
                                pass

                            text_element = review.find_element(By.CLASS_NAME, selector)
                            text = text_element.text.strip()
                            if text:
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"Erreur extraction texte: {type(e).__name__}")

                if author != "Anonyme" or text != "Non spécifié":
                    all_reviews.append({
                        "bank": row["bank"],
                        "agency": row["agency"],
                        "url": row["url"],
                        "location": location,
                        "author": author,
                        "rating": rating,
                        "date": date,
                        "text": text
                    })
                    agency_review_count += 1
            except Exception as e:
                print(f"Erreur complète sur un avis: {type(e).__name__}")
                continue

        print(f"{agency_review_count} avis récupérés.")
        total_reviews += agency_review_count

    df_reviews = pd.DataFrame(all_reviews)
    df_reviews.to_csv("bank_reviews.csv", index=False, encoding="utf-8-sig")
    print(f"\nTotal des avis collectés : {total_reviews}")
    print(f"Total des agences analysées : {len(agency_details)}")
    print("Avis sauvegardés dans bank_reviews.csv")
    driver.quit()

if __name__ == "__main__":
    main()