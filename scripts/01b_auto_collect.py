"""Script 01b - Collecte automatisée via SerpAPI (Google Jobs)

Ce script collecte automatiquement des offres d'emploi depuis Google Jobs
pour les 4 pays et 3 rôles ciblés (Product Owner, Scrum Master, Product Manager).
"""

import os
import pandas as pd
from serpapi import GoogleSearch
import time
from datetime import datetime
from typing import List, Dict

class GoogleJobsCollector:
    """Collecteur automatisé via Google Jobs API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.offres = []
        
        # Configuration par pays
        self.configs = {
            'France': {
                'google_domain': 'google.fr',
                'hl': 'fr',
                'gl': 'fr',
                'location': 'France',
                'langue': 'fr'
            },
            'USA': {
                'google_domain': 'google.com',
                'hl': 'en',
                'gl': 'us',
                'location': 'United States',
                'langue': 'en'
            },
            'Allemagne': {
                'google_domain': 'google.de',
                'hl': 'de',
                'gl': 'de',
                'location': 'Germany',
                'langue': 'de'
            },
            'Japon': {
                'google_domain': 'google.co.jp',
                'hl': 'ja',
                'gl': 'jp',
                'location': 'Japan',
                'langue': 'ja'
            }
        }
        
        # Requêtes par rôle et langue
        self.queries = {
            'France': {
                'Product Owner': 'Product Owner',
                'Scrum Master': 'Scrum Master',
                'Product Manager': 'Product Manager'
            },
            'USA': {
                'Product Owner': 'Product Owner',
                'Scrum Master': 'Scrum Master',
                'Product Manager': 'Product Manager'
            },
            'Allemagne': {
                'Product Owner': 'Product Owner',
                'Scrum Master': 'Scrum Master',
                'Product Manager': 'Product Manager'
            },
            'Japon': {
                'Product Owner': 'プロダクトオーナー OR Product Owner',
                'Scrum Master': 'スクラムマスター OR Scrum Master',
                'Product Manager': 'プロダクトマネージャー OR Product Manager'
            }
        }
        
        self.roles = ['Product Owner', 'Scrum Master', 'Product Manager']
    
    def collecter_offres(self, pays: str, role: str, num_offres: int = 10) -> List[Dict]:
        """Collecte des offres pour un pays/rôle donné."""
        
        config = self.configs[pays]
        query = self.queries[pays][role]
        
        print(f"\n🔍 Recherche : {pays} - {role}")
        print(f"   Query: '{query}'")
        
        params = {
            "api_key": self.api_key,
            "engine": "google_jobs",
            "q": query,
            "google_domain": config['google_domain'],
            "hl": config['hl'],
            "gl": config['gl'],
            "location": config['location'],
            "num": num_offres
        }
        
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if "error" in results:
                print(f"❌ Erreur API: {results['error']}")
                return []
            
            jobs = results.get("jobs_results", [])
            print(f"✅ {len(jobs)} offres récupérées")
            
            offres_extraites = []
            
            for idx, job in enumerate(jobs[:num_offres], 1):
                # Extraire les informations pertinentes
                description = job.get("description", "")
                
                # Nettoyer la description
                description_clean = self._nettoyer_description(description)
                
                # Identifier le rôle court pour l'ID
                role_code = {'Product Owner': 'PO', 'Scrum Master': 'SM', 'Product Manager': 'PM'}[role]
                
                offre = {
                    'id': f"{pays[:2].upper()}_{role_code}_{idx:03d}",
                    'pays': pays,
                    'role': role,
                    'source': 'Google Jobs',
                    'entreprise': job.get('company_name', 'N/A'),
                    'url': job.get('share_url', job.get('job_id', '')),
                    'date_collecte': datetime.now().strftime('%Y-%m-%d'),
                    'texte_brut': description_clean,
                    'langue': config['langue'],
                    'type': 'offre'
                }
                
                offres_extraites.append(offre)
                self.offres.append(offre)
                
                print(f"   [{idx}/{num_offres}] {job.get('company_name', 'N/A')[:40]}")
            
            # Rate limiting
            time.sleep(1)
            
            return offres_extraites
            
        except Exception as e:
            print(f"❌ Erreur lors de la collecte: {e}")
            return []
    
    def _nettoyer_description(self, description: str) -> str:
        """Nettoie la description (enlève HTML, garde uniquement les missions)."""
        import re
        from bs4 import BeautifulSoup
        
        # Enlever HTML
        soup = BeautifulSoup(description, 'html.parser')
        text = soup.get_text(separator='\n')
        
        # Enlever lignes vides multiples
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Extraire la section "Missions" / "Responsibilities"
        keywords = [
            'missions', 'responsabilités', 'responsabilities', 
            'your role', 'you will', 'aufgaben', '業務内容',
            'what you\'ll do', 'your responsibilities', 'vos missions'
        ]
        
        lines = text.split('\n')
        mission_start = -1
        
        for i, line in enumerate(lines):
            if any(kw in line.lower() for kw in keywords):
                mission_start = i
                break
        
        if mission_start > -1:
            # Prendre à partir de cette section (max 40 lignes pour avoir plus de contenu)
            text = '\n'.join(lines[mission_start:mission_start+40])
        
        return text.strip()
    
    def collecter_tout(self, num_par_pays_role: int = 10):
        """Collecte automatique pour tous les pays et rôles."""
        
        total_offres = num_par_pays_role * len(self.configs) * len(self.roles)
        
        print("="*70)
        print("🤖 COLLECTE AUTOMATISÉE - Google Jobs")
        print("="*70)
        print(f"Objectif: {num_par_pays_role} offres × {len(self.configs)} pays × {len(self.roles)} rôles = {total_offres} offres")
        print(f"Crédit API nécessaire: ~{total_offres} requêtes")
        print("="*70)
        
        for pays in self.configs.keys():
            print(f"\n{'='*70}")
            print(f"📍 PAYS: {pays}")
            print(f"{'='*70}")
            
            for role in self.roles:
                try:
                    self.collecter_offres(pays, role, num_par_pays_role)
                except Exception as e:
                    print(f"❌ Erreur pour {pays}/{role}: {e}")
                    continue
        
        print(f"\n✅ Collecte terminée: {len(self.offres)} offres")
    
    def sauvegarder(self, filepath: str = 'data/raw/offres_auto_collectees.csv'):
        """Sauvegarde les offres collectées."""
        df = pd.DataFrame(self.offres)
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        print(f"\n💾 {len(self.offres)} offres sauvegardées dans {filepath}")
        print("\nRépartition par pays et rôle:")
        print(df.groupby(['pays', 'role']).size())
        
        return df
    
    def afficher_stats(self):
        """Affiche les statistiques de collecte."""
        if len(self.offres) == 0:
            print("⚠️  Aucune offre collectée")
            return
        
        df = pd.DataFrame(self.offres)
        
        print("\n" + "="*70)
        print("📊 STATISTIQUES DE COLLECTE")
        print("="*70)
        
        print("\n1. Répartition par pays et rôle:")
        pivot = df.groupby(['pays', 'role']).size().unstack(fill_value=0)
        print(pivot)
        
        print("\n2. Total par pays:")
        print(df['pays'].value_counts().sort_index())
        
        print("\n3. Total par rôle:")
        print(df['role'].value_counts().sort_index())
        
        print("\n4. Top 10 entreprises:")
        print(df['entreprise'].value_counts().head(10))
        
        print(f"\n5. Longueur moyenne des descriptions (caractères):")
        df['longueur'] = df['texte_brut'].str.len()
        print(df.groupby(['pays', 'role'])['longueur'].mean().round(0))
        
        print(f"\n6. Taux de réussite:")
        print(f"   Offres collectées: {len(df)}")
        print(f"   Objectif: {10 * 4 * 3} (10 × 4 pays × 3 rôles)")
        print(f"   Taux: {len(df) / 120 * 100:.1f}%")


# ============================================================================
# UTILISATION
# ============================================================================

if __name__ == "__main__":
    
    # ⚠️ IMPORTANT: Remplacez par votre clé API SerpAPI
    # Créez un compte gratuit sur https://serpapi.com/
    API_KEY = os.environ.get("SERPAPI_KEY", "VOTRE_CLE_API_ICI")
    
    if API_KEY == "VOTRE_CLE_API_ICI":
        print("="*70)
        print("❌ ERREUR: Vous devez définir votre clé API SerpAPI")
        print("="*70)
        print("\n📝 ÉTAPES:")
        print("1. Allez sur https://serpapi.com/")
        print("2. Créez un compte gratuit (100 recherches/mois)")
        print("3. Copiez votre clé API")
        print("4. Option A: Remplacez 'VOTRE_CLE_API_ICI' dans ce script")
        print("5. Option B: Définissez la variable d'environnement:")
        print("   export SERPAPI_KEY='votre_cle_ici'")
        print("="*70)
        exit(1)
    
    # Initialiser le collecteur
    collector = GoogleJobsCollector(api_key=API_KEY)
    
    # MODE 1: Collecte complète automatique (120 offres)
    print("\n🚀 Lancement de la collecte automatique...")
    print("⏱️  Durée estimée: 3-5 minutes")
    print("💡 Vous pouvez interrompre avec Ctrl+C et relancer plus tard\n")
    
    collector.collecter_tout(num_par_pays_role=10)
    
    # MODE 2: Collecte pays par pays (plus de contrôle, commentez MODE 1 si vous utilisez MODE 2)
    # for pays in ['France', 'USA', 'Allemagne', 'Japon']:
    #     for role in ['Product Owner', 'Scrum Master', 'Product Manager']:
    #         collector.collecter_offres(pays, role, num_offres=10)
    #         collector.sauvegarder(f'data/raw/offres_{pays.lower()}.csv')  # Sauvegarde intermédiaire
    
    # Afficher les statistiques
    collector.afficher_stats()
    
    # Sauvegarder le fichier final
    df = collector.sauvegarder('data/raw/offres_google_jobs.csv')
    
    print("\n" + "="*70)
    print("🎉 COLLECTE TERMINÉE!")
    print("="*70)
    print("\n📝 PROCHAINES ÉTAPES:")
    print("1. Vérifier data/raw/offres_google_jobs.csv")
    print("2. Collecter manuellement:")
    print("   - 2 textes Scrum Guide (PO + SM)")
    print("   - 12 référentiels (4 pays × 3 rôles)")
    print("3. Fusionner tout dans data/corpus.csv")
    print("4. Exécuter scripts/02_embed.py pour les embeddings")
    print("="*70)