"""Script 01 - Collecte du corpus

Ce script fournit des templates et fonctions pour la collecte structurée
des offres d'emploi et référentiels officiels.
"""

import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from typing import Dict, List
import time


class OffreCollector:
    """Template pour collecter et structurer les offres d'emploi."""
    
    def __init__(self):
        self.offres = []
        
    def ajouter_offre_manuelle(self, 
                               id_offre: str,
                               pays: str,
                               role: str,
                               source: str,
                               entreprise: str,
                               url: str,
                               texte_brut: str,
                               langue: str) -> Dict:
        """Ajoute une offre collectée manuellement.
        
        Args:
            id_offre: Identifiant unique (ex: FR_PO_001)
            pays: France, USA, Allemagne, Japon
            role: Product Owner ou Scrum Master
            source: Indeed.fr, LinkedIn, etc.
            entreprise: Nom de l'entreprise
            url: URL de l'offre
            texte_brut: Texte des missions/responsabilités uniquement
            langue: fr, en, de, ja
        """
        offre = {
            'id': id_offre,
            'pays': pays,
            'role': role,
            'source': source,
            'entreprise': entreprise,
            'url': url,
            'date_collecte': datetime.now().strftime('%Y-%m-%d'),
            'texte_brut': texte_brut.strip(),
            'langue': langue,
            'type': 'offre'
        }
        self.offres.append(offre)
        print(f"✅ Offre ajoutée: {id_offre} - {entreprise} ({pays})")
        return offre
    
    def extraire_scrum_guide(self, url: str = "https://scrumguides.org/scrum-guide.html") -> List[Dict]:
        """Extrait les définitions PO et SM du Scrum Guide.
        
        IMPORTANT: À compléter manuellement pour garantir la précision.
        Copiez les sections exactes depuis scrumguides.org
        """
        print("⚠️  Extraction manuelle recommandée pour le Scrum Guide")
        print(f"   Visitez: {url}")
        print("   Copiez les sections 'Product Owner' et 'Scrum Master'")
        
        # Template à remplir manuellement
        scrum_guide_refs = [
            {
                'id': 'SG_PO_001',
                'pays': 'Neutral',
                'role': 'Product Owner',
                'source': 'Scrum Guide 2020',
                'entreprise': '',
                'url': url,
                'date_collecte': datetime.now().strftime('%Y-%m-%d'),
                'texte_brut': """[À REMPLIR: Coller ici la définition complète du Product Owner 
du Scrum Guide 2020, incluant toutes les responsabilités listées]""",
                'langue': 'en',
                'type': 'reference'
            },
            {
                'id': 'SG_SM_001',
                'pays': 'Neutral',
                'role': 'Scrum Master',
                'source': 'Scrum Guide 2020',
                'entreprise': '',
                'url': url,
                'date_collecte': datetime.now().strftime('%Y-%m-%d'),
                'texte_brut': """[À REMPLIR: Coller ici la définition complète du Scrum Master 
du Scrum Guide 2020, incluant toutes les responsabilités listées]""",
                'langue': 'en',
                'type': 'reference'
            }
        ]
        return scrum_guide_refs
    
    def ajouter_referentiel_officiel(self,
                                     id_ref: str,
                                     pays: str,
                                     role: str,
                                     source: str,
                                     url: str,
                                     texte_brut: str,
                                     langue: str) -> Dict:
        """Ajoute un référentiel officiel (ROME, O*NET, etc.)."""
        ref = {
            'id': id_ref,
            'pays': pays,
            'role': role,
            'source': source,
            'entreprise': '',
            'url': url,
            'date_collecte': datetime.now().strftime('%Y-%m-%d'),
            'texte_brut': texte_brut.strip(),
            'langue': langue,
            'type': 'referentiel'
        }
        self.offres.append(ref)
        print(f"✅ Référentiel ajouté: {id_ref} ({source})")
        return ref
    
    def sauvegarder_csv(self, filepath: str = 'data/raw/offres_collectees.csv'):
        """Sauvegarde les offres collectées en CSV."""
        df = pd.DataFrame(self.offres)
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"\n💾 {len(self.offres)} entrées sauvegardées dans {filepath}")
        print("\nRépartition:")
        print(df.groupby(['pays', 'role', 'type']).size())
        return df
    
    def verifier_equilibre(self) -> pd.DataFrame:
        """Vérifie l'équilibre du corpus collecté."""
        df = pd.DataFrame(self.offres)
        print("\n=== Vérification de l'équilibre du corpus ===")
        print("\nPar pays et rôle:")
        print(df.groupby(['pays', 'role']).size())
        print("\nPar type:")
        print(df.groupby('type').size())
        print("\nPar langue:")
        print(df.groupby('langue').size())
        
        # Objectifs
        print("\n📊 Objectifs Phase 1:")
        print("   - Scrum Guide: 2 textes (PO + SM) ✓")
        print("   - Référentiels: 8 fiches minimum (4 pays × 2 rôles)")
        print("   - Offres: 80 minimum (4 pays × 2 rôles × 10)")
        print(f"   - Total actuel: {len(df)} textes")
        
        return df


# ============================================================================
# EXEMPLES D'UTILISATION
# ============================================================================

if __name__ == "__main__":
    
    # Initialiser le collecteur
    collector = OffreCollector()
    
    # ========================================================================
    # EXEMPLE 1: Ajouter une offre française
    # ========================================================================
    collector.ajouter_offre_manuelle(
        id_offre="FR_PO_001",
        pays="France",
        role="Product Owner",
        source="Indeed.fr",
        entreprise="Société Générale",
        url="https://fr.indeed.com/...",
        texte_brut="""Le Product Owner est responsable de la définition, 
        de la conception et de la livraison d'un produit. Il représente les 
        besoins métier et fait le lien avec les équipes techniques du projet. 
        Il définit la roadmap produit et priorise le backlog.""",
        langue="fr"
    )
    
    # ========================================================================
    # EXEMPLE 2: Ajouter une offre américaine
    # ========================================================================
    collector.ajouter_offre_manuelle(
        id_offre="US_PO_001",
        pays="USA",
        role="Product Owner",
        source="Indeed.com",
        entreprise="Google",
        url="https://www.indeed.com/...",
        texte_brut="""The Product Owner is responsible for maximizing the value 
        of the product resulting from the work of the Development Team. 
        Works with stakeholders to define product vision and priorities.""",
        langue="en"
    )
    
    # ========================================================================
    # EXEMPLE 3: Ajouter un référentiel officiel
    # ========================================================================
    collector.ajouter_referentiel_officiel(
        id_ref="FR_REF_PO_001",
        pays="France",
        role="Product Owner",
        source="France Travail (ROME)",
        url="https://candidat.francetravail.fr/metierscope/...",
        texte_brut="""[Coller ici le texte complet de la fiche métier ROME]""",
        langue="fr"
    )
    
    # ========================================================================
    # Vérifier l'état de la collecte
    # ========================================================================
    collector.verifier_equilibre()
    
    # ========================================================================
    # Sauvegarder
    # ========================================================================
    # collector.sauvegarder_csv('data/raw/offres_collectees.csv')
    
    print("\n" + "="*70)
    print("📝 PROCHAINES ÉTAPES:")
    print("="*70)
    print("1. Visitez https://scrumguides.org et copiez les définitions PO/SM")
    print("2. Collectez les référentiels officiels (ROME, O*NET, BERUFENET, IPA)")
    print("3. Collectez 10-15 offres par pays et par rôle")
    print("4. Utilisez ce script pour structurer vos données")
    print("5. Exécutez collector.sauvegarder_csv() quand vous avez 20+ textes")
    print("\n💡 ASTUCE: Collectez en batch de 10 offres, sauvegardez, vérifiez.")
    print("="*70)