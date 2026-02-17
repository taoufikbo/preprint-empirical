"""Script 03 - Analyse et visualisation

Ce script effectue l'analyse statistique et génère les visualisations
pour valider les hypothèses du cadre Todd-Hofstede.
"""

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy.stats import kruskal, mannwhitneyu
from itertools import combinations
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import umap
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuration plot
plt.rcParams['figure.dpi'] = 200
plt.rcParams['font.size'] = 11
sns.set_style('whitegrid')


class AnalyseurSemantique:
    """Analyseur pour valider les hypothèses Todd-Hofstede."""
    
    def __init__(self):
        self.embeddings = None
        self.metadata = None
        self.pays_list = ['France', 'USA', 'Allemagne', 'Japon']
        self.color_map = {
            'France': '#0055A4',
            'USA': '#B22234',
            'Allemagne': '#000000',
            'Japon': '#BC002D',
            'Neutral': '#808080'
        }
    
    def charger_donnees(self,
                       emb_path: str = 'embeddings/embeddings_bge_m3.npy',
                       meta_path: str = 'embeddings/metadata.csv'):
        """Charge les embeddings et métadonnées."""
        self.embeddings = np.load(emb_path)
        self.metadata = pd.read_csv(meta_path)
        
        print(f"✅ Données chargées:")
        print(f"   Embeddings: {self.embeddings.shape}")
        print(f"   Métadonnées: {len(self.metadata)} entrées")
        print(f"\nRépartition par pays:")
        print(self.metadata['pays'].value_counts())
    
    def calculer_matrice_similarite(self) -> pd.DataFrame:
        """Calcule la matrice de similarité cosinus inter/intra-pays."""
        print("\n=== Calcul de la matrice de similarité ===")
        
        # Indices par pays
        indices = {p: self.metadata[self.metadata['pays'] == p].index.tolist() 
                   for p in self.pays_list}
        
        # Matrice de similarité
        sim_matrix = pd.DataFrame(index=self.pays_list, 
                                 columns=self.pays_list, 
                                 dtype=float)
        
        for p1 in self.pays_list:
            for p2 in self.pays_list:
                emb1 = self.embeddings[indices[p1]]
                emb2 = self.embeddings[indices[p2]]
                sim = cosine_similarity(emb1, emb2)
                
                if p1 == p2:
                    # Intra-pays: moyenne hors diagonale
                    np.fill_diagonal(sim, np.nan)
                    sim_matrix.loc[p1, p2] = np.nanmean(sim)
                else:
                    # Inter-pays: moyenne de toutes les similarités
                    sim_matrix.loc[p1, p2] = sim.mean()
        
        print("\n📊 Matrice de similarité cosinus:")
        print(sim_matrix.round(3))
        
        # Sauvegarder
        Path('results').mkdir(exist_ok=True)
        sim_matrix.to_csv('results/matrice_similarite.csv')
        print("\n✅ Matrice sauvegardée: results/matrice_similarite.csv")
        
        return sim_matrix
    
    def analyser_distance_scrum_guide(self) -> pd.DataFrame:
        """Calcule la distance de chaque pays au Scrum Guide."""
        print("\n=== Analyse distance au Scrum Guide ===")
        
        # Embedding du Scrum Guide (référence neutre)
        sg_idx = self.metadata[self.metadata['source'].str.contains('Scrum Guide', na=False)].index.tolist()
        
        if len(sg_idx) == 0:
            print("⚠️  Scrum Guide non trouvé dans les métadonnées")
            return None
        
        sg_embedding = self.embeddings[sg_idx].mean(axis=0, keepdims=True)
        
        # Calculer les distances par pays
        resultats = []
        
        for pays in self.pays_list:
            idx_pays = self.metadata[self.metadata['pays'] == pays].index.tolist()
            emb_pays = self.embeddings[idx_pays]
            
            # Similarités cosinus avec le Scrum Guide
            similarities = cosine_similarity(emb_pays, sg_embedding).flatten()
            
            resultats.append({
                'pays': pays,
                'similarite_moyenne': similarities.mean(),
                'similarite_std': similarities.std(),
                'n_textes': len(similarities)
            })
            
            print(f"{pays:12s} → Scrum Guide : {similarities.mean():.3f} (σ={similarities.std():.3f})")
        
        df_resultats = pd.DataFrame(resultats)
        df_resultats.to_csv('results/distances_scrum_guide.csv', index=False)
        
        return df_resultats
    
    def test_statistique_significativite(self) -> dict:
        """Teste si les différences entre pays sont significatives."""
        print("\n=== Tests statistiques ===")
        
        # Embedding Scrum Guide
        sg_idx = self.metadata[self.metadata['source'].str.contains('Scrum Guide', na=False)].index.tolist()
        sg_embedding = self.embeddings[sg_idx].mean(axis=0, keepdims=True)
        
        # Distances au Scrum Guide par pays
        distances_par_pays = {}
        for pays in self.pays_list:
            idx_pays = self.metadata[self.metadata['pays'] == pays].index.tolist()
            emb_pays = self.embeddings[idx_pays]
            distances_par_pays[pays] = cosine_similarity(emb_pays, sg_embedding).flatten()
        
        # Test de Kruskal-Wallis (non-paramétrique)
        stat, p_value = kruskal(*distances_par_pays.values())
        
        print(f"\n🧠 Kruskal-Wallis H-test:")
        print(f"   H = {stat:.2f}")
        print(f"   p-value = {p_value:.4f}")
        
        if p_value < 0.05:
            print("   ✅ Les distances au Scrum Guide diffèrent significativement entre pays")
        else:
            print("   ⚠️  Pas de différence significative détectée")
        
        # Tests post-hoc par paires (Mann-Whitney U)
        print("\n🔍 Tests par paires (Mann-Whitney U):")
        print(f"{'Pays 1':<12} {'Pays 2':<12} {'U':>8} {'p-value':>10} {'Signif'}")
        print("-" * 60)
        
        resultats_paires = []
        for p1, p2 in combinations(self.pays_list, 2):
            stat, p = mannwhitneyu(distances_par_pays[p1], distances_par_pays[p2])
            sig = "✅ *" if p < 0.05 else "  ns"
            print(f"{p1:<12} {p2:<12} {stat:8.1f} {p:10.4f} {sig}")
            
            resultats_paires.append({
                'pays1': p1,
                'pays2': p2,
                'U_statistic': stat,
                'p_value': p,
                'significatif': p < 0.05
            })
        
        df_paires = pd.DataFrame(resultats_paires)
        df_paires.to_csv('results/tests_statistiques.csv', index=False)
        
        return {
            'kruskal_H': stat,
            'kruskal_p': p_value,
            'paires': df_paires
        }
    
    def visualiser_umap(self, figsize=(14, 10)):
        """Génère la visualisation UMAP 2D de l'espace sémantique."""
        print("\n=== Génération visualisation UMAP ===")
        
        # Réduction de dimension
        print("🔄 UMAP en cours...")
        reducer = umap.UMAP(
            n_components=2,
            metric='cosine',
            random_state=42,
            n_neighbors=10,
            min_dist=0.1
        )
        embedding_2d = reducer.fit_transform(self.embeddings)
        
        # Préparer les couleurs et markers
        colors = [self.color_map.get(self.metadata.loc[i, 'pays'], 'gray') 
                  for i in range(len(self.metadata))]
        
        marker_map = {'referentiel': 's', 'offre': 'o', 'reference': '*'}
        
        # Plot
        fig, ax = plt.subplots(figsize=figsize)
        
        for i in range(len(self.metadata)):
            pays = self.metadata.loc[i, 'pays']
            type_doc = self.metadata.loc[i, 'type'] if 'type' in self.metadata.columns else 'offre'
            marker = marker_map.get(type_doc, 'o')
            
            # Taille spéciale pour le Scrum Guide
            if 'Scrum Guide' in str(self.metadata.loc[i, 'source']):
                size = 200
                marker = '*'
                edgecolor = 'black'
                linewidth = 2
            else:
                size = 80
                edgecolor = 'white'
                linewidth = 0.5
            
            ax.scatter(
                embedding_2d[i, 0], 
                embedding_2d[i, 1],
                c=self.color_map.get(pays, 'gray'),
                marker=marker,
                s=size,
                alpha=0.7,
                edgecolors=edgecolor,
                linewidth=linewidth
            )
        
        # Légende
        patches = [mpatches.Patch(color=c, label=p) 
                   for p, c in self.color_map.items() if p in self.metadata['pays'].values]
        ax.legend(handles=patches, fontsize=12, loc='best', framealpha=0.9)
        
        ax.set_title(
            'Espace sémantique des rôles agiles par pays\n(BGE-M3 + UMAP)',
            fontsize=16,
            fontweight='bold'
        )
        ax.set_xlabel('UMAP Dimension 1', fontsize=12)
        ax.set_ylabel('UMAP Dimension 2', fontsize=12)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('results/clusters_umap.png', dpi=200, bbox_inches='tight')
        print("✅ Visualisation sauvegardée: results/clusters_umap.png")
        
        return fig, ax
    
    def generer_rapport(self, output_path: str = 'results/analyse.md'):
        """Génère un rapport markdown avec l'interprétation."""
        print("\n=== Génération du rapport ===")
        
        # Charger les résultats
        sim_matrix = pd.read_csv('results/matrice_similarite.csv', index_col=0)
        distances_sg = pd.read_csv('results/distances_scrum_guide.csv')
        
        rapport = f"""# Analyse sémantique - Résultats Phase 3

Date: {pd.Timestamp.now().strftime('%Y-%m-%d')}

## 1. Matrice de similarité inter/intra-pays

{sim_matrix.to_markdown()}

### Interprétation

- **Similarité intra-pays**: Les valeurs sur la diagonale indiquent la cohérence sémantique au sein d'un même pays.
- **Similarité inter-pays**: Les valeurs hors diagonale montrent la proximité entre pays.

## 2. Distance au Scrum Guide

{distances_sg.to_markdown(index=False)}

### Hypothèses prédites vs observées

| Pays | Hypothèse | Similarité prédite | Similarité observée | Validation |
|------|-----------|---------------------|----------------------|------------|
| USA | Rôle conforme au Scrum Guide | 0.70+ | {distances_sg[distances_sg['pays']=='USA']['similarite_moyenne'].values[0]:.3f} | À valider |
| Allemagne | Friction procédurale | 0.55-0.65 | {distances_sg[distances_sg['pays']=='Allemagne']['similarite_moyenne'].values[0]:.3f} | À valider |
| France | Réabsorption hiérarchique | 0.45-0.55 | {distances_sg[distances_sg['pays']=='France']['similarite_moyenne'].values[0]:.3f} | À valider |
| Japon | Procéduralisation | 0.40-0.50 | {distances_sg[distances_sg['pays']=='Japon']['similarite_moyenne'].values[0]:.3f} | À valider |

## 3. Visualisation

![Clusters UMAP](clusters_umap.png)

## 4. Conclusions provisoires

**À COMPLÉTER après examen des résultats:**

- [ ] Les offres américaines sont-elles les plus proches du Scrum Guide ?
- [ ] Les offres françaises montrent-elles un vocabulaire hiérarchique ?
- [ ] Les offres japonaises montrent-elles un vocabulaire procédural ?
- [ ] Les différences sont-elles statistiquement significatives ?

## 5. Limites

- Corpus exploratoire (n={len(self.metadata)} textes)
- BGE-M3 est un modèle général (pas spécialisé vocabulaire agile)
- Traduction implicite par embeddings multilingues (biais possibles)
- Les offres reflètent ce qui est *prescrit*, pas *pratiqué*

## 6. Prochaines étapes

1. Analyse qualitative des textes les plus représentatifs par pays
2. Questionnaire auprès des praticiens (validation terrain)
3. Intégration des résultats dans le preprint
"""        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(rapport)
        
        print(f"✅ Rapport sauvegardé: {output_path}")
        return rapport


# ============================================================================
# SCRIPT PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    
    # Initialiser l'analyseur
    analyseur = AnalyseurSemantique()
    
    # Charger les données
    analyseur.charger_donnees(
        emb_path='embeddings/embeddings_bge_m3.npy',
        meta_path='embeddings/metadata.csv'
    )
    
    # Étape 3.1: Matrice de similarité
    sim_matrix = analyseur.calculer_matrice_similarite()
    
    # Étape 3.2: Distance au Scrum Guide
    distances = analyseur.analyser_distance_scrum_guide()
    
    # Étape 3.3: Tests statistiques
    tests = analyseur.test_statistique_significativite()
    
    # Étape 3.4: Visualisation UMAP
    fig, ax = analyseur.visualiser_umap()
    
    # Étape 3.5: Rapport final
    rapport = analyseur.generer_rapport()
    
    print("\n" + "="*70)
    print("🎉 PHASE 3 TERMINÉE!")
    print("="*70)
    print("✅ Analyse complète")
    print("\n📁 Fichiers générés dans results/:")
    print("   - matrice_similarite.csv")
    print("   - distances_scrum_guide.csv")
    print("   - tests_statistiques.csv")
    print("   - clusters_umap.png")
    print("   - analyse.md")
    print("\n📝 PROCHAINES ÉTAPES:")
    print("1. Examiner results/analyse.md")
    print("2. Valider les hypothèses H1-H4")
    print("3. Intégrer dans le preprint")
    print("="*70)
