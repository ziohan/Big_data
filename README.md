# Big_data

Travaux pratiques du cours de **Big Data** (INFO FISE 5A) : du comptage de mots en Python jusqu'au traitement distribué d'un jeu de données astronomique avec **PySpark** et **Hadoop**.

**Auteur :** Johan Balduzzi

## Sommaire

| TP | Sujet | Notebook | Outils |
|---|---|---|---|
| TP1 | Comptage de mots : Python, pseudo-MapReduce, Hadoop, Spark | [`comptage_mot.ipynb`](comptage_mot.ipynb) | Python, HDFS / Hadoop, PySpark (RDD) |
| TP2 | Étude d'un jeu de données d'accidents de la route avec PySpark | [`PySpark.ipynb`](PySpark.ipynb) | PySpark (RDD, DataFrame), pandas, matplotlib |
| TP3a | Petite base de données : classification des étoiles (`star_analysis`) | [`stars-data-analysis-pipeline.ipynb`](stars-data-analysis-pipeline.ipynb) | pandas, scikit-learn, seaborn |
| TP3b | Grosse base de données : exoplanètes Kepler (`kepler`) | [`ml-kepler-exoplanet-search-results-pyspark.ipynb`](ml-kepler-exoplanet-search-results-pyspark.ipynb) | scikit-learn, PySpark, Spark ML, Hadoop / YARN |

---

## TP1 : Comptage de mots

Objectif : comparer plusieurs façons de compter les occurrences des mots d'un fichier `mots.txt` (15 mots générés aléatoirement) et voir comment le temps d'exécution évolue avec la taille des données.

Quatre versions sont implémentées :

1. **Python simple** : dictionnaire de comptage.
2. **Pseudo-MapReduce** : fonctions `mapper`, `shuffle` et `reducer` écrites à la main.
3. **Hadoop** : `WordCount` exécuté sur HDFS (`hadoop-mapreduce-examples`).
4. **Spark** : `flatMap` → `map` → `reduceByKey` sur un RDD.

Les mesures sont faites en doublant à chaque fois la taille du fichier (facteurs 1, 2, 4 et 8), avec 3 essais par version.

**Ce qu'on observe :**

- Python simple et pseudo-MapReduce restent de l'ordre de quelques millisecondes.
- Spark tourne autour de 25 à 40 ms et Hadoop autour de 1,2 à 1,5 s, quelle que soit la taille testée.
- Le volume reste trop faible pour faire ressortir l'intérêt de Hadoop et de Spark : leur coût de démarrage domine. Ils deviennent pertinents sur de gros volumes de données.

## TP2 : Analyse avec PySpark

Reproduction d'une étude de cas sur des **données d'accidents de la route** (fichiers CSV `units_data/` et `crash_data/`) avec PySpark.

Ce qui est vu :

- **RDD** : création (`parallelize`, `textFile`), suppression des en-têtes, partitionnement personnalisé (`partitionBy`, véhicules immatriculés en Australie-Méridionale ou non), âge moyen par sexe, véhicules les plus anciens et les plus récents.
- **DataFrame** : lecture de CSV, filtres, tris, agrégations (`groupBy`, `agg`), jointure des deux tables.
- **Questions traitées** : accidents à Adelaide avec plus de 3 victimes, top 10 des accidents les plus graves, décès par type d'accident, victimes par commune pour les conducteurs sans permis, répartition de la gravité selon la consommation de drogues et/ou d'alcool.
- **Visualisation** : conversion en pandas (`toPandas`) pour le tableau récapitulatif.

## TP3 : Pipeline de data science

Les deux parties suivent le même pipeline : **Data → EDA → Prétraitement → ML → Évaluation → Déploiement**. La première utilise une petite base (scikit-learn), la seconde une base plus grosse et ajoute une version distribuée avec Spark.

### TP3a : `star_analysis` (petite base)

- **Données :** 240 étoiles (dont 239 exploitables), 6 types à prédire (Brown Dwarf, Red Dwarf, White Dwarf, Main Sequence, Supergiant, Hypergiant). Fichier : `cleaned_star_data.csv`.
- **Data / EDA :** détection des vraies valeurs manquantes (cases contenant un espace), diagrammes de Hertzsprung-Russell, échelles logarithmiques, corrélations, test de cohérence physique (loi de Stefan-Boltzmann), PCA.
- **Prétraitement :** découpage 80/20 stratifié, `Pipeline` scikit-learn sans fuite de données (log10, imputation itérative, standardisation, one-hot).
- **ML :** baseline + 5 modèles (régression logistique, SVM, k-NN, Random Forest, Gradient Boosting) en validation croisée, réglage par `GridSearchCV`, K-means pour vérifier la structure sans étiquettes.
- **Évaluation :** métriques sur le jeu de test, matrice de confusion, importance par permutation, analyse des erreurs et limites.
- **Déploiement :** sauvegarde du modèle, module `star_predictor.py`, application Streamlit et `requirements.txt`.

### TP3b : `kepler` (grosse base)

- **Données :** `cumulative.csv` de la NASA Exoplanet Archive (Kepler Objects of Interest) : 9 564 signaux × 50 colonnes. Cible : `koi_disposition` (CONFIRMED, CANDIDATE ou FALSE POSITIVE).
- **Partie 1, scikit-learn :** dictionnaire des données, exploration, classification, régression, détection d'anomalies, PCA, clustering.
- **Partie 2, traitement distribué avec PySpark :**
  - chargement, audit des valeurs manquantes et nettoyage ;
  - agrégations avec l'API DataFrame puis Spark SQL ;
  - classification **CONFIRMED / FALSE POSITIVE** avec Spark ML (`Pipeline`, `Imputer`, `RandomForestClassifier`, `CrossValidator`) ;
  - scoring des `CANDIDATE` par probabilité d'être une vraie planète ;
  - sauvegarde au format Parquet.
- **Choix méthodologiques :** exclusion des colonnes qui provoquent une fuite de données (`koi_score`, `koi_pdisposition`, `koi_fpflag_*`), découpage train/test par étoile (`kepid`), poids de classe pour le déséquilibre, imputation dans le pipeline.
- **Exécution sur Hadoop :** le notebook décrit le passage sur un cluster (données dans HDFS, `master("yarn")`, soumission avec `spark-submit`). Le fichier fait seulement quelques Mo, donc Spark n'est pas indispensable ici : l'intérêt est d'avoir un pipeline identique en local et sur cluster.

---

## Structure du dépôt

```
Big_data/
├── TP1
    ├── comptage_mot.ipynb
    ├── mots.txt                        
├── TP2
    ├── PySpark.ipynb
    ├── units_data/
    ├── crash_data/
├── TP3_ID_28
    ├── small_data
        ├── stars-data-analysis-pipeline.ipynb          
        ├── cleaned_star_data.csv
    ├── big_data                       
        ├── ml-kepler-exoplanet-search-results-pyspark.ipynb  
        └── cumulative.csv                              
```

> Adapter cette arborescence si les fichiers de données sont rangés autrement.

## Prérequis

- **Python 3.9+** et Jupyter (ou Google Colab / Kaggle)
- **Java 8, 11 ou 17** (nécessaire à Spark)
- `pyspark==3.5.3`
- `numpy`, `pandas`, `matplotlib`, `seaborn`, `scikit-learn`, `scipy`
- **Hadoop** (HDFS + commande `hadoop`) uniquement pour la partie Hadoop du TP1 et l'exécution sur cluster du TP3b
- `streamlit` pour l'application du TP3a (optionnel)

## Installation

```bash
git clone https://github.com/ziohan/Big_data.git
cd Big_data

python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate

pip install pyspark==3.5.3 numpy pandas matplotlib seaborn scikit-learn scipy jupyter
```

## Utilisation

1. Placer les fichiers de données à côté des notebooks (voir la structure ci-dessus).
2. Lancer Jupyter :
   ```bash
   jupyter notebook
   ```
3. Ouvrir le notebook du TP voulu et exécuter les cellules dans l'ordre.

Pour la partie Hadoop du TP1, démarrer HDFS avant d'exécuter les cellules `!hdfs dfs ...` et `!hadoop jar ...`.

## Notes

- Les temps du TP1 dépendent de la machine : ils servent à comparer les méthodes entre elles, pas à donner des valeurs absolues.
- Le jeu `stars` est équilibré et très propre : les scores obtenus ne se généraliseraient pas tels quels à un vrai catalogue astronomique.
- Sur Kepler, les prédictions sur les `CANDIDATE` sont un outil de priorisation, pas une confirmation.
