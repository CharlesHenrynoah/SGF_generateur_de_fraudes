# Guide de Démarrage Rapide

## Installation en 3 étapes

### 1. Installer les dépendances

```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurer les credentials

Le fichier `.env` est déjà créé avec votre clé OpenAI. Il vous reste à configurer Supabase :

1. Ouvrez votre dashboard Supabase
2. Allez dans Settings > Database
3. Copiez la connection string
4. Allez dans Settings > API
5. Copiez l'URL et les clés

Éditez le fichier `.env` et remplissez :
```env
DATABASE_URL=postgresql://postgres:[PASSWORD]@db.xxxxx.supabase.co:5432/postgres
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_SERVICE_KEY=your-supabase-service-key
```

### 3. Créer les tables dans Supabase

Exécutez le script SQL dans votre Supabase SQL Editor :

```sql
-- Copiez le contenu de db/init.sql et exécutez-le dans Supabase
```

Ou via psql :
```bash
psql "postgresql://postgres:[PASSWORD]@db.xxxxx.supabase.co:5432/postgres" -f db/init.sql
```

## Utilisation de la CLI

### Mode interactif (recommandé)

```bash
python cli.py
```

La CLI vous posera des questions interactives pour configurer la génération.

### Mode avec paramètres

```bash
# Générer 1000 transactions avec 10% de fraude
python cli.py --count 1000 --fraud-ratio 0.1

# Générer 5000 transactions en EUR pour la France
python cli.py --count 5000 --fraud-ratio 0.15 --currency EUR --countries "FR"

# Test rapide sans sauvegarde
python cli.py --count 100 --no-save --no-s3 --no-kafka
```

## Exemples d'utilisation

### Génération basique
```bash
python cli.py -c 2000 -r 0.12
```

### Génération avec scénarios spécifiques
1. Lancez `python cli.py`
2. Sélectionnez les scénarios (ex: `1,3,5` pour card_testing, identity_theft, money_laundering)
3. Configurez les autres paramètres

### Génération avec plage de dates
1. Lancez `python cli.py`
2. Répondez "o" à "Utiliser une plage de dates spécifique?"
3. Entrez les dates au format YYYY-MM-DD

## Coûts OpenAI

Avec `gpt-4o-mini` (modèle économique) :
- **Input** : $0.15 par million de tokens
- **Output** : $0.60 par million de tokens

Estimation pour 1000 transactions :
- ~50,000 tokens input
- ~200,000 tokens output
- **Coût approximatif** : ~$0.13

Pour 10,000 transactions : ~$1.30
Pour 100,000 transactions : ~$13.00

## Dépannage

### Erreur "OPENAI_API_KEY non configurée"
Vérifiez que le fichier `.env` contient bien votre clé API.

### Erreur de connexion Supabase
Vérifiez que :
- L'URL Supabase est correcte
- La clé de service est valide
- Les tables existent (exécutez `db/init.sql`)

### Erreur "Module not found"
Installez les dépendances :
```bash
pip install -r requirements.txt
```

### Génération lente
C'est normal ! OpenAI a des rate limits. Pour accélérer :
- Réduisez `LLM_BATCH_SIZE` dans `.env` (mais augmente le nombre d'appels)
- Utilisez `--no-save --no-s3 --no-kafka` pour tester plus vite

## Prochaines étapes

- Consultez `CONFIGURATION.md` pour la configuration avancée
- Consultez `EXAMPLES.md` pour des exemples d'utilisation de l'API
- Consultez `ARCHITECTURE.md` pour comprendre l'architecture
