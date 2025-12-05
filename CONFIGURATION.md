# Guide de Configuration

## Configuration OpenAI

1. **Créer un fichier `.env`** à partir de `.env.example` :
```bash
cp .env.example .env
```

2. **Configurer votre clé API OpenAI** dans le fichier `.env` :
```env
OPENAI_API_KEY=sk-... # Votre clé API OpenAI ici
OPENAI_MODEL=gpt-4o-mini  # Modèle économique recommandé
```

## Configuration Supabase

1. **Récupérer vos credentials Supabase** depuis votre dashboard :
   - URL du projet : `https://xxxxx.supabase.co`
   - Anon Key : Clé publique
   - Service Key : Clé privée (pour les opérations serveur)

2. **Récupérer la connection string PostgreSQL** :
   - Allez dans Settings > Database
   - Copiez la connection string (URI)

3. **Configurer dans `.env`** :
```env
DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.xxxxx.supabase.co:5432/postgres
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_SERVICE_KEY=your-supabase-service-key
```

## Modèles OpenAI recommandés (par coût)

| Modèle | Coût par 1M tokens (input) | Coût par 1M tokens (output) | Recommandation |
|--------|---------------------------|----------------------------|----------------|
| gpt-4o-mini | $0.15 | $0.60 | ✅ **Recommandé** - Économique |
| gpt-3.5-turbo | $0.50 | $1.50 | Alternative |
| gpt-4o | $2.50 | $10.00 | Plus cher, meilleure qualité |
| gpt-4-turbo | $10.00 | $30.00 | Très cher |

**Note** : `gpt-4o-mini` est le modèle le plus économique tout en offrant de bonnes performances.

## Vérification de la configuration

Testez votre configuration avec :
```bash
python cli.py --count 10 --no-save --no-s3 --no-kafka
```

Cela générera 10 transactions sans les sauvegarder, pour tester uniquement la génération OpenAI.
