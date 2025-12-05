# Configuration Supabase

## Étape 1: Configurer le fichier .env

Éditez le fichier `.env` et remplacez `[YOUR_PASSWORD]` par votre mot de passe Supabase :

```env
DATABASE_URL=postgresql://postgres:[VOTRE_MOT_DE_PASSE]@db.puqpycrpolnjnbaezrja.supabase.co:5432/postgres
SUPABASE_URL=https://puqpycrpolnjnbaezrja.supabase.co
```

**Important** : Remplacez `[VOTRE_MOT_DE_PASSE]` par votre vrai mot de passe Supabase.

## Étape 2: Créer les tables dans Supabase

1. Ouvrez votre dashboard Supabase : https://supabase.com/dashboard
2. Sélectionnez votre projet
3. Allez dans **SQL Editor** (menu de gauche)
4. Cliquez sur **New query**
5. Copiez le contenu du fichier `scripts/setup_supabase.sql`
6. Collez-le dans l'éditeur SQL
7. Cliquez sur **Run** (ou appuyez sur Cmd/Ctrl + Enter)

Les tables suivantes seront créées :
- `synthetic_transactions` : Stocke toutes les transactions générées
- `synthetic_batches` : Stocke les métadonnées des batches de génération
- `fraud_statistics` : Vue pour les statistiques quotidiennes

## Étape 3: Tester la connexion

Exécutez le script de test :

```bash
python scripts/test_supabase_connection.py
```

Vous devriez voir :
```
✓ Connexion à la base de données établie
✓ Table 'synthetic_transactions' existe
✓ Table 'synthetic_batches' existe
✅ Connexion testée avec succès!
```

## Étape 4: Générer et sauvegarder

Maintenant, lorsque vous générez des transactions avec la CLI, elles seront automatiquement sauvegardées dans Supabase :

```bash
python cli.py --count 1000 --fraud-ratio 0.1
```

Les transactions seront automatiquement loggées dans la table `synthetic_transactions`.

## Vérifier les données dans Supabase

1. Allez dans **Table Editor** dans votre dashboard Supabase
2. Sélectionnez la table `synthetic_transactions`
3. Vous verrez toutes les transactions générées

## Requêtes SQL utiles

### Compter les transactions
```sql
SELECT COUNT(*) FROM synthetic_transactions;
```

### Compter les transactions frauduleuses
```sql
SELECT COUNT(*) FROM synthetic_transactions WHERE is_fraud = true;
```

### Voir les dernières transactions
```sql
SELECT * FROM synthetic_transactions 
ORDER BY created_at DESC 
LIMIT 100;
```

### Statistiques par batch
```sql
SELECT 
    batch_id,
    created_at,
    generated_count,
    fraudulent_count,
    ROUND(100.0 * fraudulent_count / generated_count, 2) as fraud_percentage
FROM synthetic_batches
ORDER BY created_at DESC;
```

### Utiliser la vue de statistiques
```sql
SELECT * FROM fraud_statistics 
ORDER BY date DESC 
LIMIT 30;
```

## Dépannage

### Erreur "relation does not exist"
Les tables n'ont pas été créées. Exécutez `scripts/setup_supabase.sql` dans le SQL Editor.

### Erreur de connexion
1. Vérifiez que le mot de passe dans `.env` est correct
2. Vérifiez que l'URL de la base de données est correcte
3. Testez avec : `python scripts/test_supabase_connection.py`

### Erreur SSL
Supabase nécessite SSL. Le code est configuré pour utiliser `sslmode=require` automatiquement.

### Transactions non sauvegardées
1. Vérifiez les logs de la CLI
2. Testez la connexion avec le script de test
3. Vérifiez que les tables existent dans Supabase
