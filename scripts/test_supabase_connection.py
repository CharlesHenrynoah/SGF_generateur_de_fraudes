#!/usr/bin/env python3
"""Script pour tester la connexion à Supabase."""
import sys
import os
import asyncio

# Ajouter le répertoire app au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.services.storage_service import storage_service


async def test_connection():
    """Teste la connexion à Supabase."""
    print("🔍 Test de connexion à Supabase...")
    print(f"   DATABASE_URL: {'✓ Configuré' if settings.database_url else '✗ Non configuré'}")
    print(f"   SUPABASE_URL: {'✓ Configuré' if settings.supabase_url else '✗ Non configuré'}")
    print()
    
    if not settings.database_url and not settings.supabase_url:
        print("❌ Erreur: Aucune configuration de base de données trouvée")
        print("   Configurez DATABASE_URL ou SUPABASE_URL dans le fichier .env")
        return False
    
    try:
        # Initialiser la connexion
        await storage_service.initialize_db()
        
        if not storage_service._db_initialized:
            print("❌ Erreur: Impossible d'initialiser la connexion à la base de données")
            return False
        
        print("✓ Connexion à la base de données établie")
        
        # Tester l'accès aux tables
        from sqlalchemy import text
        
        if hasattr(storage_service, 'db_engine') and storage_service.db_engine:
            with storage_service.db_engine.connect() as conn:
                # Vérifier si les tables existent
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('synthetic_transactions', 'synthetic_batches')
                """))
                
                tables = [row[0] for row in result]
                
                if 'synthetic_transactions' in tables:
                    print("✓ Table 'synthetic_transactions' existe")
                    
                    # Compter les transactions
                    count_result = conn.execute(text("SELECT COUNT(*) FROM synthetic_transactions"))
                    count = count_result.scalar()
                    print(f"   Nombre de transactions: {count}")
                else:
                    print("⚠️  Table 'synthetic_transactions' n'existe pas")
                    print("   Exécutez db/init.sql dans votre Supabase SQL Editor")
                
                if 'synthetic_batches' in tables:
                    print("✓ Table 'synthetic_batches' existe")
                    
                    # Compter les batches
                    count_result = conn.execute(text("SELECT COUNT(*) FROM synthetic_batches"))
                    count = count_result.scalar()
                    print(f"   Nombre de batches: {count}")
                else:
                    print("⚠️  Table 'synthetic_batches' n'existe pas")
                    print("   Exécutez db/init.sql dans votre Supabase SQL Editor")
        
        elif hasattr(storage_service, 'supabase') and storage_service.supabase:
            print("✓ Client Supabase initialisé")
            # Tester avec Supabase client
            try:
                result = storage_service.supabase.table('synthetic_transactions').select('transaction_id', count='exact').limit(1).execute()
                print(f"✓ Accès à la table 'synthetic_transactions' OK")
                print(f"   Nombre de transactions: {result.count if hasattr(result, 'count') else 'N/A'}")
            except Exception as e:
                print(f"⚠️  Erreur d'accès à la table: {e}")
                print("   Vérifiez que les tables existent (exécutez db/init.sql)")
        
        print("\n✅ Connexion testée avec succès!")
        return True
        
    except Exception as e:
        print(f"\n❌ Erreur lors du test de connexion: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_connection())
    sys.exit(0 if success else 1)
