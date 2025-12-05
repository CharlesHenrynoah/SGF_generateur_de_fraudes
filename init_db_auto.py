import sys
import os
import asyncio
from sqlalchemy import text

# Ajouter le répertoire app au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config import settings
from app.services.storage_service import storage_service

async def init_db():
    print("🔄 Tentative de connexion et d'initialisation de la base de données...")
    
    # Initialiser le service de stockage
    await storage_service.initialize_db()
    
    if not storage_service.db_engine:
        print("❌ Impossible de se connecter à la base de données.")
        return False

    print("✅ Connexion établie !")
    
    # Lire le fichier SQL
    try:
        with open('scripts/setup_supabase.sql', 'r') as f:
            sql_script = f.read()
            
        print("🛠️  Création des tables en cours...")
        
        with storage_service.db_engine.connect() as conn:
            # Exécuter le script SQL
            # Note: SQLAlchemy execute ne gère pas toujours bien les scripts multiples commandes
            # On va essayer de l'exécuter bloc par bloc ou en une fois selon le support
            conn.execute(text(sql_script))
            conn.commit()
            
        print("✨ Tables créées avec succès ! (synthetic_transactions, synthetic_batches)")
        return True
        
    except Exception as e:
        print(f"⚠️  Une erreur est survenue lors de la création des tables : {e}")
        print("   (Il est possible qu'elles existent déjà ou que le format du script SQL pose problème via Python)")
        return False

if __name__ == "__main__":
    asyncio.run(init_db())
