from datetime import datetime
from dotenv import load_dotenv
from backend.db import get_client

load_dotenv()
supabase = get_client()

def save_article(article: dict) -> bool:
    """
    Inserts into 'articles'. Returns True if inserted, False if skipped (duplicate).
    Requires 'url' unique constraint in DB.
    """
    url = article.get("url")
    if not url:
        raise ValueError("Article missing url")

    # Check if exists (by url)
    exists = supabase.table("articles").select("id").eq("url", url).limit(1).execute()
    if exists.data and len(exists.data) > 0:
        return False

    # Normalize timestamp fields if present
    if "published_at" in article and isinstance(article["published_at"], str):
        # Leave as string; Supabase can parse ISO strings.
        pass

    # Add created_at if not provided
    article.setdefault("created_at", datetime.utcnow().isoformat())

    supabase.table("articles").insert(article).execute()
    return True
