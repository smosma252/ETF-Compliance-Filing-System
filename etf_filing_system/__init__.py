from pathlib import Path
from dotenv import load_dotenv

# Ensure API, Celery worker, and scripts all read the same local env file.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
