import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database credentials
DBNAME = os.getenv('DBNAME', 'your_database_name')
DATABASE_HOST = os.getenv('DATABASE_HOST', 'localhost')
USER = os.getenv('USER', 'postgres')
PASSWORD = os.getenv('PASSWORD', 'your_password')
DATABASE_PORT = int(os.getenv('DATABASE_PORT', 5432))

# Server configuration
SERVER_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
SERVER_PORT = int(os.getenv('SERVER_PORT', 5000))
