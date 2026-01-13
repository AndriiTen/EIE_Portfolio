# EIE-Economic_Indicators_Extractor.Bot-API_2.0

Economic Indicators Extractor Bot - GraphQL API for extracting and processing economic indicators data.

## Features

- **Economic Indicators Processing**: Fetches and processes various economic indicators (GDP, CPI, Treasury yields, Federal funds rate, etc.)
- **GraphQL API**: Provides GraphQL interface for querying economic data
- **Database Integration**: Stores data in PostgreSQL database
- **Automated Data Updates**: Scheduled extraction and processing of economic indicators

## Tech Stack

- Python 3.x
- GraphQL (Ariadne)
- PostgreSQL
- Flask
- psycopg2

## Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database
- pip

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd EIE-Economic_Indicators_Extractor.Bot-API_2.0-feature-change-TDFDUTP-16
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your database credentials and configuration

```bash
cp .env.example .env
```

4. Edit `.env` file with your settings:
```
DBNAME=your_database_name
DATABASE_HOST=your_database_host
USER=your_database_user
PASSWORD=your_secure_password
DATABASE_PORT=5432
SERVER_HOST=0.0.0.0
SERVER_PORT=5000
```

### Running the Application

Start the GraphQL server:
```bash
python src/app.py
```

The server will start on `http://localhost:5000/graphql`

### Usage Example

Use the client script to test the API:
```bash
python client_request.py
```

Example GraphQL query:
```graphql
query($tickers: [String!]) {
  EIE_Calculator(tickers_list: $tickers) {
    success
    error
    message
    indicators_inserted
    events_inserted
  }
}
```

## Project Structure

```
├── src/
│   ├── app.py           # GraphQL server application
│   ├── main.py          # Main ETL logic
│   ├── queries.py       # GraphQL queries
│   └── settings.py      # Configuration settings
├── tests/
│   └── test_client.py   # Test suite
├── client_request.py    # Example client
├── schema.graphql       # GraphQL schema
└── README.md
```

## Security Notes

⚠️ **Important**: Never commit `.env` file with real credentials to version control!

- All sensitive data (passwords, API keys) should be stored in `.env` file
- `.env` is already in `.gitignore` to prevent accidental commits
- Use `.env.example` as a template for required environment variables

## License

This is a portfolio project for demonstration purposes.