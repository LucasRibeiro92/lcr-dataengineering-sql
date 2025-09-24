from dataclasses import dataclass
import os
import dotenv

dotenv.load_dotenv()

@dataclass
class DbConfig:
    server: str = os.getenv("MSSQL_SERVER", "localhost")
    port: str = os.getenv("MSSQL_PORT", "1433")
    database: str = os.getenv("MSSQL_DATABASE", "master")
    username: str = os.getenv("MSSQL_USERNAME", "")
    password: str = os.getenv("MSSQL_PASSWORD", "")
    trusted: str = os.getenv("MSSQL_TRUSTED_CONNECTION", "no").lower()  # "yes" ou "no"

    def sqlalchemy_url(self) -> str:
        driver = "ODBC Driver 18 for SQL Server"
        if self.trusted in ("yes", "true", "1"):
            # Windows Authentication:
            return (
                f"mssql+pyodbc://@{self.server},{self.port}/{self.database}"
                f"?driver={driver.replace(' ', '+')}"
                f"&Trusted_Connection=yes"
                f"&TrustServerCertificate=yes"
            )
        else:
            # SQL Login:
            return (
                f"mssql+pyodbc://{self.username}:{self.password}"
                f"@{self.server},{self.port}/{self.database}"
                f"?driver={driver.replace(' ', '+')}"
                f"&TrustServerCertificate=yes"
            )
