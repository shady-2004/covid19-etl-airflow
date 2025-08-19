# Start FROM the official Airflow image
FROM apache/airflow:3.0.4

USER root

# Install dependencies for ODBC + MS SQL Server
RUN apt-get update \
    && apt-get install -y gnupg curl apt-transport-https unixodbc unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add sqlcmd and bcp to PATH
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Switch back to airflow user
USER airflow
