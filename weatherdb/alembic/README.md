# database migrations

To migrate the database alembic is used. Alembic is a lightweight database migration tool for SQLAlchemy. It is used to generate and apply migrations to the database. The migrations are generated for each version of the WeatherDB module.

To use alembic, you have to point to the `alembic.ini` file in the `weatherdb/alembic` directory, like:
```bash
alembic -c weatherdb_module_path/alembic/alembic.ini {command}
```