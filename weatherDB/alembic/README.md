# database migrations

To migrate the database alembic is used. Alembic is a lightweight database migration tool for SQLAlchemy. It is used to generate and apply migrations to the database. The migrations are generated for each version of the weatherDB module.

To use alembic, you have to point to the `alembic.ini` file in the `weatherDB/alembic` directory, like:
```bash
alembic -c weatherDB_module_path/alembic/alembic.ini {command}
```