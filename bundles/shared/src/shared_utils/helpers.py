"""Helpers for shared utilities."""


class Model:
    """Model class to handle table naming and relations for managed models."""

    def __init__(self, catalog, schema, prefix, *, model_name=None, task_params=None | dict):
        """Initializes the Model instance."""
        self._name = model_name or self.get_current_notebook_path().split("/")[-1]
        self.name = f"{prefix}{self._name}"
        self.catalog = catalog
        self.schema = schema
        self._task_params = task_params

    @property
    def relation(self):
        return f"{self.catalog}.{self.schema}.{self.name}"

    def get_current_notebook_path(self):
        """Returns the current notebook path in Databricks."""
        return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # pylint: disable=internal-api


class Source:
    """Model class to handle table naming and relations for sources."""

    def __init__(self, catalog, schema, name):
        """Initializes the Source instance."""
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.relation = f"{catalog}.{schema}.{name}"


class Sources:
    """Model class to handle table naming and relations for multiple sources."""

    def __init__(self, catalog, schema, names):
        """Initializes the Sources instance."""
        for name in names:
            setattr(self, Source(catalog, schema, name))
