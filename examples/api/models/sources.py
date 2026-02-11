"""
Source models for DummyJSON API.

These models fetch raw data from https://dummyjson.com/docs

All models share the same API instance for global rate limiting.
"""

from interlace import model, API
from interlace.utils.logging import get_logger

# Create shared API instance - all models using this will share rate limiting
api = API(base_url="https://dummyjson.com", max_concurrent=10)


@model(name="products", materialize="table", strategy="replace")
async def products():
    """Fetch all products from DummyJSON."""
    logger = get_logger(__name__)
    logger.info("Processing products...")
    async with api:
        data = await api.paginated(
            "/products",
            page_size=30,  # DummyJSON default limit
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="products"
        )
        logger.debug(f"Found {data.count().execute()} products")
        return data


@model(name="users", materialize="table", strategy="replace", schedule="")
async def users():
    """Fetch all users from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/users",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="users"
        )
        return data


@model(name="carts", materialize="table", strategy="replace")
async def carts():
    """Fetch all carts from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/carts",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="carts"
        )
        return data


@model(name="posts", materialize="table", strategy="replace")
async def posts():
    """Fetch all posts from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/posts",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="posts"
        )
        return data


@model(name="comments", materialize="table", strategy="replace")
async def comments():
    """Fetch all comments from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/comments",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="comments"
        )
        return data


@model(name="todos", materialize="table", strategy="replace")
async def todos():
    """Fetch all todos from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/todos",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="todos"
        )
        return data


@model(name="quotes", materialize="table", strategy="replace")
async def quotes():
    """Fetch all quotes from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/quotes",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="quotes"
        )
        return data


@model(name="recipes", materialize="table", strategy="replace")
async def recipes():
    """Fetch all recipes from DummyJSON."""
    async with api:
        data = await api.paginated(
            "/recipes",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="recipes"
        )
        return data

