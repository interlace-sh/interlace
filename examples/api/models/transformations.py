"""
Transformation models for DummyJSON data.

These models transform and enrich the raw data from source models.
"""

from interlace import model
import ibis


@model(name="product_categories", materialize="table", strategy="replace")
async def product_categories(products: ibis.Table):
    """Aggregate products by category with statistics."""
    result = products.group_by("category").agg(
        total_products=products.id.count(),
        avg_price=products.price.mean(),
        min_price=products.price.min(),
        max_price=products.price.max(),
        total_stock=products.stock.sum(),
        avg_rating=products.rating.mean(),
        avg_discount_percentage=products.discount_percentage.mean()
    ).order_by(ibis.desc("total_products"))
    
    return result


@model(name="user_activity_summary", materialize="table", strategy="replace")
async def user_activity_summary(users: ibis.Table, posts: ibis.Table, todos: ibis.Table, carts: ibis.Table):
    """Summarize user activity across posts, todos, and carts."""
    # Count posts per user
    posts_by_user = (
        posts.group_by("user_id")
        .agg(post_count=posts.id.count())
        .rename({"user_id": "id"})
    )
    
    # Count todos per user
    todos_by_user = (
        todos.group_by("user_id")
        .agg(todo_count=todos.id.count(), completed_todos=todos.completed.sum())
        .rename({"user_id": "id"})
    )
    
    # Count carts per user
    carts_by_user = (
        carts.group_by("user_id")
        .agg(cart_count=carts.id.count(), total_cart_value=carts.total.sum())
        .rename({"user_id": "id"})
    )
    
    # Join with users - build joins incrementally
    base = users[["id", "username", "email", "first_name", "last_name", "age", "gender"]]
    joined = base.left_join(posts_by_user, base.id == posts_by_user.id)
    joined = joined.left_join(todos_by_user, joined.id == todos_by_user.id)
    joined = joined.left_join(carts_by_user, joined.id == carts_by_user.id)
    
    # Fill NaN with 0 using coalesce
    result = joined.mutate(
        post_count=ibis.coalesce(joined.post_count, 0),
        todo_count=ibis.coalesce(joined.todo_count, 0),
        completed_todos=ibis.coalesce(joined.completed_todos, 0),
        cart_count=ibis.coalesce(joined.cart_count, 0),
        total_cart_value=ibis.coalesce(joined.total_cart_value, 0)
    )
    
    return result


@model(name="high_value_carts", materialize="table", strategy="replace")
async def high_value_carts(carts: ibis.Table):
    """Identify high-value carts with product details."""
    # Filter high-value carts (1.5x average as threshold)
    avg_total = carts.total.mean()
    high_value = carts.filter(carts.total >= avg_total * 1.5)
    
    result = high_value.select([
        "id",
        "user_id",
        "total",
        "discounted_total",
        "total_products",
        "total_quantity"
    ]).order_by(ibis.desc("total"))
    
    return result


@model(name="post_engagement", materialize="table", strategy="replace")
async def post_engagement(posts: ibis.Table, comments: ibis.Table):
    """Calculate post engagement metrics."""
    # Count comments per post
    comments_by_post = (
        comments.group_by("post_id")
        .agg(comment_count=comments.id.count())
    )
    
    # Join with posts using post_id (don't rename, keep original column name)
    joined = (
        posts[["id", "title", "body", "user_id", "tags", "reactions"]]
        .left_join(comments_by_post, posts.id == comments_by_post.post_id)
    )
    
    result = joined.mutate(
        comment_count=ibis.coalesce(joined.comment_count, 0)
    ).order_by(ibis.desc("comment_count"))
    
    return result


@model(name="todo_completion_rates", materialize="table", strategy="replace")
async def todo_completion_rates(todos: ibis.Table, users: ibis.Table):
    """Calculate todo completion rates by user."""
    # Calculate completion stats per user
    agg_stats = (
        todos.group_by("user_id")
        .agg(
            total_todos=todos.id.count(),
            completed_todos=todos.completed.sum()
        )
    )
    
    completion_stats = (
        agg_stats
        .mutate(
            pending_todos=(agg_stats.total_todos - agg_stats.completed_todos),
            completion_rate=(agg_stats.completed_todos / agg_stats.total_todos * 100)
        )
    )
    
    # Join with users using user_id (don't rename, keep original column name)
    result = (
        users[["id", "username", "email", "first_name", "last_name"]]
        .join(completion_stats, users.id == completion_stats.user_id)
        .order_by(ibis.desc("completion_rate"))
    )
    
    return result


@model(name="product_price_analysis", materialize="table", strategy="replace")
async def product_price_analysis(products: ibis.Table):
    """Analyze product pricing with discount impact."""
    result = products.mutate(
        original_price=products.price,
        discounted_price=products.price * (1 - products.discount_percentage / 100),
        savings_amount=products.price * (products.discount_percentage / 100),
        has_discount=(products.discount_percentage > 0),
        price_range=(
            products.price.cases(
                (products.price < 50, "Budget"),
                (products.price < 200, "Mid-range"),
                (products.price < 500, "Premium"),
                "Luxury"
            )
        )
    ).select([
        "id",
        "title",
        "category",
        "brand",
        "original_price",
        "discounted_price",
        "discount_percentage",
        "savings_amount",
        "has_discount",
        "price_range",
        "rating",
        "stock"
    ]).order_by(ibis.desc("savings_amount"))
    
    return result
