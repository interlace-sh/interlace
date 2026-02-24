"""
API documentation handler.

Serves OpenAPI specification and Swagger UI at /api/docs.
"""

from importlib.resources import files
from pathlib import Path

from aiohttp import web

SWAGGER_UI_VERSION = "5.17.14"

# Resolve the openapi.yaml path relative to this package
_OPENAPI_PATH = Path(str(files("interlace.service.api").joinpath("openapi.yaml")))

_SWAGGER_HTML = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Interlace API Documentation</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui.css">
  <style>
    html {{ box-sizing: border-box; overflow-y: scroll; }}
    *, *::before, *::after {{ box-sizing: inherit; }}
    body {{ margin: 0; background: #fafafa; }}
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({{
      url: '/api/openapi.yaml',
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: 'StandaloneLayout',
      deepLinking: true,
    }});
  </script>
</body>
</html>
"""


async def swagger_ui(request: web.Request) -> web.Response:
    """Serve the Swagger UI HTML page."""
    return web.Response(text=_SWAGGER_HTML, content_type="text/html")


async def openapi_yaml(request: web.Request) -> web.Response:
    """Serve the OpenAPI YAML specification."""
    if not _OPENAPI_PATH.exists():
        raise web.HTTPNotFound(text="openapi.yaml not found")
    content = _OPENAPI_PATH.read_text(encoding="utf-8")
    return web.Response(text=content, content_type="text/yaml")
