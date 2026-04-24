"""
Silence noisy third-party deprecation warnings during local Rasa commands.

Python automatically imports `sitecustomize` (if present on sys.path) after
the `site` module. Keeping this file inside `rasa_bot/` makes it active when
running commands from this folder, e.g. `rasa train --force`.
"""

from __future__ import annotations

import os
import warnings


# Rasa 3.6 uses SQLAlchemy APIs that emit this warning even on <2.0.
os.environ.setdefault("SQLALCHEMY_SILENCE_UBER_WARNING", "1")


# pkg_resources namespace / deprecation warnings triggered by rasa internals.
warnings.filterwarnings(
    "ignore",
    message=r".*pkg_resources is deprecated as an API.*",
    category=DeprecationWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r".*Deprecated call to `pkg_resources\.declare_namespace.*",
    category=DeprecationWarning,
)


# Rasa tracker store warning from SQLAlchemy transition messaging.
warnings.filterwarnings(
    "ignore",
    message=r".*Deprecated API features detected!.*SQLAlchemy 2\.0.*",
)
