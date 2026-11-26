"""Latency alerting service with live rule version switching."""

from .app import create_app

__all__ = ["create_app"]
