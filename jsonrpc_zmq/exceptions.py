# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.

from jsonrpcserver import JsonRpcError

class TransportError(Exception):
    """Transport-level fatal error"""

class BackPressureError(Exception):
    """Send buffer is full; caller should apply flow control"""

class RequestTimeoutError(Exception):
    """Request timed out"""

class InvalidStateError(Exception):
    """Operation is not allowed in current peer state."""

